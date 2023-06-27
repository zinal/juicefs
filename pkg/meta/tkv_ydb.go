//go:build !noydbkv
// +build !noydbkv

/*
 * JuiceFS, Copyright 2023 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	yc "github.com/ydb-platform/ydb-go-yc"
)

func init() {
	Register("ydbkv", newKVMeta)
	drivers["ydbkv"] = newYdbClient
}

type ydbkvTxn struct {
	queries *ydbkvQueries
	ctx     context.Context
	actor   table.TransactionActor
	vput    map[string][]byte
	vdel    map[string]bool
}

func (tx *ydbkvTxn) get(key []byte) []byte {
	if tx.vdel != nil {
		if _, found := tx.vdel[string(key)]; found {
			return nil
		}
	}
	if tx.vput != nil {
		if value, found := tx.vput[string(key)]; found {
			return value
		}
	}
	data, err := tx.actor.Execute(tx.ctx, tx.queries.selectOne,
		table.NewQueryParameters(
			table.ValueParam("$k", types.BytesValue(key)),
		), options.WithKeepInCache(true),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = data.Close()
	}()
	if data.NextResultSet(tx.ctx) {
		if data.NextRow() {
			var value []byte
			if err = data.Scan(&value); err != nil {
				panic(err)
			}
			return value
		}
	}
	return nil
}

func (tx *ydbkvTxn) gets(keys ...[]byte) [][]byte {
	var rs = make([][]byte, 0, len(keys))
	if len(keys) > 128 {
		for i := 0; i < len(keys); i += 128 {
			rs = append(rs, tx.gets(keys[i:min(i+128, len(keys))]...)...)
		}
	} else {
		input := make([]types.Value, len(keys))
		for i, v := range keys {
			input[i] = types.StructValue(
				types.StructFieldValue("k", types.BytesValue(v)),
			)
		}
		data, err := tx.actor.Execute(tx.ctx, tx.queries.selectSome,
			table.NewQueryParameters(
				table.ValueParam("$tab", types.ListValue(input...)),
			), options.WithKeepInCache(true),
		)
		if err != nil {
			panic(err)
		}
		defer func() {
			_ = data.Close()
		}()
		if data.NextResultSet(tx.ctx) {
			if data.CurrentResultSet().RowCount() != len(keys) {
				panic(fmt.Errorf("logical error: query returned row count %d, expected %d",
					data.CurrentResultSet().RowCount(), len(keys)))
			}
			pos := 0
			for data.NextRow() {
				var item []byte
				if err = data.Scan(&item); err != nil {
					panic(err)
				}
				rs[pos] = item
			}
		}
	}
	return rs
}

func (tx *ydbkvTxn) scan(begin, end []byte, keysOnly bool, handler func(k, v []byte) bool) {
	const limit int32 = 200
	limit_p := types.Int32Value(limit)
	begin_p := types.BytesValue(begin)
	end_p := types.BytesValue(end)
	initialScan := true
	statement := tx.queries.selectRange
	if keysOnly {
		statement = tx.queries.selectKeyRange
	}
	for {
		next := func() bool {
			data, err := tx.actor.Execute(tx.ctx, statement,
				table.NewQueryParameters(
					table.ValueParam("$left", begin_p),
					table.ValueParam("$right", end_p),
					table.ValueParam("$limit", limit_p),
				), options.WithKeepInCache(true))
			if err != nil {
				panic(err)
			}
			defer func() {
				_ = data.Close()
			}()
			var current int32 = 0
			if data.NextResultSet(tx.ctx) {
				for data.NextRow() {
					var k, v []byte
					if err = data.Scan(&k, &v); err != nil {
						panic(err)
					}
					if !handler(k, v) {
						return false
					}
					current++
				}
			}
			return current >= limit
		}()
		if !next {
			break
		}
		if initialScan {
			// need "greater than" instead of "greater or equal" on further scans
			statement = strings.ReplaceAll(statement, "WHERE k>=$left ", "WHERE k>$left ")
			initialScan = false
		}
	}
}

func (tx *ydbkvTxn) exist(prefix []byte) bool {
	if tx.vput != nil {
		for k := range tx.vput {
			if strings.HasPrefix(k, string(prefix)) {
				return true
			}
		}
	}
	data, err := tx.actor.Execute(tx.ctx, tx.queries.existsPrefix,
		table.NewQueryParameters(
			table.ValueParam("p", types.BytesValue(prefix)),
		), options.WithKeepInCache(true),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = data.Close()
	}()
	if data.NextResultSet(tx.ctx) {
		if data.NextRow() {
			var key []byte
			if err = data.Scan(&key); err != nil {
				panic(err)
			}
			return bytes.HasPrefix(key, prefix)
		}
	}
	return false
}

func (tx *ydbkvTxn) applyChanges() error {
	if len(tx.vdel) > 0 {
		input := make([]types.Value, len(tx.vdel))
		i := 0
		for k := range tx.vdel {
			input[i] = types.StructValue(
				types.StructFieldValue("k", types.BytesValue([]byte(k))),
			)
			i++
		}
		_, err := tx.actor.Execute(tx.ctx, tx.queries.deleteSome,
			table.NewQueryParameters(
				table.ValueParam("$tab", types.ListValue(input...)),
			), options.WithKeepInCache(true),
		)
		if err != nil {
			return err
		}
		tx.vdel = nil
	}
	if len(tx.vput) > 0 {
		input := make([]types.Value, len(tx.vdel))
		i := 0
		for k, v := range tx.vput {
			input[i] = types.StructValue(
				types.StructFieldValue("k", types.BytesValue([]byte(k))),
				types.StructFieldValue("v", types.BytesValue(v)),
			)
			i++
		}
		_, err := tx.actor.Execute(tx.ctx, tx.queries.upsertSome,
			table.NewQueryParameters(
				table.ValueParam("$tab", types.ListValue(input...)),
			), options.WithKeepInCache(true),
		)
		if err != nil {
			return err
		}
		tx.vput = nil
	}
	return nil
}

func (tx *ydbkvTxn) delete(key []byte) {
	if tx.vdel == nil {
		tx.vdel = make(map[string]bool)
	}
	tx.vdel[string(key)] = true
	if tx.vput != nil {
		delete(tx.vput, string(key))
	}
}

func (tx *ydbkvTxn) set(key, value []byte) {
	if tx.vput == nil {
		tx.vput = make(map[string][]byte)
	}
	tx.vput[string(key)] = value
	if tx.vdel != nil {
		delete(tx.vdel, string(key))
	}
}

func (tx *ydbkvTxn) append(key []byte, value []byte) []byte {
	old := tx.get(key)
	new := append(old, value...)
	tx.set(key, new)
	return new
}

func (tx *ydbkvTxn) incrBy(key []byte, value int64) int64 {
	buf := tx.get(key)
	new := parseCounter(buf)
	if value != 0 {
		new += value
		tx.set(key, packCounter(new))
	}
	return new
}

type YdbAuthMode int

const (
	YdbAuthNone YdbAuthMode = iota
	YdbAuthStatic
	YdbAuthMeta
	YdbAuthSaKey
)

var ydbAuthModeMap = map[string]YdbAuthMode{
	"":       YdbAuthNone,
	"none":   YdbAuthNone,
	"static": YdbAuthStatic,
	"meta":   YdbAuthMeta,
	"sakey":  YdbAuthSaKey,
}

type ydbkvQueries struct {
	deleteAll      string
	deleteRange    string
	deleteSome     string
	upsertSome     string
	selectOne      string
	selectSome     string
	selectRange    string
	selectKeyRange string
	existsPrefix   string
}

type ydbkvClient struct {
	sync.RWMutex
	isOpen    bool
	con       *ydb.Driver
	tableName string
	queries   ydbkvQueries
}

var ydbkvInstance *ydbkvClient
var ydbkvOnce sync.Once

func ydbkv() *ydbkvClient {
	ydbkvOnce.Do(func() {
		ydbkvInstance = new(ydbkvClient)
		ydbkvInstance.isOpen = false
	})
	return ydbkvInstance
}

func expandQuery(tableName string, template string) string {
	return strings.ReplaceAll(template, "{kvtable}", "`"+tableName+"`")
}

func (c *ydbkvClient) initQueries(tableName string) {
	c.tableName = tableName
	c.queries.deleteAll = expandQuery(tableName, `DECLARE $limit AS Int32;
	        $q=(SELECT k FROM {kvtable} ORDER BY k LIMIT $limit); 
			SELECT CAST(COUNT(*) AS Int32) AS cnt FROM $q; 
			DELETE FROM {kvtable} ON SELECT * FROM $q`)
	c.queries.deleteRange = expandQuery(tableName, `DECLARE $left AS String;
	        DECLARE $right AS String;
			DECLARE $limit AS Int32;
			$q=(SELECT k FROM {kvtable} WHERE k>=$left AND k<$right ORDER BY k LIMIT $limit);
			SELECT CAST(COUNT(*) AS Int32) AS cnt FROM $q;
			DELETE FROM {kvtable} ON SELECT * FROM $q;`)
	c.queries.deleteSome = expandQuery(tableName, `DECLARE $tab AS List<Struct<k:String>>;
			DELETE FROM {kvtable} ON SELECT * FROM AS_TABLE($tab);`)
	c.queries.upsertSome = expandQuery(tableName, `DECLARE $tab AS List<Struct<k:String, v:String>>;
			UPSERT INTO {kvtable} SELECT k, v FROM AS_TABLE($tab);`)
	c.queries.selectOne = expandQuery(tableName, `DECLARE $k AS String; 
			SELECT v FROM {kvtable} WHERE k=$k;`)
	c.queries.selectSome = expandQuery(tableName, `DECLARE $tab AS List<Struct<k:String>>; 
			SELECT b.v FROM AS_TABLE($tab) a LEFT JOIN {kvtable} b ON a.k=b.k;`)
	c.queries.selectRange = expandQuery(tableName, `DECLARE $left AS String;
	        DECLARE $right AS String;
			DECLARE $limit AS Int32;
			SELECT k, v FROM {kvtable} 
			WHERE k>=$left AND k<$right ORDER BY k LIMIT $limit;`)
	c.queries.selectKeyRange = expandQuery(tableName, `DECLARE $left AS String;
	        DECLARE $right AS String;
			DECLARE $limit AS Int32;
			SELECT k, CAST(NULL AS String?) AS v FROM {kvtable} 
			WHERE k>=$left AND k<$right ORDER BY k LIMIT $limit;`)
	c.queries.existsPrefix = expandQuery(tableName, `DECLARE $p AS String; 
			SELECT k FROM {kvtable} WHERE k>=$p LIMIT 1;`)
}

func ydbAuth(mode YdbAuthMode, q url.Values, u *url.URL) ydb.Option {
	switch mode {
	case YdbAuthStatic:
		p, _ := u.User.Password()
		return ydb.WithStaticCredentials(u.User.Username(), p)
	case YdbAuthMeta:
		return yc.WithMetadataCredentials()
	case YdbAuthSaKey:
		return yc.WithServiceAccountKeyFileCredentials(q.Get("saKeyFile"))
	default:
		return ydb.WithAnonymousCredentials()
	}
}

func ydbCreateKvTable(ctx context.Context, con *ydb.Driver, tableName string) error {
	tablePath := con.Scheme().Database() + "/" + tableName
	return con.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, tablePath,
				options.WithColumn("k", types.TypeBytes),
				options.WithColumn("v", types.Optional(types.TypeBytes)),
				options.WithPrimaryKeyColumn("k"),
				options.WithPartitioningSettings(
					options.WithMinPartitionsCount(50),
					options.WithMaxPartitionsCount(100),
					options.WithPartitioningByLoad(options.FeatureEnabled),
				),
			)
		},
		table.WithIdempotent(),
	)
}

func (c *ydbkvClient) initDo(addr string) error {
	u, err := url.Parse("ydbkv://" + addr)
	if err != nil {
		return err
	}
	q := u.Query()
	database := u.Path
	if len(database) == 0 {
		database = q.Get("database")
	}
	if !strings.HasPrefix(database, "/") {
		database = "/" + database
	}
	tls := false
	if q.Has("tls") {
		tls, _ = strconv.ParseBool(q.Get("tls"))
	}
	var ydbUrl string
	if tls {
		ydbUrl = "grpcs://" + u.Host + database
	} else {
		ydbUrl = "grpc://" + u.Host + database
	}
	authModeStr := strings.ToLower(q.Get("authMode"))
	authMode, authModeFound := ydbAuthModeMap[authModeStr]
	if !authModeFound {
		return fmt.Errorf("unknown auth mode: %q", authModeStr)
	}
	tableName := q.Get("tableName")
	if len(tableName) == 0 {
		tableName = "juicefs_kv"
	}

	ydbContext, ctxCloseFn := context.WithTimeout(context.Background(), 10*time.Second)
	defer ctxCloseFn()
	c.con, err = ydb.Open(ydbContext, ydbUrl,
		ydb.WithUserAgent("juicefs"),
		ydb.WithSessionPoolSizeLimit(5*runtime.GOMAXPROCS(-1)),
		ydb.WithSessionPoolIdleThreshold(10*time.Minute), ydbAuth(authMode, q, u))
	if err != nil {
		return err
	}
	err = ydbCreateKvTable(ydbContext, c.con, tableName)
	if err != nil {
		return err
	}
	c.initQueries(tableName)
	return nil
}

func (c *ydbkvClient) initIf(addr string) error {
	c.Lock()
	defer c.Unlock()
	if !c.isOpen {
		if err := c.initDo(addr); err != nil {
			return err
		}
		c.isOpen = true
	}
	return nil
}

func newYdbClient(addr string) (tkvClient, error) {
	client := ydbkv()
	err := client.initIf(addr)
	if err == nil {
		return client, nil
	}
	return nil, err
}

func (c *ydbkvClient) name() string {
	return "ydbkv"
}

func (c *ydbkvClient) shouldRetry(err error) bool {
	// all retries are handled internally
	return false
}

func (c *ydbkvClient) txn(f func(*kvTxn) error, retry int) error {
	ydbContext, ctxCloseFn := context.WithCancel(context.Background())
	defer ctxCloseFn()

	return c.con.Table().DoTx(
		ydbContext,
		func(ctx context.Context, tx table.TransactionActor) (err error) {
			defer func() {
				if r := recover(); r != nil {
					fe, ok := r.(error)
					if ok {
						err = fe
					} else {
						err = fmt.Errorf("ydb client txn func error: %v", r)
					}
				}
			}()
			data := ydbkvTxn{&c.queries, ctx, tx, nil, nil}
			err = f(&kvTxn{&data, 0})
			if err != nil {
				return err
			}
			err = data.applyChanges()
			return err
		},
		table.WithIdempotent(),
	)
}

func (c *ydbkvClient) scan(prefix []byte, handler func(key, value []byte)) error {
	tablePath := c.con.Scheme().Database() + "/" + c.tableName

	var keyRange *options.KeyRange
	if len(prefix) > 0 {
		keyRange = &options.KeyRange{
			From: types.TupleValue(
				types.OptionalValue(types.BytesValue(prefix)),
			),
			To: types.TupleValue(
				types.OptionalValue(types.BytesValue(nextKey(prefix))),
			),
		}
	}

	ydbContext, ctxCloseFn := context.WithCancel(context.Background())
	defer ctxCloseFn()

	return c.con.Table().Do(ydbContext, func(ctx context.Context, s table.Session) (err error) {
		var data result.StreamResult
		if keyRange == nil {
			data, err = s.StreamReadTable(ctx, tablePath, options.ReadOrdered())
		} else {
			data, err = s.StreamReadTable(ctx, tablePath, options.ReadOrdered(), options.ReadKeyRange(*keyRange))
		}
		if err != nil {
			return err
		}
		defer func() {
			_ = data.Close()
		}()
		for data.NextResultSet(ctx) {
			for data.NextRow() {
				var key, value []byte
				if err = data.Scan(&key, &value); err != nil {
					return err
				}
				handler(key, value)
			}
		}
		return nil
	})
}

func (c *ydbkvClient) reset(prefix []byte) (err error) {
	var statement string
	if len(prefix) > 0 {
		statement = c.queries.deleteRange
	} else {
		statement = c.queries.deleteAll
	}
	const limit int32 = 500
	limit_p := types.Int32Value(limit)
	left_p := types.BytesValue(prefix)
	right_p := types.BytesValue(nextKey(prefix))

	var deletedRows *int32
	cleaner := func(ctx context.Context, tx table.TransactionActor) (err error) {
		var data result.Result
		if len(prefix) > 0 {
			data, err = tx.Execute(ctx, statement, table.NewQueryParameters(
				table.ValueParam("$left", left_p),
				table.ValueParam("$right", right_p),
				table.ValueParam("$limit", limit_p),
			), options.WithKeepInCache(true))
		} else {
			data, err = tx.Execute(ctx, statement, table.NewQueryParameters(
				table.ValueParam("$limit", limit_p),
			), options.WithKeepInCache(true))
		}
		if err != nil {
			return err
		}
		defer func() {
			_ = data.Close()
		}()
		if data.NextResultSet(ctx) && data.NextRow() {
			err = data.Scan(&deletedRows)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("missing output row count for delete statement: %s", statement)
		}
		return nil
	}

	ydbContext, ctxCloseFn := context.WithCancel(context.Background())
	defer ctxCloseFn()

	for { // incremental deletion cycle
		deletedRows = nil
		err = c.con.Table().DoTx(ydbContext, cleaner, table.WithIdempotent())
		if err != nil {
			return err
		}
		if deletedRows == nil || *deletedRows < limit {
			break
		}
	}
	return nil
}

func (c *ydbkvClient) close() error {
	return nil
}

func (c *ydbkvClient) gc() {
	// noop
}
