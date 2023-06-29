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
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"syscall"
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

func unnestBytes(v *[]byte) []byte {
	if v == nil {
		return nil
	}
	return *v
}

func (tx *ydbkvTxn) getValue1(k []byte, v []byte) ([]byte, bool) {
	if tx.vdel != nil {
		if _, found := tx.vdel[string(k)]; found {
			return nil, true
		}
	}
	if tx.vput != nil {
		if value, found := tx.vput[string(k)]; found {
			return value, true
		}
	}
	return v, false
}

func (tx *ydbkvTxn) getValue2(k []byte, v *[]byte) ([]byte, bool) {
	if v == nil {
		return tx.getValue1(k, nil)
	}
	return tx.getValue1(k, *v)
}

func (tx *ydbkvTxn) get(key []byte) []byte {
	if value, replace := tx.getValue1(key, nil); replace {
		return value
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
			var value *[]byte
			if err = data.Scan(&value); err != nil {
				panic(err)
			}
			return unnestBytes(value)
		}
	}
	return nil
}

func (tx *ydbkvTxn) gets(keys ...[]byte) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		values := make([][]byte, 1)
		values[0] = tx.get(keys[0])
		return values
	}
	const maxPortion = 500
	if len(keys) > maxPortion {
		values := make([][]byte, 0, len(keys))
		for i := 0; i < len(keys); i += maxPortion {
			values = append(values, tx.gets(keys[i:min(i+maxPortion, len(keys))]...)...)
		}
		return values
	}
	input := make([]types.Value, len(keys))
	for i, v := range keys {
		input[i] = types.StructValue(
			types.StructFieldValue("k", types.BytesValue(v)),
		)
	}
	data, err := tx.actor.Execute(tx.ctx, tx.queries.selectSome,
		table.NewQueryParameters(
			table.ValueParam("$findKeys", types.ListValue(input...)),
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
		// build the temporary map, as keys may return in a different order
		xmap := make(map[string]*[]byte)
		for data.NextRow() {
			var key []byte
			var value *[]byte
			if err = data.Scan(&key, &value); err != nil {
				panic(err)
			}
			xmap[string(key)] = value
		}
		values := make([][]byte, len(keys))
		for i, k := range keys {
			if v, ok := xmap[string(k)]; ok {
				values[i], _ = tx.getValue2(k, v)
			} else {
				panic("logical error: missing input key in result set")
			}
		}
		return values
	}
	panic("logical error: missing result set")
}

func (tx *ydbkvTxn) scan(begin, end []byte, keysOnly bool, handler func(k, v []byte) bool) {
	const limit int32 = 500
	limit_p := types.Int32Value(limit)
	workBegin := begin
	end_p := types.BytesValue(end)
	initialScan := true
	statement := tx.queries.selectRange
	if keysOnly {
		statement = tx.queries.selectKeyRange
	}
	iterateFunc := func() bool {
		begin_p := types.BytesValue(workBegin)
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
		switched := false
		if data.NextResultSet(tx.ctx) {
			for data.NextRow() {
				var k []byte
				var v *[]byte
				if err = data.Scan(&k, &v); err != nil {
					panic(err)
				}
				if len(k) > 0 {
					workBegin = k
					switched = true
				}
				value, _ := tx.getValue2(k, v)
				if !handler(k, value) {
					return false
				}
				current++
			}
		}
		return (current >= limit) && switched
	}
	for iterateFunc() {
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
				table.ValueParam("$delKeys", types.ListValue(input...)),
			), options.WithKeepInCache(true),
		)
		if err != nil {
			return err
		}
		tx.vdel = nil
	}
	if len(tx.vput) > 0 {
		input := make([]types.Value, len(tx.vput))
		i := 0
		for k, v := range tx.vput {
			var v_p types.Value
			if v == nil {
				v_p = types.NullValue(types.TypeBytes)
			} else {
				v_p = types.OptionalValue(types.BytesValue(v))
			}
			input[i] = types.StructValue(
				types.StructFieldValue("k", types.BytesValue([]byte(k))),
				types.StructFieldValue("v", v_p),
			)
			i++
		}
		_, err := tx.actor.Execute(tx.ctx, tx.queries.upsertSome,
			table.NewQueryParameters(
				table.ValueParam("$insertPairs", types.ListValue(input...)),
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
	con       *ydb.Driver
	tableName string
	queries   ydbkvQueries
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
	c.queries.deleteSome = expandQuery(tableName, `DECLARE $delKeys AS List<Struct<k:String>>;
			DELETE FROM {kvtable} ON SELECT * FROM AS_TABLE($delKeys);`)
	c.queries.upsertSome = expandQuery(tableName, `DECLARE $insertPairs AS List<Struct<k:String, v:String?>>;
			UPSERT INTO {kvtable} SELECT k, v FROM AS_TABLE($insertPairs);`)
	c.queries.selectOne = expandQuery(tableName, `DECLARE $k AS String; 
			SELECT v FROM {kvtable} WHERE k=$k;`)
	c.queries.selectSome = expandQuery(tableName, `DECLARE $findKeys AS List<Struct<k:String>>; 
			SELECT a.k, b.v FROM AS_TABLE($findKeys) a LEFT JOIN {kvtable} b ON a.k=b.k;`)
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
	c.initQueries(tableName)

	func() {
		poolSize := 2 * runtime.GOMAXPROCS(-1)
		logger.Infof("Connecting to YDB at %q, session pool size %v", ydbUrl, poolSize)
		ydbContext, ctxCloseFn := context.WithTimeout(context.Background(), 10*time.Second)
		defer ctxCloseFn()
		c.con, err = ydb.Open(ydbContext, ydbUrl,
			ydb.WithUserAgent("juicefs"),
			ydb.WithSessionPoolSizeLimit(poolSize),
			ydb.WithSessionPoolIdleThreshold(time.Hour), ydbAuth(authMode, q, u))
	}()
	if err != nil {
		return err
	}
	func() {
		ydbContext, ctxCloseFn := context.WithCancel(context.Background())
		defer ctxCloseFn()
		err = ydbCreateKvTable(ydbContext, c.con, tableName)
	}()
	if err != nil {
		return err
	}
	return nil
}

func newYdbClient(addr string) (tkvClient, error) {
	client := &ydbkvClient{}
	if err := client.initDo(addr); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *ydbkvClient) name() string {
	return "ydbkv"
}

func (c *ydbkvClient) shouldRetry(err error) bool {
	// all retries are handled internally
	return false
}

func deepUnwrapError(err error) error {
	if err == nil {
		return nil
	}
	err2 := errors.Unwrap(err)
	for err2 != nil {
		err = err2
		err2 = errors.Unwrap(err)
	}
	return err
}

func (c *ydbkvClient) txn(f func(*kvTxn) error, retry int) error {
	ydbContext, ctxCloseFn := context.WithCancel(context.Background())
	defer ctxCloseFn()

	var algoErr error = nil

	err := c.con.Table().DoTx(
		ydbContext,
		func(ctx context.Context, tx table.TransactionActor) error {
			data := ydbkvTxn{&c.queries, ctx, tx, nil, nil}
			algoErr = func() (err error) {
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
				return f(&kvTxn{&data, 0})
			}()
			if algoErr != nil {
				algoErr = deepUnwrapError(algoErr)
				if _, ok := algoErr.(syscall.Errno); ok {
					// Avoid session drops on logical errors
					return nil
				}
				return algoErr
			}
			return data.applyChanges()
		},
		table.WithIdempotent(),
	)
	if algoErr != nil {
		return deepUnwrapError(algoErr)
	}
	return deepUnwrapError(err)
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
				var k *[]byte
				var v *[]byte
				if err = data.Scan(&k, &v); err != nil {
					return err
				}
				handler(unnestBytes(k), unnestBytes(v))
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
