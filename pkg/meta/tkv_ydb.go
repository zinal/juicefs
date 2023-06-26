//go:build !noydbkv
// +build !noydbkv

/*
 * JuiceFS, Copyright 2021 Juicedata, Inc.
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
	rs, err := tx.actor.Execute(tx.ctx, tx.queries.selectOne,
		table.NewQueryParameters(
			table.ValueParam("k", types.BytesValue(key)),
		), options.WithKeepInCache(true),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = rs.Close()
	}()
	if rs.NextResultSet(tx.ctx) {
		if rs.NextRow() {
			var value []byte
			if err = rs.Scan(&value); err != nil {
				panic(err)
			}
			return value
		}
	}
	return nil
}

func (tx *ydbkvTxn) gets(keys ...[]byte) [][]byte {
	if len(keys) > 128 {
		var rs = make([][]byte, 0, len(keys))
		for i := 0; i < len(keys); i += 128 {
			rs = append(rs, tx.gets(keys[i:min(i+128, len(keys))]...)...)
		}
		return rs
	}
	return nil
}

func (tx *ydbkvTxn) scan(begin, end []byte, keysOnly bool, handler func(k, v []byte) bool) {
}

func (tx *ydbkvTxn) exist(prefix []byte) bool {
	if tx.vput != nil {
		for k, _ := range tx.vput {
			if strings.HasPrefix(k, string(prefix)) {
				return true
			}
		}
	}
	rs, err := tx.actor.Execute(tx.ctx, tx.queries.existsPrefix,
		table.NewQueryParameters(
			table.ValueParam("p", types.BytesValue(prefix)),
		), options.WithKeepInCache(true),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = rs.Close()
	}()
	if rs.NextResultSet(tx.ctx) {
		if rs.NextRow() {
			var key []byte
			if err = rs.Scan(&key); err != nil {
				panic(err)
			}
			return bytes.HasPrefix(key, prefix)
		}
	}
	return false
}

func (tx *ydbkvTxn) set(key, value []byte) {
}

func (tx *ydbkvTxn) append(key []byte, value []byte) []byte {
	old := tx.get(key)
	new := append(old, value...)
	tx.set(key, new)
	return new
}

func (tx *ydbkvTxn) incrBy(key []byte, value int64) int64 {
	return 0
}

func (tx *ydbkvTxn) delete(key []byte) {
}

func (tx *ydbkvTxn) applyChanges() {
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
	deleteAll    string
	deleteRange  string
	deleteSome   string
	upsertSome   string
	selectOne    string
	selectSome   string
	existsPrefix string
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
	c.queries.deleteAll = expandQuery(tableName, "")
	c.queries.deleteRange = expandQuery(tableName, "")
	c.queries.deleteSome = expandQuery(tableName, "")
	c.queries.upsertSome = expandQuery(tableName, "")
	c.queries.selectOne = expandQuery(tableName, `DECLARE $k AS String; SELECT v FROM {kvtable} WHERE k=$k;`)
	c.queries.selectSome = expandQuery(tableName, "")
	c.queries.existsPrefix = expandQuery(tableName, `DECLARE $p AS String; SELECT k FROM {kvtable} WHERE k>=$p LIMIT 1;`)
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
		func(ctx context.Context, tx table.TransactionActor) error {
			data := ydbkvTxn{&c.queries, ctx, tx, nil, nil}
			err := f(&kvTxn{&data, 0})
			if err != nil {
				return err
			}
			data.applyChanges()
			return nil
		},
		table.WithIdempotent(),
	)
}

func (c *ydbkvClient) scan(prefix []byte, handler func(key, value []byte)) error {
	return nil
}

func (c *ydbkvClient) reset(prefix []byte) (err error) {
	ydbContext, ctxCloseFn := context.WithCancel(context.Background())
	defer ctxCloseFn()

	if len(prefix) == 0 {
		// replace the table with an empty one
		tableName := c.tableName + "_empty"
		err = ydbCreateKvTable(ydbContext, c.con, tableName)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (c *ydbkvClient) close() error {
	return nil
}

func (c *ydbkvClient) gc() {
	// noop
}
