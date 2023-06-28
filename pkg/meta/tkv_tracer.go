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
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type tracerTxn struct {
	*kvTxn
	owner *tracerClient
}

func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func (tx *tracerTxn) get(key []byte) []byte {
	gid := goid()
	logger.Infof("%d BEGIN get() %v", gid, key)
	out := tx.kvTxn.get(key)
	logger.Infof("%d END get() %v -> %v", gid, key, out)
	return out
}

func (tx *tracerTxn) gets(keys ...[]byte) [][]byte {
	gid := goid()
	logger.Infof("%d BEGIN gets() %v", gid, keys)
	out := tx.kvTxn.gets(keys...)
	logger.Infof("%d END gets() %v", gid, out)
	return out
}

func (tx *tracerTxn) scan(begin, end []byte, keysOnly bool, handler func(k, v []byte) bool) {
	gid := goid()
	logger.Infof("%d BEGIN scan() %v %v", gid, begin, end)
	tx.kvTxn.scan(begin, end, keysOnly, func(k, v []byte) bool {
		logger.Infof("%d DATA scan() %v %v", gid, k, v)
		return handler(k, v)
	})
	logger.Infof("%d END scan() %v %v", gid, begin, end)
}

func (tx *tracerTxn) exist(prefix []byte) bool {
	gid := goid()
	logger.Infof("%d BEGIN exists() %v", gid, prefix)
	out := tx.kvTxn.exist(prefix)
	logger.Infof("%d END exists() %v -> %v", gid, prefix, out)
	return out
}

func (tx *tracerTxn) set(key, value []byte) {
	gid := goid()
	logger.Infof("%d BEGIN set() %v %v", gid, key, value)
	tx.kvTxn.set(key, value)
	logger.Infof("%d END set() %v %v", gid, key, value)
}

func (tx *tracerTxn) append(key []byte, value []byte) []byte {
	gid := goid()
	logger.Infof("%d BEGIN append() %v %v", gid, key, value)
	out := tx.kvTxn.append(key, value)
	logger.Infof("%d END append() %v -> %v", gid, key, out)
	return out
}

func (tx *tracerTxn) incrBy(key []byte, value int64) int64 {
	gid := goid()
	logger.Infof("%d BEGIN incrBy() %v %v", gid, key, value)
	out := tx.kvTxn.incrBy(key, value)
	logger.Infof("%d END incrBy() %v -> %v", gid, key, out)
	return out
}

func (tx *tracerTxn) delete(key []byte) {
	gid := goid()
	logger.Infof("%d BEGIN delete() %v", gid, key)
	tx.kvTxn.delete(key)
	logger.Infof("%d END delete() %v", gid, key)
}

type tracerClient struct {
	sync.Mutex
	tkvClient
	trc *os.File
}

func (c *tracerClient) txn(f func(*kvTxn) error, retry int) error {
	gid := goid()
	logger.Infof("%d BEGIN Ctxn()", gid)
	err := c.tkvClient.txn(func(tx *kvTxn) error {
		return f(&kvTxn{&tracerTxn{tx, c}, retry})
	}, retry)
	logger.Infof("%d END Ctxn() %v", gid, err)
	return err
}

func (c *tracerClient) scan(prefix []byte, handler func(key, value []byte)) error {
	gid := goid()
	logger.Infof("%d BEGIN Cscan() %v", gid, prefix)
	err := c.tkvClient.scan(prefix, func(key, value []byte) {
		logger.Infof("%d OUT Cscan() %v -> %v", gid, key, value)
		handler(key, value)
	})
	logger.Infof("%d END Cscan() %v", gid, err)
	return err
}

func (c *tracerClient) reset(prefix []byte) error {
	gid := goid()
	logger.Infof("%d BEGIN Creset() %v", gid, prefix)
	err := c.tkvClient.reset(prefix)
	logger.Infof("%d END Creset() %v", gid, err)
	return err
}

func (c *tracerClient) close() error {
	if c.trc != nil {
		logger.Info("Tracing stopped")
		c.trc.Close()
		c.trc = nil
	}
	return c.tkvClient.close()
}

func (c *tracerClient) gc() {
	c.tkvClient.gc()
}

/*
func (c *tracerClient) turboLog(a ...any) {
	c.Lock()
	defer func() { c.Unlock() }()
	if c.trc != nil {
		fmt.Fprintln(c.trc, a...)
	}
}
*/

func withTracer(client tkvClient, traceFile string) tkvClient {
	trcFile, err := os.Create(traceFile)
	if err != nil {
		panic(err)
	}
	ret := &tracerClient{sync.Mutex{}, client, trcFile}
	logger.Info("Tracing started")
	return ret
}
