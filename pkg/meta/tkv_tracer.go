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
	"sync"
)

type tracerTxn struct {
	*kvTxn
	owner *tracerClient
}

func (tx *tracerTxn) get(key []byte) []byte {
	tx.owner.turboLog("BEGIN get()", key)
	out := tx.kvTxn.get(key)
	tx.owner.turboLog("END out()", out)
	return out
}

func (tx *tracerTxn) gets(keys ...[]byte) [][]byte {
	tx.owner.turboLog("BEGIN gets()", keys)
	out := tx.kvTxn.gets(keys...)
	tx.owner.turboLog("END gets()", out)
	return out
}

func (tx *tracerTxn) scan(begin, end []byte, keysOnly bool, handler func(k, v []byte) bool) {
	tx.owner.turboLog("BEGIN scan()", begin, end, keysOnly)
	tx.kvTxn.scan(begin, end, keysOnly, func(k, v []byte) bool {
		return handler(k, v)
	})
	tx.owner.turboLog("END scan()")
}

func (tx *tracerTxn) exist(prefix []byte) bool {
	tx.owner.turboLog("BEGIN append()", prefix)
	out := tx.kvTxn.exist(prefix)
	return out
}

func (tx *tracerTxn) set(key, value []byte) {
	tx.owner.turboLog("BEGIN set()", key, value)
	tx.kvTxn.set(key, value)
	tx.owner.turboLog("END set()")
}

func (tx *tracerTxn) append(key []byte, value []byte) []byte {
	tx.owner.turboLog("BEGIN append()", key, value)
	out := tx.kvTxn.append(key, value)
	tx.owner.turboLog("END append()", out)
	return out
}

func (tx *tracerTxn) incrBy(key []byte, value int64) int64 {
	tx.owner.turboLog("BEGIN incrBy()", key, value)
	out := tx.kvTxn.incrBy(key, value)
	tx.owner.turboLog("END incrBy()", out)
	return out
}

func (tx *tracerTxn) delete(key []byte) {
	tx.owner.turboLog("BEGIN delete()", key)
	tx.kvTxn.delete(key)
	tx.owner.turboLog("END delete()")
}

type tracerClient struct {
	sync.Mutex
	tkvClient
	trc *os.File
}

func (c *tracerClient) txn(f func(*kvTxn) error, retry int) error {
	c.turboLog("BEGIN Ctxn()", retry)
	err := c.tkvClient.txn(func(tx *kvTxn) error {
		return f(&kvTxn{&tracerTxn{tx, c}, retry})
	}, retry)
	c.turboLog("END Ctxn()", err)
	return err
}

func (c *tracerClient) scan(prefix []byte, handler func(key, value []byte)) error {
	c.turboLog("BEGIN Cscan()", prefix)
	err := c.tkvClient.scan(prefix, func(key, value []byte) {
		c.turboLog("OUT Cscan()", key, value)
		handler(key, value)
	})
	c.turboLog("END Cscan()", err)
	return err
}

func (c *tracerClient) reset(prefix []byte) error {
	c.turboLog("BEGIN Creset()", prefix)
	err := c.tkvClient.reset(prefix)
	c.turboLog("END Creset()", err)
	return err
}

func (c *tracerClient) close() error {
	if c.trc != nil {
		c.turboLog("Tracing stopped")
		c.trc.Close()
		c.trc = nil
	}
	return c.tkvClient.close()
}

func (c *tracerClient) gc() {
	c.tkvClient.gc()
}

func (c *tracerClient) turboLog(a ...any) {
	c.Lock()
	defer func() { c.Unlock() }()
	if c.trc != nil {
		fmt.Fprintln(c.trc, a...)
	}
}

func withTracer(client tkvClient, traceFile string) tkvClient {
	trcFile, err := os.Create(traceFile)
	if err != nil {
		panic(err)
	}
	ret := &tracerClient{sync.Mutex{}, client, trcFile}
	ret.turboLog("Tracing started")
	return ret
}
