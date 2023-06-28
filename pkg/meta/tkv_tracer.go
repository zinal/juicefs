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
)

type tracerTxn struct {
	*kvTxn
	trc *os.File
}

func (tx *tracerTxn) get(key []byte) []byte {
	fmt.Fprintln(tx.trc, "BEGIN get()", key)
	out := tx.kvTxn.get(key)
	fmt.Fprintln(tx.trc, "END out()", out)
	return out
}

func (tx *tracerTxn) gets(keys ...[]byte) [][]byte {
	fmt.Fprintln(tx.trc, "BEGIN gets()", keys)
	out := tx.kvTxn.gets(keys...)
	fmt.Fprintln(tx.trc, "END gets()", out)
	return out
}

func (tx *tracerTxn) scan(begin, end []byte, keysOnly bool, handler func(k, v []byte) bool) {
	fmt.Fprintln(tx.trc, "BEGIN scan()", begin, end, keysOnly)
	tx.kvTxn.scan(begin, end, keysOnly, func(k, v []byte) bool {
		return handler(k, v)
	})
	fmt.Fprintln(tx.trc, "END scan()")
}

func (tx *tracerTxn) exist(prefix []byte) bool {
	fmt.Fprintln(tx.trc, "BEGIN append()", prefix)
	out := tx.kvTxn.exist(prefix)
	return out
}

func (tx *tracerTxn) set(key, value []byte) {
	fmt.Fprintln(tx.trc, "BEGIN set()", key, value)
	tx.kvTxn.set(key, value)
	fmt.Fprintln(tx.trc, "END set()")
}

func (tx *tracerTxn) append(key []byte, value []byte) []byte {
	fmt.Fprintln(tx.trc, "BEGIN append()", key, value)
	out := tx.kvTxn.append(key, value)
	fmt.Fprintln(tx.trc, "END append()", out)
	return out
}

func (tx *tracerTxn) incrBy(key []byte, value int64) int64 {
	fmt.Fprintln(tx.trc, "BEGIN incrBy()", key)
	out := tx.kvTxn.incrBy(key, value)
	fmt.Fprintln(tx.trc, "END incrBy()", out)
	return out
}

func (tx *tracerTxn) delete(key []byte) {
	fmt.Fprintln(tx.trc, "BEGIN delete()", key)
	tx.kvTxn.delete(key)
	fmt.Fprintln(tx.trc, "END delete()")
}

type tracerClient struct {
	tkvClient
	trc *os.File
}

func (c *tracerClient) txn(f func(*kvTxn) error, retry int) error {
	fmt.Fprintln(c.trc, "BEGIN Ctxn()", retry)
	err := c.tkvClient.txn(func(tx *kvTxn) error {
		return f(&kvTxn{&tracerTxn{tx, c.trc}, retry})
	}, retry)
	fmt.Fprintln(c.trc, "END Ctxn()", err)
	return err
}

func (c *tracerClient) scan(prefix []byte, handler func(key, value []byte)) error {
	fmt.Fprintln(c.trc, "BEGIN Cscan()", prefix)
	err := c.tkvClient.scan(prefix, func(key, value []byte) {
		fmt.Fprintln(c.trc, "OUT Cscan()", key, value)
		handler(key, value)
	})
	fmt.Fprintln(c.trc, "END Cscan()", err)
	return err
}

func (c *tracerClient) reset(prefix []byte) error {
	fmt.Fprintln(c.trc, "BEGIN Creset()", prefix)
	err := c.tkvClient.reset(prefix)
	fmt.Fprintln(c.trc, "END Creset()", err)
	return err
}

func (c *tracerClient) close() error {
	if c.trc != nil {
		fmt.Fprintln(c.trc, "Tracing stopped")
		c.trc.Close()
		c.trc = nil
	}
	return c.tkvClient.close()
}

func (c *tracerClient) gc() {
	c.tkvClient.gc()
}

func withTracer(client tkvClient, traceFile string) tkvClient {
	trcFile, err := os.Create(traceFile)
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(trcFile, "Tracing started")
	return &tracerClient{client, trcFile}
}
