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

//nolint:errcheck
package meta

import (
	"testing"
)

func TestMemClient(t *testing.T) { //skip mutate
	client, err := newTkvClient("memkv", "")
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	client = withTracer(client, "trace-MemClient.txt")
	defer func() {
		client.close()
	}()
	m := &kvMeta{
		baseMeta: newBaseMeta("memkv", testConfig()),
		client:   client,
	}
	m.en = m
	testMeta(t, m)
}

func TestMem(t *testing.T) { //skip mutate
	client, err := newMockClient("")
	if err != nil {
		t.Fatal(err)
	}
	client = withTracer(client, "trace-MemKv.txt")
	defer func() {
		client.close()
	}()
	testTKV(t, client)
}
