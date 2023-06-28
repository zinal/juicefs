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
	m := &kvMeta{
		baseMeta: newBaseMeta("memkv", testConfig()),
		client:   withTracer(client, "trace-TestMemClient.txt"),
	}
	m.en = m
	testMeta(t, m)
	client.close()
}

func TestMem(t *testing.T) { //skip mutate
	c, err := newMockClient("")
	if err != nil {
		t.Fatal(err)
	}
	c = withTracer(c, "trace-TestMem.txt")
	testTKV(t, c)
	c.close()
}
