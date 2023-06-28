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

//nolint:errcheck
package meta

import (
	"os"
	"testing"
)

func TestYdbClient(t *testing.T) { //skip mutate
	// export JUICEFS_YDB_URL='ydb.serverless.yandexcloud.net:2135/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5?tls=true&authMode=saKey&saKeyFile=/home/zinal/Keys/ydb-sa1-key1.json&tableName=testJuicefs2'
	// go test github.com/juicedata/juicefs/pkg/meta -run TestYdbClient -v
	testUrl := os.Getenv("JUICEFS_YDB_URL")
	if len(testUrl) == 0 {
		//return
		testUrl = "ydb.serverless.yandexcloud.net:2135/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5?tls=true&authMode=saKey&saKeyFile=/home/zinal/Keys/ydb-sa1-key1.json&tableName=testJuicefs3"
	}

	client, err := newTkvClient("ydbkv", testUrl)
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	//client = withTracer(client, "trace-YdbClient.txt")
	m := &kvMeta{
		baseMeta: newBaseMeta("ydbkv", testConfig()),
		client:   client,
	}
	m.en = m
	defer func() {
		client.close()
	}()
	testMeta(t, m)
}

func TestYdb(t *testing.T) { //skip mutate
	testUrl := os.Getenv("JUICEFS_YDB_URL")
	if len(testUrl) == 0 {
		return
	}
	client, err := newYdbClient(testUrl)
	if err != nil {
		t.Fatal(err)
	}
	//client = withTracer(client, "trace-YdbKv.txt")
	defer func() {
		client.close()
	}()
	testTKV(t, client)
}
