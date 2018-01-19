package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/testUtil"
)

func TestPutAndGet(t *testing.T) {
	defer testUtil.FileDelete()
	tsdb := &TsdbService{}
	err := tsdb.Start()
	if err != nil {
		t.Fatal(err)
	}

	num := 10
	putReq := testUtil.DataGenerator(1, num)

	time.Sleep(1000 * time.Millisecond)

	putRes, err := tsdb.Put(putReq)
	if err != nil {
		t.Fatal(err)
	}
	if putRes.N != int32(num) {
		t.Fatalf("Put failed! Want %d,put %d", num, putRes.N)
	}

	time.Sleep(1000 * time.Millisecond)

	getReq := &dataTypes.GetRequest{
		Begin:   0,
		End:     int64(60 * (num + 1)),
		ShardId: 1,
		Key:     putReq.Datas[0].Key,
	}
	getRes, err := tsdb.Get(getReq)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(putReq)
	fmt.Println(getRes)

	if string(getRes.Key) != string(putReq.Datas[0].Key) {
		t.Fatal("wrong result")
	}

	if len(putReq.Datas) != len(getRes.Dps) {
		t.Fatalf("Length of putReq(%d) and getRes(%d) not equal!", len(putReq.Datas), len(getRes.Dps))
	}

	for i, data := range putReq.Datas {
		if data.Value != getRes.Dps[i].Value || data.Timestamp != getRes.Dps[i].Timestamp {
			t.Fatal("wrong result")
		}
	}

}
