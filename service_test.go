package tsdb

import (
	"sort"
	"testing"
	"time"
)

func TestServicePutAndGet(t *testing.T) {
	defer FileDelete()
	ts := &TsdbService{}
	err := ts.Start()
	if err != nil {
		t.Fatal(err)
	}

	num := 10
	putReq := DataGenerator(1, num)

	time.Sleep(1000 * time.Millisecond)

	putRes, err := ts.Put(putReq)
	if err != nil {
		t.Fatal(err)
	}
	if putRes.N != int32(num) {
		t.Fatalf("Put failed! Want %d,put %d", num, putRes.N)
	}

	time.Sleep(1000 * time.Millisecond)

	getReq := &GetRequest{
		Begin: 0,
		End:   int64(60 * (num + 1)),
		Keys: []*Key{
			&Key{
				ShardId: 1,
				Key:     putReq.Data[0].Key.Key,
			},
		},
	}
	getRes, err := ts.Get(getReq)
	if err != nil {
		t.Fatal(err)
	}

	// fmt.Println(putReq)
	// fmt.Println(getRes)

	if string(getRes.Data[0].Key.Key) != string(putReq.Data[0].Key.Key) {
		t.Fatal("wrong result")
	}

	if len(putReq.Data) != len(getRes.Data[0].Values) {
		t.Fatalf("Length of putReq(%d) and getRes(%d) not equal!", len(putReq.Data), len(getRes.Data[0].Values))
	}

	for i, dp := range putReq.Data {
		if dp.Value.Value != getRes.Data[0].Values[i].Value ||
			dp.Value.Timestamp != getRes.Data[0].Values[i].Timestamp {
			t.Fatal("wrong result")
		}
	}
}

func TestServiceReload(t *testing.T) {

	ts := &TsdbService{}
	err := ts.Start()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	ts.reload()
	time.Sleep(100 * time.Millisecond)

	var newMap []int
	for k, v := range ts.buckets {
		if v.GetState() == OWNED {
			newMap = append(newMap, k)
		}
	}
	sort.Ints(newMap)
	wantMap := []int{1, 3, 5, 7}
	// fmt.Println("newMap: ", newMap)
	// fmt.Println("wantMap: ", wantMap)

	if len(wantMap) != len(newMap) {
		t.Fatal("wrong result of reload()")
	}
	for i := 0; i < len(wantMap); i++ {
		if wantMap[i] != newMap[i] {
			t.Fatal("wrong result of reload()")
		}
	}
}
