package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/huangaz/tsdb"
)

var profile = flag.String("profile", "/tmp/prof.out", "write cpu profile to file")

func main() {
	flag.Parse()
	f, err := os.Create(*profile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	defer pprof.WriteHeapProfile(f)

	ts := &tsdb.TsdbService{}
	err = ts.Start()
	if err != nil {
		log.Fatal("tsdb start error!")
	}

	num := 10
	putReq := tsdb.DataGenerator(1, num)

	time.Sleep(time.Second)

	_, err = ts.Put(putReq)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second)

	getReq := &tsdb.GetRequest{
		Begin: 0,
		End:   int64(60 * (num + 1)),
		Keys: []*tsdb.Key{
			&tsdb.Key{
				ShardId: 1,
				Key:     putReq.Data[0].Key.Key,
			},
		},
	}
	getRes, err := ts.Get(getReq)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(putReq)
	fmt.Println(getRes)
}
