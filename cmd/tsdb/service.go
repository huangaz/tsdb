package main

import (
	"flag"
	"time"

	"github.com/huangaz/tsdb"
	"github.com/yubo/gotool/flags"
)

func init() {
	flags.NewCommand("start", "start service", start, flag.ExitOnError)
}

func start(arg interface{}) {
	t := tsdb.TsdbService{}
	t.Start()
	for {
		putreq := tsdb.DataGenerator(1, 10)
		t.Put(putreq)
		time.Sleep(5 * time.Second)
	}
}

func main() {
	flags.Parse()
	flags.Exec()
}
