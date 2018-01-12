package timeSeriesStream

import (
	"github.com/huangaz/tsdb/lib/testUtil"
	"testing"
)

var TestData = testUtil.TestData

func TestAppendAndRead(t *testing.T) {
	s := NewSeries()
	for _, p := range TestData {
		err := s.Append(p.Timestamp, p.Value, 1)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i, p := range TestData {
		timestamp, value, err := s.Read()
		if err != nil {
			t.Fatal(err)
		}
		if p.Timestamp != timestamp || p.Value != value {
			t.Fatalf("No.%d get (%v,%v),want (%v,%v)\n", i, timestamp, value, p.Timestamp, p.Value)
		}
	}

	s.Reset()
	if len(s.Bs.Stream) != 0 {
		t.Fatal("Reset failed!")
	}
}
