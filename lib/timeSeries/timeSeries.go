package timeSeries

import (
	"github.com/huangaz/tsdb/lib/dataTypes"
	"github.com/huangaz/tsdb/lib/timeSeriesStream"
)

// Build a TimeSeriesBlock from the given data points.
func WriteValues(dps []dataTypes.DataPoint) (block dataTypes.TimeSeriesBlock) {
	s := timeSeriesStream.NewSeries(nil)

	for _, dp := range dps {
		err := s.Append(dp.Timestamp, dp.Value, 0)
		if err == nil {
			block.Count++
		}
	}

	block.Data = s.ReadData()
	return block
}
