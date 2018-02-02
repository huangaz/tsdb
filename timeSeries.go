package tsdb

// Build a TimeSeriesBlock from the given data points.
func WriteValues(dps []*TimeValuePair) *TimeSeriesBlock {
	s := NewSeries(nil)

	res := &TimeSeriesBlock{}

	for _, dp := range dps {
		err := s.Append(dp.Timestamp, dp.Value, 0)
		if err == nil {
			res.Count++
		}
	}

	res.Data = s.ReadData()
	return res
}
