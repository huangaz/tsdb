package tsdb

// Build a TimeSeriesBlock from the given data points.
func WriteValues(dps []TimeValuePair) (block TimeSeriesBlock) {
	s := NewSeries(nil)

	for _, dp := range dps {
		err := s.Append(dp.Timestamp, dp.Value, 0)
		if err == nil {
			block.Count++
		}
	}

	block.Data = s.ReadData()
	return block
}
