package tsdbPb

import fmt "fmt"

func (p *PutRequest) PrintForDebug() string {
	var res string
	res += fmt.Sprintf("--PutRequest--\n")
	for _, dp := range p.Datas {
		res += fmt.Sprintf("%4s%20s%10s%3d%8s%5d%8s%15f\n", "key:", string(dp.Key.Key),
			"shardId:", dp.Key.ShardId, "time:", dp.Value.Timestamp,
			"value:", dp.Value.Value)
	}
	return res
}

func (g *GetResponse) PrintForDebug() string {
	var res string
	res += fmt.Sprintf("--GetResponse--\n")
	for _, dp := range g.Datas {
		res += fmt.Sprintf("key: %s\n", string(dp.Key.Key))
		for _, v := range dp.Values {
			res += fmt.Sprintf("%10s%5d%8s%10f\n", "timestamp:", v.Timestamp,
				"value:", v.Value)
		}

	}
	return res
}
