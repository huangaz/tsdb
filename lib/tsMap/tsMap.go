package tsMap

type TsMap struct {
	Map_ map[string]int
}

func NewTsmap() *TsMap {
	return &TsMap{Map_: make(map[string]int)}
}
