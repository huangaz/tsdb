package tsdb

import (
	"container/heap"
)

type IntHeap []int

type PriorityQueue struct {
	IntHeap
}

func NewPriorityQueue() *PriorityQueue {
	res := &PriorityQueue{}
	heap.Init(&res.IntHeap)
	return res
}

func (p *PriorityQueue) Push(x int) {
	heap.Push(&p.IntHeap, x)
}

func (p *PriorityQueue) Pop() int {
	return heap.Pop(&p.IntHeap).(int)
}

func (p *PriorityQueue) Len() int {
	return p.IntHeap.Len()
}

func (h IntHeap) Len() int {
	return len(h)
}

func (h IntHeap) Less(i, j int) bool {
	return h[i] < h[j]
}

func (h IntHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *IntHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

/*
type Item struct {
        value    string
        priority int
        index    int
}

type PriorityQueue []*Item

func (p PriorityQueue) Len() int {
        return len(p)
}

func (p PriorityQueue) Less(i, j int) bool {
        return p[i].priority > p[j].priority
}

func (p PriorityQueue) Swap(i, j int) {
        p[i], p[j] = p[j], p[i]
        p[i].index = i
        p[j].index = j
}

func (p *PriorityQueue) Push(x interface{}) {
        n := len(*p)
        item := x.(*Item)
        item.index = n
        *p = append(*p, item)
}

func (p *PriorityQueue) Pop() interface{} {
        old := *p
        n := len(old)
        item := old[n-1]
        item.index = -1
        *p = old[0 : n-1]
        return item
}

func (p *PriorityQueue) update(item *Item, value string, priority int) {
        item.value = value
        item.priority = priority
        heap.Fix(p, item.index)
}
*/
