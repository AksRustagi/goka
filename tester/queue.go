package tester

import (
	"sync"
)

type message struct {
	offset int64
	key    string
	value  []byte
}

type queue struct {
	sync.Mutex
	topic    string
	messages []*message
	hwm      int64
}

func newQueue(topic string) *queue {

	return &queue{
		topic: topic,
	}
}

func (q *queue) Hwm() int64 {
	q.Lock()
	defer q.Unlock()

	hwm := q.hwm
	return hwm
}

func (q *queue) push(key string, value []byte) {
	q.Lock()
	defer q.Unlock()
	q.messages = append(q.messages, &message{
		offset: q.hwm,
		key:    key,
		value:  value,
	})
	q.hwm++
}
