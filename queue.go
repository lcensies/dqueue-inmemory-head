package main

import (
	"github.com/pkg/errors"
)

type Ordered[T any] interface {
	Enqueue(v *T) error
	Dequeue() (*T, error)
	Len() int
}

type Queue[T any] struct {
	data []*T
}

func (q *Queue[T]) Enqueue(v *T) error {
	q.data = append(q.data, v)
	return nil
}

func (q *Queue[T]) Dequeue() (*T, error) {
	if len(q.data) == 0 {
		return nil, errors.New("queue is empty")
	}

	value := q.data[0]
	q.data = q.data[1:]
	return value, nil
}

func (q *Queue[T]) Len() int {
	return len(q.data)
}

type OrderedStorage[T any] interface {
	Open(path string) error
	Sync() error
	Close() error
	Flush() error
	Ordered[T]
}
