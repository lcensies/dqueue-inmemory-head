package main

import (
	"github.com/joncrlsn/dque"
	"github.com/pkg/errors"
	"path"
	"unsafe"
)

type DiskOrderedStorage[T any] struct {
	path            string
	name            string
	builderFunc     func() *T
	itemsPerSegment int
	dq              *dque.DQue
}

func (diskQueue *DiskOrderedStorage[T]) Flush() error {
	return diskQueue.Sync()
}

var NotOpenedErr = errors.New("Queue is not opened")

func (diskQueue *DiskOrderedStorage[T]) Len() int {
	return diskQueue.dq.Size()
}

func (diskQueue *DiskOrderedStorage[T]) Sync() error {
	if diskQueue.dq == nil {
		return NotOpenedErr
	}

	if diskQueue.dq.Turbo() {
		return diskQueue.dq.TurboSync()
	}

	return nil
}

func (diskQueue *DiskOrderedStorage[T]) Enqueue(v *T) error {
	if diskQueue.dq == nil {
		return NotOpenedErr
	}

	return diskQueue.dq.Enqueue(v)
}

func (diskQueue *DiskOrderedStorage[T]) Dequeue() (*T, error) {
	if diskQueue.dq == nil {
		return nil, NotOpenedErr
	}

	v, err := diskQueue.dq.Dequeue()

	if v == nil {
		return nil, err
	}

	return v.(*T), err
}

func (diskQueue *DiskOrderedStorage[T]) Open(storeAt string) error {
	if diskQueue.dq != nil {
		return nil
	}

	diskQueue.path = path.Dir(storeAt)
	diskQueue.name = path.Base(storeAt)

	dq, err := dque.NewOrOpen(diskQueue.name, diskQueue.path, diskQueue.itemsPerSegment, func() interface{} {
		return diskQueue.builderFunc()
	})

	if err != nil {
		return errors.Wrap(err, "Unable to init driver")
	}

	diskQueue.dq = dq
	return nil
}

func (diskQueue *DiskOrderedStorage[T]) Close() error {
	if diskQueue.dq == nil {
		return NotOpenedErr
	}

	_ = diskQueue.Sync()
	return diskQueue.dq.Close()
}

func (diskQueue *DiskOrderedStorage[T]) TurboOn() error {
	return diskQueue.dq.TurboOn()
}

func (diskQueue *DiskOrderedStorage[T]) TurboOff() error {
	return diskQueue.dq.TurboOff()
}

func NewDiskOrderedQueue[T any](builder func() *T, RAMLimit ...int) (*DiskOrderedStorage[T], error) {
	diskQueue := DiskOrderedStorage[T]{
		dq:              nil,
		path:            "",
		name:            "",
		itemsPerSegment: 0,
		builderFunc:     builder,
	}
	if len(RAMLimit) > 0 {
		var t T
		structSize := int(unsafe.Sizeof(t))
		diskQueue.itemsPerSegment = RAMLimit[0] / structSize / 2
	}
	if diskQueue.itemsPerSegment <= 0 {
		diskQueue.itemsPerSegment = 1
	}
	return &diskQueue, nil
}
