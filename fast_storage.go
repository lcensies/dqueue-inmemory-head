package main

import (
	"sync"
	"time"
)

type FastDiskOrderedStorage[T any] struct {
	writesPending       bool
	workerIsActive      bool
	quitChan            chan struct{}
	flushInterval       time.Duration
	writesPendingMutex  sync.Mutex
	workerIsActiveMutex sync.Mutex
	Disk                *DiskOrderedStorage[T]
}

func NewFastDiskOrderedQueue[T any](disk *DiskOrderedStorage[T], flushInterval ...time.Duration) *FastDiskOrderedStorage[T] {
	storage := &FastDiskOrderedStorage[T]{Disk: disk}
	if len(flushInterval) > 0 {
		storage.flushInterval = flushInterval[0]
	} else {
		storage.flushInterval = 10 * time.Millisecond
	}
	return storage
}

func (f *FastDiskOrderedStorage[T]) Open(path string) error {
	err := f.Disk.Open(path)
	if err != nil {
		return err
	}
	err = f.Disk.dq.TurboOn()
	if err != nil {
		return err
	}
	f.startFlushWorker(f.flushInterval)
	return err
}

func (f *FastDiskOrderedStorage[T]) Close() error {
	err := f.Disk.Close()
	if err != nil {
		return err
	}
	f.writesPendingMutex.Lock()
	f.writesPending = false
	f.writesPendingMutex.Unlock()
	f.stopFlushWorker()
	return err
}

func (f *FastDiskOrderedStorage[T]) Sync() error {
	err := f.Disk.Sync()
	if err != nil {
		return err
	}
	f.writesPendingMutex.Lock()
	f.writesPending = false
	f.writesPendingMutex.Unlock()
	return err
}

func (f *FastDiskOrderedStorage[T]) Flush() error {
	return f.Sync()
}

func (f *FastDiskOrderedStorage[T]) Enqueue(v *T) error {
	err := f.Disk.Enqueue(v)
	if err != nil {
		return err
	}
	f.writesPendingMutex.Lock()
	f.writesPending = true
	f.writesPendingMutex.Unlock()
	return err
}

func (f *FastDiskOrderedStorage[T]) Dequeue() (*T, error) {
	item, err := f.Disk.Dequeue()
	if err != nil {
		return item, err
	}
	f.writesPendingMutex.Lock()
	f.writesPending = true
	f.writesPendingMutex.Unlock()
	return item, err
}

func (f *FastDiskOrderedStorage[T]) Len() int {
	return f.Disk.Len()
}

func (f *FastDiskOrderedStorage[T]) startFlushWorker(flushInterval time.Duration) {
	var err error
	ticker := time.NewTicker(flushInterval)
	f.quitChan = make(chan struct{})
	f.workerIsActive = true
	go func() {
		for {
			select {
			case <-ticker.C:
				f.writesPendingMutex.Lock()
				writesPending := f.writesPending
				f.writesPendingMutex.Unlock()
				if writesPending {
					err = f.Flush()
				}
				if err != nil {
					f.stopFlushWorker()
				}
			case <-f.quitChan:
				ticker.Stop()
				return
			}
		}
	}()
}

func (f *FastDiskOrderedStorage[T]) stopFlushWorker() {
	f.workerIsActiveMutex.Lock()
	if f.workerIsActive {
		f.workerIsActive = false
		close(f.quitChan)
	}
	f.workerIsActiveMutex.Unlock()
}
