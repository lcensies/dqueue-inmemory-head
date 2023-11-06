package main

import (
	"github.com/google/uuid"
	"log"
	"os"
	"path"
	"testing"
	"time"
	"unsafe"
)

func testTime(test *testing.T, storage OrderedStorage[TestStruct], suffix string, maxExecution time.Duration) {
	wd, _ := os.Getwd()

	shouldBe := 10000
	nowInQueue := 0

	err := storage.Open(path.Join(wd, "storage-"+suffix))
	defer storage.Close()

	if err != nil {
		test.Fatal(err)
	}

	ids := Queue[string]{}

	t1 := time.Now()

	for nowInQueue = 0; nowInQueue < shouldBe; nowInQueue++ {
		id := uuid.NewString()
		err := ids.Enqueue(&id)

		if err != nil {
			test.Fatal(err)
		}

		s := TestStruct{
			Uuid: id,
		}

		err = storage.Enqueue(&s)

		if err != nil {
			test.Fatal(err)
		}
	}

	decoded := 0
	for ; nowInQueue > 0; nowInQueue-- {
		s, err := storage.Dequeue()

		if err != nil {
			log.Printf("EQ size %d", nowInQueue)
			test.Fatal(err)
		}

		id, iderr := ids.Dequeue()

		if iderr != nil {
			test.Fatal(iderr)
		}

		decoded++

		if s.Uuid != *id {
			test.Fatalf("Decoded event (%d) (%s) != evevnt id (%s)", decoded, s.Uuid, *id)
		}
	}

	total := time.Since(t1)

	if maxExecution < total {
		test.Fatalf("Too long execution time: %+v", total)
	}
}

type TestStruct struct {
	Uuid string
}

func TestStorage(test *testing.T) {
	storage, err := NewDiskOrderedQueue(func() *TestStruct {
		return &TestStruct{}
	})

	if err != nil {
		test.Fatal(err)
	}

	testTime(test, storage, "slow", 40*time.Second)
}

func TestFastDiskStorage(test *testing.T) {
	storage, err := NewDiskOrderedQueue(func() *TestStruct {
		return &TestStruct{}
	})

	if err != nil {
		test.Fatal(err)
	}

	fastStorage := NewFastDiskOrderedQueue(storage, 10*time.Millisecond)
	testTime(test, fastStorage, "fast", 10*time.Second)
}

func TestRAMLimit(test *testing.T) {
	RAMLimit := 32 * 1024
	shouldBe := 3000
	var err error
	wd, _ := os.Getwd()
	testStructSize := int(unsafe.Sizeof(TestStruct{uuid.NewString()}))
	storagePath := path.Join(wd, "storage-limited")

	storage, err := NewDiskOrderedQueue(func() *TestStruct {
		return &TestStruct{}
	}, RAMLimit)
	if err != nil {
		test.Fatal(err)
	}

	fastStorage := NewFastDiskOrderedQueue(storage)
	err = fastStorage.Open(storagePath)
	defer os.RemoveAll(storagePath)
	defer fastStorage.Close()

	for i := 0; i < shouldBe; i++ {
		err = fastStorage.Enqueue(&TestStruct{uuid.NewString()})
		if err != nil {
			test.Fatal(err)
		}
	}

	files, err := os.ReadDir(storagePath)

	if err != nil {
		test.Fatal(err)
	}

	for i := 0; i < shouldBe; i++ {
		_, err = fastStorage.Dequeue()
		if err != nil {
			test.Fatal(err)
		}
	}

	expectedSegments := shouldBe / (RAMLimit / testStructSize / 2)
	// -2 because of links to current and parent directories
	nSegments := len(files) - 2

	if expectedSegments != nSegments {
		test.Fatalf("Expected N of memory segments (%d) != N of files (%d)", expectedSegments, nSegments)
	}
}
