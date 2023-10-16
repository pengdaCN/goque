package goque

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestAckQueue(t *testing.T) {
	ackQueue, err := OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if _, err := ackQueue.Enqueue([]byte(`xx1` + strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	for {
		item, err := ackQueue.Dequeue()
		if err != nil {
			t.Log(err)
			break
		}

		t.Log(keyToID(item.Key))
	}

	for _, id := range []uint64{1, 42, 29, 2, 19, 22, 6, 7, 19, 11} {
		if err := ackQueue.Submit(id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAckQueue2(t *testing.T) {
	ackQueue, err := OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if _, err := ackQueue.Enqueue([]byte(`xx1` + strconv.Itoa(i))); err != nil {
				t.Fatal(err)
			}

			time.Sleep(time.Millisecond * 200)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		var c int
		for {
			dequeue, err := ackQueue.Dequeue()
			if err != nil {
				if errors.Is(err, ErrOutOfBounds) {
					if c > 3 {
						return
					}

					c++

					time.Sleep(time.Second)
				}

				continue
			}

			c = 0

			t.Log(dequeue.ID)

			if dequeue.ID%2 == 0 {
				if err := ackQueue.Submit(dequeue.ID); err != nil {
					t.Fatal(err)
				}
			}
		}
	}()

	wg.Wait()
}

func TestAckQueue3(t *testing.T) {
	ackQueue, err := OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}

	for {
		item, err := ackQueue.Dequeue()
		if err != nil {
			t.Fatal(err)
		}

		t.Log(item.ID)
		if err := ackQueue.Submit(item.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAckQueue_BDequeue(t *testing.T) {
	ackQueue, err := OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 10; i++ {
			if _, err := ackQueue.Enqueue([]byte(`xx21` + strconv.Itoa(i+1))); err != nil {
				panic(err)
			}

			time.Sleep(time.Second)
		}

		ackQueue.HalfClose()
	}()

	for {
		item, err := ackQueue.BDequeue(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		t.Log(item.ID)
		if err := ackQueue.Submit(item.ID); err != nil {
			t.Fatal(err)
		}
	}
}
