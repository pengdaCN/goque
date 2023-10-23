package goque

import (
	"context"
	"errors"
	"log"
	"os"
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

	{
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for i := 0; i < 10; i++ {
					if _, err := ackQueue.Enqueue([]byte(`xx21` + strconv.Itoa(i+1))); err != nil {
						panic(err)
					}

					time.Sleep(time.Second)
				}

			}()
		}

		wg.Wait()
		if err := ackQueue.CloseWrite(); err != nil {
			panic(err)
		}
	}

	defer ackQueue.CloseRead()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)

		id := i
		go func() {
			defer wg.Done()
			for {
				item, err := ackQueue.BDequeue(context.Background())
				if err != nil {
					log.Println(err.Error())
					return
				}

				t.Log("id:", id, item.ID)
				if err := ackQueue.Submit(item.ID); err != nil {
					log.Println("id:", item.ID, "err:", err.Error())
					return
				}
			}

		}()
	}

	wg.Wait()
}

func TestAckQueue_BDequeue2(t *testing.T) {
	ackQueue, err := OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}

	defer ackQueue.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	item, err := ackQueue.BDequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(item.ID)
}

func TestOpenAckQueue(t *testing.T) {
	ackQueue, err := OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}
	_ = ackQueue.CloseWrite()
	_ = ackQueue.CloseRead()

	ackQueue, err = OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}

	_ = ackQueue.CloseWrite()
	_ = ackQueue.CloseRead()

	ackQueue, err = OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}

	_ = ackQueue.CloseWrite()
	_ = ackQueue.CloseRead()

	ackQueue, err = OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}
}

func randomBytes(n int) []byte {
	f, err := os.Open(`/dev/urandom`)
	if err != nil {
		panic(err)
	}

	bs := make([]byte, n)

	if _, err := f.Read(bs); err != nil {
		panic(err)
	}

	return bs
}

func TestAckQueue_Write(t *testing.T) {
	now := time.Now()

	ackQueue, err := OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}

	//defer ackQueue.Drop()

	for i := 0; i < 100000; i++ {
		_, err := ackQueue.Enqueue(randomBytes(4 * (1024 * 1024)))
		if err != nil {
			log.Println("写错误:", err.Error())
			return
		}
	}

	_ = ackQueue.Close()
	log.Println("写耗时", time.Since(now))
}

func TestAckQueue_WriteMinute(t *testing.T) {

	ackQueue, err := OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}

	timer := time.NewTimer(time.Minute)
	defer timer.Stop()

	var c int
loop:
	for {
		select {
		case <-timer.C:
			break loop
		default:
		}

		_, err := ackQueue.Enqueue(randomBytes(4 * (1024 * 1024)))
		if err != nil {
			log.Println("写错误:", err.Error())
			return
		}

		c++
	}

	_ = ackQueue.Close()
	log.Println("写入数量:", c)
}

func TestAckQueue_Read(t *testing.T) {
	now := time.Now()

	ackQueue, err := OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}

	_ = ackQueue.CloseWrite()

	for {
		item, err := ackQueue.BDequeue(context.Background())
		if err != nil {
			log.Println("读错误:", err.Error())
			break
		}

		if err := ackQueue.Submit(item.ID); err != nil {
			log.Println("提交错误:", err.Error())
			break
		}
	}

	_ = ackQueue.CloseRead()
	log.Println("读耗时", time.Since(now))
}

func TestAckQueue_Full(t *testing.T) {
	//random := rand.New(rand.NewSource(time.Now().Unix() + 4546))
	now := time.Now()

	ackQueue, err := OpenAckQueue(`./test_queue/ack_queue2`)
	if err != nil {
		t.Fatal(err)
	}

	defer ackQueue.Drop()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 100000; i++ {
			_, err := ackQueue.Enqueue(randomBytes(4 * (1024 * 1024)))
			if err != nil {
				log.Println("写错误:", err.Error())
				return
			}
		}

		_ = ackQueue.CloseWrite()
		log.Println("写耗时", time.Since(now))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			item, err := ackQueue.BDequeue(context.Background())
			if err != nil {
				log.Println("读错误:", err.Error())
				break
			}

			if err := ackQueue.Submit(item.ID); err != nil {
				log.Println("提交错误:", err.Error())
				break
			}
		}

		_ = ackQueue.CloseRead()
		log.Println("读耗时", time.Since(now))
	}()
	wg.Wait()
}

type TestVal struct {
	Name string
	Age  int
}

func TestObjectAckQueue(t *testing.T) {
	oaq, err := OpenObjectAckQueue[TestVal](`./test_queue/ack_queue3`)
	if err != nil {
		t.Fatal(err)
	}
	defer oaq.Drop()

	err = oaq.Enqueue(TestVal{
		Name: "xxx",
		Age:  100,
	})
	if err != nil {
		t.Fatal(err)
	}

	item, err := oaq.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(item.Value)

	oaq2, err := OpenObjectAckQueue[*TestVal](`./test_queue/ack_queue4`)
	if err != nil {
		t.Fatal(err)
	}
	defer oaq2.Drop()

	if err := oaq2.Enqueue(&TestVal{
		Name: "ppix123",
		Age:  100,
	}); err != nil {
		t.Fatal(err)
	}

	item2, err := oaq2.Dequeue()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(item2.Value)
}
