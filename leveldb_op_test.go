package goque

import (
	"strconv"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestDeleteAndIterator1(t *testing.T) {
	q, err := OpenQueue(`./test_queue/ack_queue`)
	if err != nil {
		t.Fatal(err)
	}

	var items []*Item
	for i := 0; i < 20; i++ {
		item, err := q.EnqueueString(`xa` + strconv.Itoa(i+1))
		if err != nil {
			t.Fatal(err)
		}

		items = append(items, item)
	}

	iterator := q.db.NewIterator(nil, nil)
	for iterator.Next() {
		id := keyToID(iterator.Key())
		err := iterator.Error()
		if err != nil {
			t.Fatal(err)
		}

		t.Log(`读取到的id`, id)
	}
	iterator.Release()
}

func TestIterator2(t *testing.T) {
	q, err := OpenQueue(`./test_queue/ack_queue`)
	if err != nil {
		t.Fatal(err)
	}

	iterator := q.db.NewIterator(&util.Range{
		Start: idToKey(10),
	}, nil)
	for iterator.Next() {
		id := keyToID(iterator.Key())
		err := iterator.Error()
		if err != nil {
			t.Fatal(err)
		}

		t.Log(`读取到的id`, id)
	}
	iterator.Release()
}

func TestIterator3(t *testing.T) {
	q, err := OpenQueue(`./test_queue/ack_queue`)
	if err != nil {
		t.Fatal(err)
	}

	if err := q.db.Delete(idToKey(2), nil); err != nil {
		t.Fatal(err)
	}
	if err := q.db.Delete(idToKey(7), nil); err != nil {
		t.Fatal(err)
	}
	if err := q.db.Delete(idToKey(5), nil); err != nil {
		t.Fatal(err)
	}

	iterator := q.db.NewIterator(nil, nil)
	if err := q.db.Delete(idToKey(8), nil); err != nil {
		t.Fatal(err)
	}

	for iterator.Next() {
		id := keyToID(iterator.Key())
		err := iterator.Error()
		if err != nil {
			t.Fatal(err)
		}

		t.Log(`读取到的id`, id)
	}
	iterator.Release()
}
