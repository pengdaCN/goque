package goque

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"os"
	"sync"
)

type AckQueue struct {
	mu                     sync.Mutex
	q                      *Queue
	globalNextReaderCursor uint64
	size                   uint64
	oldIterator            iterator.Iterator
	writeOpen              bool
	oldIteratorIsClose     bool
	isOpen                 bool
}

func OpenAckQueue(dataDir string) (*AckQueue, error) {
	q, err := openQueueWithoutGoqueType(dataDir)
	if err != nil {
		return nil, err
	}

	ok, err := checkGoqueType(dataDir, goqueAckQueue)
	if err != nil {
		_ = q.Close()

		return nil, err
	}
	if !ok {
		_ = q.Close()

		return nil, ErrIncompatibleType
	}

	firstIterator := q.db.NewIterator(nil, nil)
	if err := firstIterator.Error(); err != nil {
		firstIterator.Release()

		return nil, err
	}

	return &AckQueue{
		q:           q,
		oldIterator: firstIterator,
		writeOpen:   true,
		isOpen:      true,
	}, nil
}

func (a *AckQueue) incrSize() error {
	a.size += 1

	return nil
}

func (a *AckQueue) Enqueue(value []byte) (*Item, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.writeOpen {
		return nil, ErrCloseWriteOperation
	}

	if err := a.incrSize(); err != nil {
		return nil, err
	}

	return a.q.Enqueue(value)
}

func (a *AckQueue) Dequeue() (*Item, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var next uint64
	if !a.oldIteratorIsClose {
		if a.oldIterator.Next() {
			next = keyToID(a.oldIterator.Key())

			if err := a.oldIterator.Error(); err != nil {
				return nil, err
			}
		} else {
			a.oldIterator.Release()
			a.oldIterator = nil
			a.oldIteratorIsClose = true

			next = a.globalNextReaderCursor
		}
	} else {
		next = a.globalNextReaderCursor
	}

	a.globalNextReaderCursor = next + 1

	v, err := a.q.getItemByID(next)
	if err != nil {
		if !a.writeOpen {
			switch {
			case errors.Is(err, ErrOutOfBounds), errors.Is(err, ErrEmpty):
				if err := a.innerClose(); err != nil {
					return nil, err
				}

				return nil, ErrDBClosed
			}
		}

		return nil, err
	}

	return v, nil
}

func (a *AckQueue) decrSize() error {
	a.size -= 1

	return nil
}

func (a *AckQueue) Submit(id uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.globalNextReaderCursor < id {
		return ErrInvalidAckID
	}

	if err := a.decrSize(); err != nil {
		return err
	}

	if err := a.q.db.Delete(idToKey(id), nil); err != nil {
		return err
	}

	return nil
}

func (a *AckQueue) HalfClose() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.writeOpen {
		a.writeOpen = false
	}
}

func (a *AckQueue) innerClose() error {
	// Check if queue is already closed.
	if !a.isOpen {
		return nil
	}

	if !a.oldIteratorIsClose {
		a.oldIterator.Release()
		a.oldIterator = nil
		a.oldIteratorIsClose = true
	}

	// Close the LevelDB database.
	if err := a.q.Close(); err != nil {
		return err
	}

	a.globalNextReaderCursor = 0
	a.isOpen = false

	return nil
}

func (a *AckQueue) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.innerClose()
}

func (a *AckQueue) Drop() error {
	if err := a.Close(); err != nil {
		return err
	}

	return os.RemoveAll(a.q.DataDir)
}
