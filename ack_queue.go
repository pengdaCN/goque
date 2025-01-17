package goque

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/syndtr/goleveldb/leveldb/iterator"
)

const (
	ackQueuePrefixData byte = iota + 1
	ackQueuePrefixMeta
)

var (
	ackQueueMetaSizeKey = []byte{
		ackQueuePrefixMeta,
		's',
		'i',
		'z',
		'e',
	}
)

type AckQueue struct {
	mu                     sync.Mutex
	q                      *Queue
	globalNextReaderCursor uint64
	size                   uint64 // 队列大小
	readCount              uint64 // 读取数量统计
	confirmCount           uint64 // 确认消息统计
	oldIterator            iterator.Iterator
	writeMsgEvent          chan struct{}
	writeOpen              bool
	readOpen               bool
	oldIteratorIsClose     bool
	isOpen                 bool
	isPaused               bool
	_pauseStateWait        atomic.Pointer[chan struct{}]
}

func OpenAckQueue(dataDir string) (*AckQueue, error) {
	q, err := openQueueWithoutGoqueType(dataDir)
	if err != nil {
		return nil, err
	}
	q.prefix = []byte{ackQueuePrefixData}

	if err := q.init(); err != nil {
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

	qu := &AckQueue{
		q:                      q,
		globalNextReaderCursor: q.head + 1,
		writeMsgEvent:          make(chan struct{}, 1),
		writeOpen:              true,
		readOpen:               true,
		isOpen:                 true,
	}

	if err := qu.init(); err != nil {
		return nil, err
	}

	// 初始化
	qu.Pause()
	qu.Continue()

	return qu, nil
}

func (a *AckQueue) init() error {
	firstIterator := a.q.db.NewIterator(&util.Range{
		Start: a.q.prefix,
		Limit: a.q.encodingKey(idToKey(math.MaxUint64)),
	}, nil)
	if err := firstIterator.Error(); err != nil {
		firstIterator.Release()

		return err
	}
	a.oldIterator = firstIterator

	size, err := a.getSize()
	if err != nil {
		return err
	}

	a.size = size

	return nil
}

func (a *AckQueue) getSize() (uint64, error) {
	v, err := a.q.db.Get(ackQueueMetaSizeKey, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return 0, nil
		}

		return 0, err
	}

	return keyToID(v), nil
}

func (a *AckQueue) putSize(size uint64) error {
	if err := a.q.db.Put(ackQueueMetaSizeKey, idToKey(size), nil); err != nil {
		return err
	}

	return nil
}

func (a *AckQueue) incrSize() error {
	a.size += 1

	if err := a.putSize(a.size); err != nil {
		return err
	}

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

	v, err := a.q.Enqueue(value)
	if err != nil {
		return nil, err
	}

	select {
	case a.writeMsgEvent <- struct{}{}:
	default:
	}

	return v, nil
}

func (a *AckQueue) incrReadCount() {
	a.readCount++
}

func (a *AckQueue) Dequeue() (*Item, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.isOpen {
		return nil, ErrDBClosed
	}

	if a.isPaused {
		return nil, ErrDbPaused
	}

	var next uint64
	if !a.oldIteratorIsClose {
		if a.oldIterator.Next() {
			next = keyToID(a.q.decodingKey(a.oldIterator.Key()))

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

	v, err := a.q.getItemByID(next)
	if err != nil {
		if !a.writeOpen {
			switch {
			case errors.Is(err, ErrOutOfBounds), errors.Is(err, ErrEmpty):

				return nil, ErrCloseWriteOperation
			}
		}

		return nil, err
	}

	a.globalNextReaderCursor = next + 1

	a.incrReadCount()

	return v, nil
}

func (a *AckQueue) pauseWaitCh() <-chan struct{} {
	return *a._pauseStateWait.Load()
}

func (a *AckQueue) BDequeue(ctx context.Context) (*Item, error) {
readMsg:
	for {
		item, err := a.Dequeue()
		if err != nil {
			switch {
			case errors.Is(err, ErrOutOfBounds), errors.Is(err, ErrEmpty):
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case _, ok := <-a.writeMsgEvent:
					if ok {
						continue readMsg
					}

					// 关闭时，再次读取会返回db close
					continue readMsg
				}
			case errors.Is(err, ErrDbPaused):
				// 暂停的处理
				pch := a.pauseWaitCh()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-pch:
					continue readMsg
				}
			}

			return nil, err
		}

		return item, nil
	}
}

func (a *AckQueue) decrSize() error {
	a.size -= 1

	if err := a.putSize(a.size); err != nil {
		return err
	}

	return nil
}

func (a *AckQueue) incrConfirmCount() {
	a.confirmCount++
}

func (a *AckQueue) Submit(id uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.globalNextReaderCursor-1 < id {
		return ErrInvalidAckID
	}

	_, err := a.q.getItemByID(id)
	if err != nil {
		return nil
	}

	if err := a.decrSize(); err != nil {
		return err
	}

	a.incrConfirmCount()

	if err := a.q.db.Delete(a.q.encodingKey(idToKey(id)), nil); err != nil {
		return err
	}

	return nil
}

func (a *AckQueue) Pause() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.isPaused {
		return
	}

	a.isPaused = true
	ch := make(chan struct{})
	a._pauseStateWait.Store(&ch)
}

func (a *AckQueue) Continue() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.isPaused {
		return
	}

	a.isPaused = false
	ch := *a._pauseStateWait.Load()
	close(ch)
}

type AckQueueState struct {
	Length       uint64 // 队列现有数据长度
	ReadCount    uint64 // 读取数据数量记录
	ConfirmCount uint64 // 确认数据数量记录
	Pause        bool   // 是否暂停了读操作
}

func (a *AckQueue) State() AckQueueState {
	a.mu.Lock()

	state := AckQueueState{
		Length:       a.size,
		ReadCount:    a.readCount,
		ConfirmCount: a.confirmCount,
		Pause:        a.isPaused,
	}

	a.mu.Unlock()

	return state
}

func (a *AckQueue) CloseWrite() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.writeOpen {
		a.writeOpen = false
		close(a.writeMsgEvent)

		return a.innerClose()
	}

	return nil
}

func (a *AckQueue) CloseRead() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.readOpen {
		a.readOpen = false

		return a.innerClose()
	}

	return nil
}

func (a *AckQueue) innerClose() error {
	// Check if queue is already closed.
	if !a.isOpen {
		return nil
	}

	if a.writeOpen || a.readOpen {
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
	if err := a.CloseWrite(); err != nil {
		return err
	}

	if err := a.CloseRead(); err != nil {
		return err
	}

	return nil
}

func (a *AckQueue) Drop() error {
	if err := a.Close(); err != nil {
		return err
	}

	return os.RemoveAll(a.q.DataDir)
}

type ObjectItem[T any] struct {
	ID    uint64
	Key   []byte
	Value T
}

type ObjectAckQueue[T any] struct {
	queue *AckQueue
}

func OpenObjectAckQueue[T any](dataDir string) (*ObjectAckQueue[T], error) {
	q, err := OpenAckQueue(dataDir)
	if err != nil {
		return nil, err
	}

	return &ObjectAckQueue[T]{
		queue: q,
	}, nil
}

func (o *ObjectAckQueue[T]) Enqueue(value T) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(value); err != nil {
		return err
	}

	if _, err := o.queue.Enqueue(buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func (o *ObjectAckQueue[T]) toObject(item *Item) (*ObjectItem[T], error) {
	dec := gob.NewDecoder(bytes.NewReader(item.Value))
	var v T
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}

	return &ObjectItem[T]{
		ID:    item.ID,
		Key:   item.Key,
		Value: v,
	}, nil
}

func (o *ObjectAckQueue[T]) Dequeue() (*ObjectItem[T], error) {
	item, err := o.queue.Dequeue()
	if err != nil {
		return nil, err
	}

	return o.toObject(item)
}

func (o *ObjectAckQueue[T]) BDequeue(ctx context.Context) (*ObjectItem[T], error) {
	item, err := o.queue.BDequeue(ctx)
	if err != nil {
		return nil, err
	}

	return o.toObject(item)
}

func (o *ObjectAckQueue[T]) Submit(id uint64) error {
	return o.queue.Submit(id)
}

func (o *ObjectAckQueue[T]) State() AckQueueState {
	return o.queue.State()
}

func (o *ObjectAckQueue[T]) CloseWrite() error {
	return o.queue.CloseWrite()
}

func (o *ObjectAckQueue[T]) CloseRead() error {
	return o.queue.CloseRead()
}

func (o *ObjectAckQueue[T]) Close() error {
	return o.queue.Close()
}

func (o *ObjectAckQueue[T]) Drop() error {
	return o.queue.Drop()
}

func (o *ObjectAckQueue[T]) Pause() {
	o.queue.Pause()
}
func (o *ObjectAckQueue[T]) Continue() {
	o.queue.Continue()
}

type IAckQueue[T any] interface {
	Push(v T) error
	Pop() (*ObjectItem[T], error)
	BPop(ctx context.Context) (*ObjectItem[T], error)
	Submit(id uint64) error
	State() AckQueueState
	CloseWrite() error // 关闭写请求
	CloseRead() error  // 关闭读请求
	Close() error      // 同时关闭读写请求
	Drop() error       // 删除数据合目录
	Pause()            // 暂停读操作
	Continue()         // 从暂停状态中恢复
}

var _ IAckQueue[any] = (*iAckQueueImpl[any])(nil)

type iAckQueueImpl[T any] struct {
	q *ObjectAckQueue[T]
}

func (i *iAckQueueImpl[T]) Push(v T) error {
	if err := i.q.Enqueue(v); err != nil {
		return err
	}

	return nil
}

func (i *iAckQueueImpl[T]) Pop() (*ObjectItem[T], error) {
	return i.q.Dequeue()
}

func (i *iAckQueueImpl[T]) BPop(ctx context.Context) (*ObjectItem[T], error) {
	return i.q.BDequeue(ctx)
}

func (i *iAckQueueImpl[T]) Submit(id uint64) error {
	return i.q.Submit(id)
}

func (i *iAckQueueImpl[T]) State() AckQueueState {
	return i.q.State()
}

func (i *iAckQueueImpl[T]) CloseWrite() error {
	return i.q.CloseWrite()
}

func (i *iAckQueueImpl[T]) CloseRead() error {
	return i.q.CloseRead()
}

func (i *iAckQueueImpl[T]) Close() error {
	return i.q.Close()
}

func (i *iAckQueueImpl[T]) Drop() error {
	return i.q.Drop()
}

func (i *iAckQueueImpl[T]) Pause() {
	i.q.Pause()
}

func (i *iAckQueueImpl[T]) Continue() {
	i.q.Continue()
}

type AckQueuePush[T any] interface {
	Push(v T) error
	Close() error
}

var _ AckQueuePush[any] = (*ackQueuePushImpl[any])(nil)

type ackQueuePushImpl[T any] struct {
	q IAckQueue[T]
}

func (a *ackQueuePushImpl[T]) Push(v T) error {
	return a.q.Push(v)
}

func (a *ackQueuePushImpl[T]) Close() error {
	return a.q.CloseWrite()
}

type AckQueuePop[T any] interface {
	Pop() (*ObjectItem[T], error)
	BPop(ctx context.Context) (*ObjectItem[T], error)
	Submit(id uint64) error
	Close() error
}

var _ AckQueuePop[any] = (*ackQueuePopImpl[any])(nil)

type ackQueuePopImpl[T any] struct {
	q IAckQueue[T]
}

func (a *ackQueuePopImpl[T]) Pop() (*ObjectItem[T], error) {
	return a.q.Pop()
}

func (a *ackQueuePopImpl[T]) BPop(ctx context.Context) (*ObjectItem[T], error) {
	return a.q.BPop(ctx)
}

func (a *ackQueuePopImpl[T]) Submit(id uint64) error {
	return a.q.Submit(id)
}

func (a *ackQueuePopImpl[T]) Close() error {
	return a.q.CloseRead()
}

func MakeIAckQueue[T any](q *ObjectAckQueue[T]) IAckQueue[T] {
	return &iAckQueueImpl[T]{
		q: q,
	}
}

func SplitIAckQueue[T any](q IAckQueue[T]) (AckQueuePush[T], AckQueuePop[T]) {
	return &ackQueuePushImpl[T]{
			q: q,
		}, &ackQueuePopImpl[T]{
			q: q,
		}
}
