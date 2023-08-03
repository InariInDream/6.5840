package mr

import (
	"errors"
	"sync"
)

/*
 * listNode
 */

type listNode struct {
	data interface{} // data can be any type
	next *listNode
	prev *listNode
}

func (node *listNode) addBefore(data interface{}) {
	prev := node.prev

	newnode := listNode{}
	newnode.data = data
	// newnode -> node
	newnode.next = node
	node.prev = &newnode
	newnode.prev = prev
	prev.next = &newnode
}

func (node *listNode) addAfter(data interface{}) {
	next := node.next

	newnode := listNode{}
	newnode.data = data
	// node -> newnode
	newnode.prev = node
	node.next = &newnode
	newnode.next = next
	next.prev = &newnode
}

func (node *listNode) removeBefore() {
	prev := node.prev.prev
	prev.next = node
	node.prev = prev
}

func (node *listNode) removeAfter() {
	next := node.next.next
	next.prev = node
	node.next = next
}

/*
 * LinkedList
 */

type LinkedList struct {
	head  listNode
	count int
}

func NewLinkedList() *LinkedList {
	list := LinkedList{}
	list.head.next = &list.head
	list.head.prev = &list.head
	list.count = 0
	return &list
}

func (list *LinkedList) pushFront(data interface{}) {
	list.head.addAfter(data)
	list.count++
}

func (list *LinkedList) pushBack(data interface{}) {
	list.head.addBefore(data)
	list.count++
}

func (list *LinkedList) front() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("list is empty")
	}
	return list.head.next.data, nil
}

func (list *LinkedList) back() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("list is empty")
	}
	return list.head.prev.data, nil
}

func (list *LinkedList) popFront() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("list is empty")
	}
	data := list.head.next.data
	list.head.removeAfter()
	list.count--
	return data, nil
}

func (list *LinkedList) popBack() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("list is empty")
	}
	data := list.head.prev.data
	list.head.removeBefore()
	list.count--
	return data, nil
}

func (list *LinkedList) size() int {
	return list.count
}

/*
 * BlockQueue
 */

type BlockQueue struct {
	list *LinkedList
	cond *sync.Cond
}

func NewBlockQueue() *BlockQueue {
	queue := BlockQueue{}

	queue.list = NewLinkedList()
	queue.cond = sync.NewCond(&sync.Mutex{})

	return &queue
}

func (queue *BlockQueue) appendFront(data interface{}) {
	queue.cond.L.Lock() // get the lock, if the lock is already held, wait
	queue.list.pushFront(data)
	queue.cond.L.Unlock()  // !!! important, release the lock before broadcast
	queue.cond.Broadcast() // or other goroutines will never get the lock
}

func (queue *BlockQueue) appendBack(data interface{}) {
	queue.cond.L.Lock()
	queue.list.pushBack(data)
	queue.cond.L.Unlock()
	queue.cond.Broadcast()
}

func (queue *BlockQueue) front() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.size() == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.front()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) back() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.size() == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.back()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) popFront() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.size() == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.popFront()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) popBack() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.size() == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.popBack()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) size() int {
	queue.cond.L.Lock()
	size := queue.list.size()
	queue.cond.L.Unlock()
	return size
}
