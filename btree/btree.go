package btree

import (
	"bytes"
	"encoding/binary"
	"etcd/raft/raftexample/verify"
)

const (
	BNODE_NODE          = 1
	BNODE_LEAF          = 2
	HEADER              = 4
	BTREE_MAX_PAGE_SIZE = 4096
	BTREE_MAX_KEY_SIZE  = 1000
	BTREE_MAX_VAL_SIZE  = 3000
)

func init() {
	nodeMax := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	verify.Assert(nodeMax < BTREE_MAX_PAGE_SIZE, "can not create a page with size more than %v", BTREE_MAX_PAGE_SIZE)
}

type BNode struct {
	data []byte
}

func (n BNode) bType() uint16 {
	return binary.LittleEndian.Uint16(n.data)
}

func (n BNode) nKeys() uint16 {
	return binary.LittleEndian.Uint16(n.data[2:4])
}

func (n BNode) setHeader(bType uint16, nKeys uint16) {
	binary.LittleEndian.PutUint16(n.data[0:2], bType)
	binary.LittleEndian.PutUint16(n.data[2:4], nKeys)
}

func (n BNode) getPtr(idx uint64) uint64 {
	verify.Assert(idx < uint64(n.nKeys()), "page index is larger than max number of keys in page")
	pos := HEADER + idx*8
	return binary.LittleEndian.Uint64(n.data[pos:])
}

func (n BNode) setPtr(idx uint64, val uint64) {
	verify.Assert(idx < uint64(n.nKeys()), "page index is larger than max number of keys in page")
	pos := HEADER + idx*8
	binary.LittleEndian.PutUint64(n.data[pos:], val)
}

func offsetPos(node BNode, idx uint16) uint16 {
	verify.Assert(1 <= int(idx) && idx <= node.nKeys(), "")
	return HEADER + 8*node.nKeys() + 2*(idx-1)
}

func (n BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(n.data[offsetPos(n, idx):])
}

func (n BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(n.data[offsetPos(n, idx):], offset)
}

func (n BNode) kvPos(idx uint16) uint16 {
	verify.Assert(idx <= n.nKeys(), "")
	return HEADER + 8*n.nKeys() + 2*n.nKeys() + n.getOffset(idx)
}

func (n BNode) getKey(idx uint16) []byte {
	verify.Assert(idx < n.nKeys(), "")
	pos := n.kvPos(idx)
	klen := binary.LittleEndian.Uint16(n.data[pos:])
	return n.data[pos+4:][:klen]
}

func (n BNode) getVal(idx uint16) []byte {
	verify.Assert(idx < n.nKeys(), "")
	pos := n.kvPos(idx)
	klen := binary.LittleEndian.Uint16(n.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(n.data[pos+2:])
	return n.data[pos+4+klen:][:vlen]
}

func (n BNode) nbytes() uint16 {
	return n.kvPos(n.nKeys())
}

type BTree struct {
	root uint64
	get  func(uint64) BNode // deref a page
	new  func(BNode) uint64 // allocate a page
	del  func(uint64)       // delete a page
}

func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nKeys()
	found := uint16(0)

	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// add a new key to a leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nKeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nKeys()-idx)
}

func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	verify.Assert(srcOld+n <= old.nKeys(), "")
	verify.Assert(dstNew+n <= new.nKeys(), "")
	if n == 0 {
		return
	}
	// pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(uint64(dstNew+i), old.getPtr(uint64(srcOld+i)))
	}
	// offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ { // NOTE: the range is [1, n]
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}
	// KVs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

// copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) { // ptrs
	new.setPtr(uint64(idx), ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}
