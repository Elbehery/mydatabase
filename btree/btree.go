package btree

import (
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
