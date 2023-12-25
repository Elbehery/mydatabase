package btree

import "etcd/raft/raftexample/verify"

const (
	BNODE_NODE          = 1
	BNODE_LEAF          = 2
	HEADER              = 4
	BTREE_MAX_PAGE_SIZE = 4096
	BTREE_MAX_KEY_SIZE  = 1000
	BTREE_MAX_VAL_SIZE  = 3000
)

type BNode struct {
	data []byte
}

type BTree struct {
	root uint64
	get  func(uint64) BNode // deref a page
	new  func(BNode) uint64 // allocate a page
	del  func(uint64)       // delete a page
}

func init() {
	nodeMax := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	verify.Assert(nodeMax < BTREE_MAX_PAGE_SIZE, "can not create a page with size more than %v", BTREE_MAX_PAGE_SIZE)
}
