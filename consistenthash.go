// Package consistenthash implements a consistent hash.
package consistenthash

import (
	"errors"
	"fmt"
	"hash/crc32"
)

// ConsistentHash maps hashed values to targets.
type ConsistentHash struct {
	// to enter elements into the hash multiple times
	Mult int
	// the sorted list of hashed elements
	SumList []uint32
	// a map of hash values to original strings
	Source (map[uint32]string)
}

// NewConsistentHash will create a new consistent hash with hashed elements multiplied and entered
// into the SumList "mult" times.
func NewConsistentHash(mult int) (*ConsistentHash, error) {
	if mult <= 0 {
		return nil, errors.New("mult factor must be 1 or greater")
	}
	h := new(ConsistentHash)
	h.SumList = make([]uint32, 0)
	h.Mult = mult
	h.Source = make(map[uint32]string)
	return h, nil
}

// New is an alias to NewConsistentHash.
func New(mult int) (*ConsistentHash, error) {
	return NewConsistentHash(mult)
}

// multElt is the way we "multiply" a string...we simply append and integer to the end.
func multElt(s string, i int) string {
	return fmt.Sprintf("%s.%d", s, i)
}

// insertOne inserts a new element into the SumList. this list is kept sorted.
func (h *ConsistentHash) insertOne(s, sMult string) error {
	sSum := crc32.ChecksumIEEE([]byte(sMult))
	hl := make([]uint32, len(h.SumList)+1)
	i := 0
	done := false
	for _, v := range h.SumList {
		if sSum == v {
			// duplicate entry
			e := fmt.Sprintf("collision on %s hashed as %d", s, sSum)
			return errors.New(e)
		}
		if sSum <= v && !done {
			hl[i] = sSum
			i++
			done = true
		}
		hl[i] = v
		i++
	}
	if !done {
		hl[i] = sSum
	}
	h.SumList = hl
	h.Source[sSum] = s
	return nil
}

// Insert a new element into the SumList as "mult" instances of crc32 hashes.
func (h *ConsistentHash) Insert(s string) error {
	for i := 1; i <= h.Mult; i++ {
		insertErr := h.insertOne(s, multElt(s, i))
		if insertErr != nil {
			return insertErr
		}
	}
	return nil
}

// remove a single element from SumList
func (h *ConsistentHash) removeOne(sMult string) error {
	sSum := crc32.ChecksumIEEE([]byte(sMult))
	if _, ok := h.Source[sSum]; !ok {
		return nil // not found
	}
	hl := make([]uint32, len(h.SumList)-1)
	i := 0
	for _, v := range h.SumList {
		if v != sSum {
			hl[i] = v
			i++
		}
	}
	h.SumList = hl
	delete(h.Source, sSum)
	return nil
}

// Remove a new element from the SumList as "mult" instances of crc32 hashes.
func (h *ConsistentHash) Remove(s string) error {
	for i := 1; i <= h.Mult; i++ {
		removeErr := h.removeOne(multElt(s, i))
		if removeErr != nil {
			return removeErr
		}
	}
	return nil
}

// Find the nearest hashed element (in ascending order)
// that the candidate string s maps to. should s hash to a greater
// value than the maximum hashed item in the SumList, loop around
// and select the zeroth hashed element
func (h *ConsistentHash) Find(s string) (string, error) {
	if len(h.SumList) == 0 {
		e := fmt.Sprintf("empty sumlist")
		return "", errors.New(e)
	}
	sSum := crc32.ChecksumIEEE([]byte(s))
	for _, v := range h.SumList {
		if sSum <= v {
			if src, ok := h.Source[v]; ok {
				return src, nil
			}
			e := fmt.Sprintf("no source mapping for %v", v)
			return "", errors.New(e)
		}
	}
	v := h.SumList[0] // if not valid, first line would have triggered
	if src, ok := h.Source[v]; ok {
		return src, nil
	}
	e := fmt.Sprintf("no source mapping for %v", v)
	return "", errors.New(e)
}
