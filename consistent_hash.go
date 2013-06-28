package consistent_hash 

import (
	"fmt"
	"log"
	"errors"
	"hash/crc32"
	"sync"
)

type ConsistentHash struct {
	// to enter elements into the hash multiple times
	Mult int
	// the sorted list of hashed elements
	SumList []uint32
	// a map of hash values to original strings
	Source (map [uint32] string)
	// lock for access
	mutex sync.RWMutex
}

// Create a new consistent hash with hashed elements multiplied and entered
// into the SumList "mult" times.
func NewConsistentHash(mult int) (*ConsistentHash) {
	if mult <= 0 {
		log.Print("mult factor must be 1 or greater")
		return nil
	}
	h := new(ConsistentHash)
	h.SumList = make([]uint32,0)
	h.Mult = mult
	h.Source = make(map [uint32] string)
	return h
}

// Alias to NewConsistentHash
func New(mult int) (*ConsistentHash) {
	return NewConsistentHash(mult)
}

// the way we "multiply" a string...we simply append and integer to the end
func mult_elt(s string,i int) string {
	return fmt.Sprintf("%s.%d",s,i)
}

// insert a new element into the SumList. this list is kept sorted.
func (h *ConsistentHash) insert_one(s,s_mult string) error {
	s_sum := crc32.ChecksumIEEE([]byte(s_mult))
	hl := make([]uint32,len(h.SumList)+1)
	i := 0
	done := false
	for _,v := range h.SumList {
		if s_sum == v {
			// duplicate entry
			e := fmt.Sprintf("collision on %s hashed as %d",s,s_sum)
			return errors.New(e)
		}
		if s_sum <= v && !done {
			hl[i] = s_sum
			i++
			done = true
		}
		hl[i] = v
		i++
	}
	if !done {
		hl[i] = s_sum
	}
	h.SumList = hl	
	h.Source[s_sum] = s
	return nil
}

// insert a new element into the SumList as "mult" instances of crc32 hashes.
func (h *ConsistentHash) Insert(s string) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	for i := 1; i <= h.Mult; i++ {
		insert_err := h.insert_one(s,mult_elt(s,i))
		if insert_err != nil {
			return insert_err
		}
	} 
	return nil
}

// remove a single element from SumList
func (h *ConsistentHash) remove_one(s_mult string) error {
	s_sum := crc32.ChecksumIEEE([]byte(s_mult))
	if _,source_ok := h.Source[s_sum]; !source_ok {
		log.Printf("%s not found",s_mult)
		return nil
	}
	hl := make([]uint32,len(h.SumList)-1)
	i := 0
	for _,v := range h.SumList {
		if v != s_sum {
			hl[i] = v
			i++
		}
	}
	h.SumList = hl	
	delete(h.Source,s_sum)
	return nil
}

// remove a new element from the SumList as "mult" instances of crc32 hashes.
func (h *ConsistentHash) Remove(s string) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	for i := 1; i <= h.Mult; i++ {
		remove_err := h.remove_one(mult_elt(s,i))
		if remove_err != nil {
			return remove_err
		}
	} 
	return nil
}

// find the nearest hashed element (in ascending order) 
// that the candidate string s maps to. should s hash to a greater
// value than the maximum hashed item in the SumList, loop around
// and select the zeroth hashed element
func (h *ConsistentHash) Find(s string) (string,error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()	
	if len(h.SumList) == 0 {
		e := fmt.Sprintf("empty sumlist")
		return "",errors.New(e)		
	}
	s_sum := crc32.ChecksumIEEE([]byte(s))
	for _,v := range h.SumList {
		if s_sum <= v {
			if src,src_ok := h.Source[v]; src_ok {
				return src,nil
			} else {
				e := fmt.Sprintf("no source mapping for %v",v)
				return "",errors.New(e)
			}
		}
	}
	v := h.SumList[0] // if not valid, first line would have triggered
	if src,src_ok := h.Source[v]; src_ok {
		return src,nil
	} else {
                e := fmt.Sprintf("no source mapping for %v",v)
                return "",errors.New(e)
        }
}

