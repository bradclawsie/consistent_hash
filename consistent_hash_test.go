package consistent_hash 

import (
	"testing"
	"fmt"
	"sort"
	"strconv"
	"hash/crc32"
	"math/rand"
	"encoding/base64"
)

func TestBuildCH(t *testing.T) {
	fmt.Printf("\n---\nTEST BUILD, INSERT AND FIND\n")
	mult := 100
	ch := NewConsistentHash(mult)
	items := []string{"127.0.0.1","17.0.1.1","1.1.0.1","27.99.0.111","64.0.8.8","8.8.8.8","10.100.0.100",
		"128.4.4.4","28.28.1.1","28.10.0.10","12.9.0.10","11.11.8.1","13.10.0.19","128.19.19.19"}
	for _,item := range items {
		insert_err := ch.Insert(item)
		if insert_err != nil {
			t.Errorf(insert_err.Error())
		}
	}
	// make sure the len is correct
	if len(ch.SumList) != (mult * len(items)) {
		e := fmt.Sprintf("SumList len should be %d, but is %d",(mult*len(items)),len(ch.SumList))
		t.Errorf(e)
	}
	// make sure the list is indeed sorted
	sl := make([]int,len(ch.SumList))
	for k,v := range ch.SumList {
		sl[k] = int(v)
	}
	sort.Ints(sl)
	for k,v := range sl {
		if v != int(ch.SumList[k]) {
			e := fmt.Sprintf("sorted list differs at position %d",k)
			t.Errorf(e)
		}
	}
	count := make(map [string] int)
	for _,sum := range ch.SumList {
		count[ch.Source[sum]]++
	}
	for k,c := range count {
		if c != mult {
			e := fmt.Sprintf("%s appears %d times, not % times",k,c,mult)
			t.Errorf(e)
		}
	}
	nearest_hash,nh_err := ch.Find("hello")
	if nh_err != nil || nearest_hash == "" {
		t.Errorf("err returned on finding nearest hash for hello")
	}

	// test distribution
	dist := make(map [string] int)
	total := 10000
	for j := 0; j < total; j++ {
		b64 := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(int(rand.Int31()))))
		nearest_hash,nh_err := ch.Find(b64)
		if nh_err != nil || nearest_hash == "" {
			e := fmt.Sprintf("err returned on finding nearest hash for %s",b64)
			t.Errorf(e)
		}
		dist[nearest_hash]++
	}
	for k,v := range dist {
		fmt.Printf("%s %d (%f pct) \n",k,v,(float64(v)/float64(total))*100)
	}
}

func TestEmptyCH(t *testing.T) {
	fmt.Printf("\n---\nTEST EMPTY\n")
	ch := NewConsistentHash(0)
	if ch != nil {
		t.Errorf("should return nil when mult factor 0")
	}
	ch = NewConsistentHash(1)
	if ch == nil {
		t.Errorf("should return non-nil when mult factor nonzero")
	}
	_,err := ch.Find("hello")
	if err == nil {
		t.Errorf("should not find anything on empty hash")
	}
}

func TestCollision(t *testing.T) {
	fmt.Printf("\n---\nTEST COLLISION\n")
	ch := NewConsistentHash(1)
	insert_err := ch.Insert("hello")
	if insert_err != nil {
		t.Errorf(insert_err.Error())
	}
	insert_err  = ch.Insert("hello")
	if insert_err == nil {
		t.Errorf("should have caused collision")
	}
}

func TestRemove(t *testing.T) {
	fmt.Printf("\n---\nTEST REMOVE\n")
	mult := 2
	ch := NewConsistentHash(mult)
	items := []string{"127.0.0.1","17.0.1.1","1.1.0.1","27.99.0.111","64.0.8.8","8.8.8.8","10.100.0.100",
		"128.4.4.4","28.28.1.1","28.10.0.10","12.9.0.10","11.11.8.1","13.10.0.19","128.19.19.19"}
	for _,item := range items {
		insert_err := ch.Insert(item)
		if insert_err != nil {
			t.Errorf(insert_err.Error())
		}
	}
	
	// remove something that isn't there
	_ = ch.Remove("hello")

	// make sure the len is correct
	if len(ch.SumList) != (mult * len(items)) {
		e := fmt.Sprintf("SumList len should be %d, but is %d",(mult*len(items)),len(ch.SumList))
		t.Errorf(e)
	}

	// remove something that is there
	_ = ch.Remove("28.28.1.1")

	// make sure the len is correct
	if len(ch.SumList) != (mult * (len(items)-1)) {
		e := fmt.Sprintf("SumList len should be %d, but is %d",(mult*len(items)),len(ch.SumList))
		t.Errorf(e)
	}

	sum1 := crc32.ChecksumIEEE([]byte(mult_elt("28.28.1.1",1)))
	sum2 := crc32.ChecksumIEEE([]byte(mult_elt("28.28.1.1",2)))
	for _,v := range ch.SumList {
		if v == sum1 || v == sum2 {
			t.Errorf("found element in SumList that should have been deleted")
		}
	}
}