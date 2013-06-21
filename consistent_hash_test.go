package consistent_hash 

import (
	"testing"
	"fmt"
	"sort"
	"strconv"
	"math/rand"
	"encoding/base64"
)

func TestBuildCH(t *testing.T) {
	mult := 20
	ch := NewConsistentHash(mult)
	items := []string{"127.0.0.1","127.0.1.1","127.1.0.1","127.9.0.1","127.0.8.1","127.10.0.1","127.1.1.1",
		"128.0.0.1","128.0.1.1","128.1.0.1","128.9.0.1","128.0.8.1","128.10.0.1","128.1.1.1"}
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
	total := 1000000
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
