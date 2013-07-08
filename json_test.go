package gorb

import (
	"reflect"
	"testing"
	"time"
)

type (
	A struct {
		Id  int64      `gorb:"id,pk"`
		Str string     `gorb:"str,:30"`
		Tm  *time.Time `gorb:"tm"`
		PB  []*B       `gorb:"B"`
	}
	B struct {
		Id  uint64 `gorb:"id,pk"`
		Pid int64  `gorb:"pid,fk"`
		Str string `gorb:"str,:56"`
	}
)

var (
	mgr  *GorbManager
	entA *Entity
	e    error
)

func init() {
	mgr = new(GorbManager)
	entA, e = mgr.RegisterEntity(reflect.TypeOf((*A)(nil)).Elem(), "A")
}

func TestEntityClone(t *testing.T) {
	var tm time.Time = time.Now()
	var a1 A = A{Id: 1, Str: "String 1", Tm: &tm}
	var b B = B{Id: 1, Pid: 1, Str: "PString 1"}

	a1.PB = append(a1.PB, &b)
	var a2 A
	var i interface{}

	i, e = mgr.EntityClone(a1)
	if e != nil {
		t.Error(e)
	}
	a2 = i.(A)
	if a1 != a2 {
		t.Fail()
	}
}

/*
func TestEntityGet(t *testing.T) {
	var tm time.Time = time.Now()
	var b B = B{Id: 1, Pid: 1, Str: "PString 1"}
	var a1 A = A{Id: 1, Str: "String 1", Tm: tm}
	var a2 *A
	var a3 A

	a1.PB = make([]*B, 0, 4)
	a1.PB = append(a1.PB, &b)

	var js []byte
	js, e = mgr.EntityJsonGet(a1, a2)
	if e != nil {
		t.Error(e)
	}
	t.Log(string(js))

	e = mgr.EntityJsonApply(&a3, js)
	if e != nil {
		t.Error(e)
	}
	js, e = mgr.EntityJsonGet(a3, a2)
	t.Log(string(js))

}
*/
