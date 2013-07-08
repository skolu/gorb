package gorb

import (
	"fmt"
	"reflect"
)

func (t *Table) cloneInstance(from, to reflect.Value) {
	for _, f := range t.Fields {
		vFrom := from.FieldByIndex(f.ClassIdx)
		vTo := to.FieldByIndex(f.ClassIdx)
		if f.FieldType.Kind() == reflect.Ptr {
			if vFrom.IsNil() {
				vTo.Set(reflect.Zero(f.FieldType))
			} else {
				vTo.Set(reflect.New(f.FieldType.Elem()))
				vTo.Elem().Set(vFrom.Elem())
			}
		} else {
			vTo.Set(vFrom)
		}
	}

	for _, ch := range t.Children {
		vChFrom := from.FieldByIndex(ch.ClassIdx)
		vChTo := to.FieldByIndex(ch.ClassIdx)
		if vChFrom.IsNil() {
			vChTo.Set(reflect.Zero(ch.ChildClass))
		} else {
			switch ch.ChildClass.Kind() {
			case reflect.Ptr:
				vChTo.Set(reflect.New(ch.RowClass))
				ch.cloneInstance(vChFrom.Elem(), vChTo.Elem())
			case reflect.Slice:
				len := vChFrom.Len()
				vChTo.Set(reflect.MakeSlice(ch.ChildClass, 0, len))
				for i := 0; i < len; i++ {
					vChRowFrom := vChFrom.Index(i)
					vChRowTo := reflect.New(ch.RowClass)
					ch.cloneInstance(vChRowFrom.Elem(), vChRowTo.Elem())
					vChTo.Set(reflect.Append(vChTo, vChRowTo))
				}
			}
		}
	}
}

func (conn *GorbManager) EntityClone(entity interface{}) (interface{}, error) {
	if entity == nil {
		return nil, nil
	}

	var ent *Entity
	eType := reflect.TypeOf(entity)
	var isPtr = eType.Kind() == reflect.Ptr
	if isPtr {
		eType = eType.Elem()
	}
	ent = conn.LookupEntity(eType)
	if ent == nil {
		return nil, fmt.Errorf("Unsupported entity %s", eType.Name())
	}

	var pV reflect.Value = reflect.New(ent.RowClass)
	initf, ok := pV.Interface().(interface {
		OnEntityInit()
	})
	if ok {
		initf.OnEntityInit()
	}
	var vDest = pV.Elem()

	var vSrc = reflect.ValueOf(entity)
	if isPtr {
		vSrc = vSrc.Elem()
	}

	ent.cloneInstance(vSrc, vDest)

	if isPtr {
		return pV.Interface(), nil
	} else {
		return vDest.Interface(), nil
	}
}
