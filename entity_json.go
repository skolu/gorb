package gorb

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func sameValue(value1, value2 reflect.Value) bool {
	var ok bool

	switch value1.Kind() {
	case reflect.Bool:
		return value1.Bool() == value2.Bool()
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
		return value1.Int() == value2.Int()
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value1.Uint() == value2.Uint()
	case reflect.Float32, reflect.Float64:
		return math.Abs(value1.Float()-value2.Float()) < 0.0000001
	case reflect.String:
		return value1.String() == value2.String()
	case reflect.Slice:
		if value1.Len() == value2.Len() {
			var b1, b2 []byte
			b1, ok = value1.Interface().([]byte)
			if ok {
				b2, ok = value2.Interface().([]byte)
				if ok {
					for i := 0; i < len(b1); i++ {
						if b1[i] != b2[i] {
							return false
						}
					}
					return true
				}
			}
		}
	default:
		var d1, d2 time.Time
		d1, ok = value1.Interface().(time.Time)
		if ok {
			d2, ok = value2.Interface().(time.Time)
			if ok {
				return d1.Unix() == d2.Unix()
			}
		}
	}
	return false
}

func (t *Table) marshalToJson(newRow reflect.Value, oldRow *reflect.Value) map[string]interface{} {
	res := make(map[string]interface{}, len(t.Fields))
	var allSkipped bool = oldRow != nil

	for _, f := range t.Fields {
		skip := false
		vF := newRow.FieldByIndex(f.ClassIdx)
		if oldRow != nil {
			vFF := (*oldRow).FieldByIndex(f.ClassIdx)
			if vF.Kind() == reflect.Ptr {
				if vF.IsNil() || vFF.IsNil() {
					skip = vF.IsNil() && vFF.IsNil()
				} else {
					skip = sameValue(vF.Elem(), vFF.Elem())
				}
			} else {
				skip = sameValue(vF, vFF)
			}
			allSkipped = allSkipped && skip
		}
		if !f.IsRequired && skip {
			continue
		}

		var vSet interface{} = nil
		if vF.Kind() == reflect.Ptr {
			if !vF.IsNil() {
				vSet = vF.Elem().Interface()
			}
		} else {
			vSet = vF.Interface()
		}
		if vSet != nil {
			if f.DataType == DateTime {
				t, ok := vSet.(time.Time)
				if ok {
					vSet = timeToString(t)
				}
			}
		}
		res[f.FieldName] = vSet
	}

	for _, ch := range t.Children {
		var chJson map[string]interface{}
		chV := newRow.FieldByIndex(ch.ClassIdx)
		switch ch.ChildClass.Kind() {
		case reflect.Ptr:
			if chV.IsNil() {
				if oldRow != nil {
					if !(*oldRow).IsNil() {
						allSkipped = false
						res[ch.TableName] = nil
					}
				} else {
					allSkipped = false
					res[ch.TableName] = nil
				}
			} else {
				if oldRow != nil {
					chOV := (*oldRow).FieldByIndex(ch.ClassIdx)
					if chOV.IsNil() {
						chJson = ch.marshalToJson(chV.Elem(), nil)
					} else {
						var chOVP reflect.Value = chOV.Elem()
						chJson = ch.marshalToJson(chV.Elem(), &chOVP)
					}
				} else {
					chJson = ch.marshalToJson(chV.Elem(), nil)
				}
				if chJson != nil {
					allSkipped = false
					res[ch.TableName] = chJson
				}
			}

		case reflect.Slice:
			var chOV reflect.Value
			jsonChArray := make([]map[string]interface{}, 0, 10)
			oldRows := make(map[int64]reflect.Value, 10)
			if oldRow != nil {
				chOV = (*oldRow).FieldByIndex(ch.ClassIdx)
				if !chOV.IsNil() {
					ol := chOV.Len()
					for i := 0; i < ol; i++ {
						vv := chOV.Index(i)
						vv = vv.Elem()
						vID := vv.FieldByIndex(ch.PrimaryKey.ClassIdx)
						id, ee := parseInt(vID.Interface())
						if ee != nil {
							oldRows[id] = vv
						}
					}
				}
			}
			if !chV.IsNil() {
				l := chV.Len()
				for i := 0; i < l; i++ {
					vv := chV.Index(i)
					vv = vv.Elem()
					vID := vv.FieldByIndex(ch.PrimaryKey.ClassIdx)
					id, ee := parseInt(vID.Interface())
					if ee == nil {
						chOV, ok := oldRows[id]
						if ok {
							chJson = ch.marshalToJson(vv, &chOV)
							delete(oldRows, id)
						} else {
							chJson = ch.marshalToJson(vv, nil)
						}
					} else {
						chJson = ch.marshalToJson(vv, nil)
					}
					if chJson != nil {
						jsonChArray = append(jsonChArray, chJson)
					}
				}
				if len(jsonChArray) > 0 {
					allSkipped = false
					res[ch.TableName] = jsonChArray
				}
			}

			if len(oldRows) > 0 {
				allSkipped = false
				for id, _ := range oldRows {
					res[fmt.Sprintf("%s[%d]", ch.TableName, id)] = nil
				}
			}
		}
	}

	if allSkipped {
		return nil
	} else {
		return res
	}
}

func parseName(fullName string) (name string, id int64, e error) {
	if strings.HasSuffix(fullName, "]") {
		idx := strings.Index(fullName, "[")
		if idx > 0 {
			ids := fullName[idx+1 : len(fullName)-1]
			id, e = strconv.ParseInt(ids, 10, 64)
			if e != nil {
				return
			}
			name = fullName[:idx]
			if idx == 0 {
				e = fmt.Errorf("Invalid name: %s", fullName)
			}
		}
	} else {
		name = fullName
	}
	return
}

func (t *Table) getId(row reflect.Value) int64 {
	vId := row.FieldByIndex(t.PrimaryKey.ClassIdx)
	k := vId.Type().Kind()
	if k == reflect.Ptr {
		vId = vId.Elem()
	}
	switch k {
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
		return vId.Int()
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(vId.Uint())
	}
	return 0
}

func (t *Table) idxWithId(id int64, slice reflect.Value, lastIdx int) int {
	for i := 0; i < lastIdx; i++ {
		vR := slice.Index(i)
		if vR.IsValid() {
			vR = vR.Elem()
			rowId := t.getId(vR)
			if rowId == id {
				return i
			}
		}
	}
	return -1
}

func (t *Table) applyJson(row reflect.Value, js map[string]interface{}) error {
	var scanner gorbScanner
	var e error
	var name string
	var idx int
	var rowId int64

	for key, value := range js {
		switch jn := value.(type) {
		case []interface{}:
			ch := t.ChildByName(key)
			if ch == nil {
				return fmt.Errorf("There is no scope \"%s\"", key)
			}
			if ch.ChildClass.Kind() != reflect.Slice {
				return fmt.Errorf("Scope \"%s\" is expected to be a slice", key)
			}

			chV := row.FieldByIndex(ch.ClassIdx)
			if chV.IsNil() {
				chV.Set(reflect.MakeSlice(ch.ChildClass, 0, 10))
			}
			lastIdx := chV.Len()
			for _, jsn1 := range jn {
				jsn, ok := jsn1.(map[string]interface{})
				if ok {
					rowId = 0
					iv, ok := jsn[ch.PrimaryKey.FieldName]
					if ok {
						rowId, e = parseInt(iv)
						if e != nil {
							return e
						}
					}
					idx = -1
					if rowId != 0 {
						idx = ch.idxWithId(rowId, chV, lastIdx)
					}
					if idx >= 0 {
						chVV := chV.Index(idx)
						e = ch.applyJson(chVV.Elem(), jsn)
					} else {
						chVV := reflect.New(ch.RowClass)
						chV.Set(reflect.Append(chV, chVV))
						e = ch.applyJson(chVV.Elem(), jsn)
					}
					if e != nil {
						return e
					}
				} else {
					return fmt.Errorf("Unsupported JSON type: %T", jsn1)
				}
			}
		case map[string]interface{}:
			name, rowId, e = parseName(key)
			if e != nil {
				return e
			}
			ch := t.ChildByName(name)
			if ch == nil {
				return fmt.Errorf("There is no scope \"%s\"", name)
			}
			chV := row.FieldByIndex(ch.ClassIdx)
			if chV.IsNil() {
				chVV := reflect.New(ch.RowClass)
				e = ch.applyJson(chVV.Elem(), jn)
				if e != nil {
					return e
				}
				switch ch.ChildClass.Kind() {
				case reflect.Ptr:
					chV.Set(chVV)
				case reflect.Slice:
					chV.Set(reflect.MakeSlice(ch.ChildClass, 0, 10))
					chV.Set(reflect.Append(chV, chVV))
				}
			} else {
				switch ch.ChildClass.Kind() {
				case reflect.Ptr:
					e = ch.applyJson(chV.Elem(), jn)
					if e != nil {
						return e
					}
				case reflect.Slice:
					idx = -1
					if rowId == 0 {
						iv, ok := jn[ch.PrimaryKey.FieldName]
						if ok {
							rowId, e = parseInt(iv)
							if e != nil {
								return e
							}
						}
					}
					if rowId != 0 {
						idx = ch.idxWithId(rowId, chV, chV.Len())
					}
					if idx >= 0 {
						chVV := chV.Index(idx)
						e = ch.applyJson(chVV, jn)
					} else {
						chVV := reflect.New(ch.RowClass)
						chV.Set(reflect.Append(chV, chVV))
						e = ch.applyJson(chVV.Elem(), jn)
					}
					if e != nil {
						return e
					}
				}
			}

		case nil:
			name, rowId, e = parseName(key)
			if e != nil {
				return e
			}
			ch := t.ChildByName(name)
			if ch != nil {
				chV := row.FieldByIndex(ch.ClassIdx)
				if !chV.IsNil() {
					switch ch.ChildClass.Kind() {
					case reflect.Ptr:
						if idx != 0 {
							rId := ch.getId(chV.Elem())
							if rId == rowId {
								chV.Set(reflect.Zero(ch.ChildClass))
							}
						} else {
							chV.Set(reflect.Zero(ch.ChildClass))
						}
					case reflect.Slice:
						if rowId != 0 {
							idx = ch.idxWithId(rowId, chV, chV.Len())
							if idx >= 0 {
								l := chV.Len()
								chIdV := chV.Index(idx)
								chIdV.Set(reflect.Zero(chIdV.Type()))
								if idx == 0 {
									chV.Set(chV.Slice(1, l))
								} else if idx == l-1 {
									chV.Set(chV.Slice(0, l-1))
								} else {
									nS := chV.Slice(0, idx)
									nS = reflect.AppendSlice(nS, chV.Slice(idx+1, l))
									chV.Set(nS)
								}
							}
						} else {
							chV.Set(reflect.Zero(ch.ChildClass))
						}
					}
				}
			} else {
				f := t.FieldByName(key)
				if f == nil {
					return fmt.Errorf("Field %s not found", key)
				}
				fV := row.FieldByIndex(f.ClassIdx)
				fV.Set(reflect.Zero(fV.Type()))
			}
		default:
			f := t.FieldByName(key)
			if f == nil {
				return fmt.Errorf("Field %s not found", key)
			}
			scanner.ptr = row.FieldByIndex(f.ClassIdx).Addr().Interface()
			e = scanner.Scan(value)
			if e != nil {
				return e
			}
		}
	}

	return nil
}

func (conn *GorbManager) EntityJsonApply(entity interface{}, js []byte) error {
	if entity == nil {
		return fmt.Errorf("Entity is nil")
	}

	var ent *Entity
	eType := reflect.TypeOf(entity)
	var isPtr = eType.Kind() == reflect.Ptr
	if !isPtr {
		return fmt.Errorf("Pointer type is expected")
	}
	eType = eType.Elem()
	ent = conn.LookupEntity(eType)
	if ent == nil {
		return fmt.Errorf("Unsupported entity %s", eType.Name())
	}

	var e error
	var j map[string]interface{}
	e = json.Unmarshal(js, &j)
	if e != nil {
		return e
	}

	return ent.applyJson(reflect.ValueOf(entity).Elem(), j)
}

func (conn *GorbManager) EntityJsonGet(newEntity, oldEntity interface{}) (ret []byte, e error) {
	if newEntity == nil {
		return nil, nil
	}

	var ent *Entity
	eType := reflect.TypeOf(newEntity)
	var isPtr = eType.Kind() == reflect.Ptr
	if isPtr {
		eType = eType.Elem()
	}
	ent = conn.LookupEntity(eType)
	if ent == nil {
		return nil, fmt.Errorf("Unsupported entity %s", eType.Name())
	}

	newValue := reflect.ValueOf(newEntity)
	if isPtr {
		newValue = newValue.Elem()
	}

	var res map[string]interface{}

	if oldEntity != nil {
		oldType := reflect.TypeOf(oldEntity)
		isOldPtr := oldType.Kind() == reflect.Ptr
		if isOldPtr {
			oldType = oldType.Elem()
		}
		if eType != oldType {
			return nil, fmt.Errorf("Old entity value has not the same type as ")
		}
		oldValue := reflect.ValueOf(oldEntity)
		if isOldPtr {
			oldValue = oldValue.Elem()
		}
		res = ent.marshalToJson(newValue, &oldValue)

	} else {
		res = ent.marshalToJson(newValue, nil)
	}

	ret, e = json.Marshal(res)
	return
}
