package gorb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	TagPrefix string = "gorb"
	TagPK     string = "pk"    // field: primary key
	TagFK     string = "fk"    // field: foreign key
	TagIndex  string = "index" // field: index
	TagNull   string = "null"  // field: field accepts null
)

var (
	dateTimeType reflect.Type
)

func init() {
	dateTimeType = reflect.TypeOf((*time.Time)(nil)).Elem()
}

func getPrimitiveDataType(t reflect.Type) DataType {
	dt := t
	if t.Kind() == reflect.Ptr {
		dt = t.Elem()
	}

	if dt == dateTimeType {
		return DateTime
	}

	switch dt.Kind() {
	case reflect.Bool:
		return Bool
	case reflect.Uint8, reflect.Uint16, reflect.Int8, reflect.Int16:
		fmt.Printf("short integer fields are not supported")

	case reflect.Int, reflect.Uint, reflect.Int64, reflect.Uint64:
		return Int64
	case reflect.Int32, reflect.Uint32:
		return Int32
	case reflect.Float32, reflect.Float64:
		return Float
	case reflect.String:
		return String
	case reflect.Slice:
		{
			if dt.Elem().Kind() == reflect.Uint8 {
				return Blob
			}
		}
	}

	return Unsupported
}

func (t *Table) extractGorbSchema(class reflect.Type, path []int) (bool, error) {
	for i := 0; i < class.NumField(); i++ {
		ft := class.Field(i)

		gorbTag := ft.Tag.Get(TagPrefix)

		if len(gorbTag) > 0 {
			props := strings.Split(gorbTag, ",")
			if len(props) == 0 {
				return false, fmt.Errorf("Invalid GORB tag for field: %s", ft.Name)
			}
			dataType := getPrimitiveDataType(ft.Type)
			if dataType != Unsupported {
				fld := new(Field)
				fld.DataType = dataType
				fld.FieldType = ft.Type
				fld.sqlName = strings.TrimSpace(props[0])
				fld.classIdx = append(path, i)

				for i := 1; i < len(props); i++ {
					prop := strings.TrimSpace(props[i])
					prop = strings.ToLower(prop)

					if prop == "pk" {
						if t.PrimaryKey != nil {
							return false, fmt.Errorf("Duplicate primary key definition")
						}
						t.PrimaryKey = fld
					} else if prop == "fk" {
						if t.ParentKey != nil {
							return false, fmt.Errorf("Duplicate parent key definition")
						}
						t.ParentKey = fld
					} else if prop == "index" {
						fld.isIndex = true
					} else if prop == "null" {
						fld.isNullable = true
					} else if strings.HasPrefix(prop, ":") {
						i16, e := strconv.ParseInt(prop[1:], 10, 16)
						if e == nil {
							fld.precision = uint16(i16)
						}
					} else {
						return false, fmt.Errorf("Unsupported field property: %s", prop)
					}
				}
				if fld.FieldType.Kind() == reflect.Ptr {
					fld.isNullable = true
				}
				t.Fields = append(t.Fields, fld)
			} else {
				switch ft.Type.Kind() {
				case reflect.Slice, reflect.Map, reflect.Ptr:
					{ // child entity
						chType := ft.Type.Elem()
						if ft.Type.Kind() != reflect.Ptr {
							chType = chType.Elem()
						}
						if chType.Kind() == reflect.Struct {
							c := new(ChildTable)
							c.init()
							c.TableName = props[0]
							c.ChildClass = ft.Type
							c.RowClass = chType
							c.ClassIdx = append(path, i)
							res, err := c.extractGorbSchema(chType, []int{})
							if res {
								t.Children = append(t.Children, c)
							} else {
								return res, err
							}
						}
					}
				default:
					return false, fmt.Errorf("Unsupported GORB field")
				}
			}

		} else if ft.Type.Kind() == reflect.Struct {
			res, err := t.extractGorbSchema(ft.Type, append(path, i))
			if !res {
				return res, err
			}
		}

	}

	return true, nil
}
