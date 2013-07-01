package gorb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	TagPrefix  string = "gorb"
	TagPK      string = "pk"      // field: primary key
	TagFK      string = "fk"      // field: foreign key
	TagToken   string = "token"   // field: sync token
	TagTeenant string = "teenant" // field:
	TagIndex   string = "index"   // field: index
	TagNull    string = "null"    // field: field accepts null
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

func (t *Table) ParseFieldProperty(property string, field *Field) error {
	if property == TagPK {
		if t.PrimaryKey != nil {
			return fmt.Errorf("Duplicate primary key definition")
		}
		if field.DataType == Int32 || field.DataType == Int64 {
			t.IsPkSerial = true
		} else if field.DataType != String {
			return fmt.Errorf("Column \"%s\" in table \"%s\" cannot be Primary Key", field.sqlName, t.TableName)
		}
		t.PrimaryKey = field
	} else if property == TagIndex {
		field.isIndex = true
	} else if property == TagNull {
		field.isNullable = true
	} else if strings.HasPrefix(property, ":") {
		i16, e := strconv.ParseInt(property[1:], 10, 16)
		if e == nil {
			field.precision = uint16(i16)
		}
	} else {
		return fmt.Errorf("Unsupported property %s for field %s", property, field.sqlName)
	}

	return nil
}

func (c *ChildTable) ParseFieldProperty(property string, field *Field) error {
	if property == TagFK {
		if c.ParentKey != nil {
			return fmt.Errorf("Duplicate parent key definition")
		}
		c.ParentKey = field
		if c.PrimaryKey == c.ParentKey {
			c.IsPkSerial = false
		}
	} else {
		var t *Table = &((*c).Table)
		return t.ParseFieldProperty(property, field)
	}
	return nil
}

func (e *Entity) ParseFieldProperty(property string, field *Field) error {
	if property == TagToken {
		if e.TokenField != nil {
			return fmt.Errorf("Duplicate token field definition")
		}
		e.TokenField = field
	} else if property == TagTeenant {
		if e.TeenantField != nil {
			return fmt.Errorf("Duplicate teenant field definition")
		}
		e.TeenantField = field
	} else {
		var t *Table = &((*e).Table)
		return t.ParseFieldProperty(property, field)
	}
	return nil
}

func (t *Table) extractGorbSchema(class reflect.Type, path []int, propertyParser FieldPropertyParser) (bool, error) {
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
				fld.FieldName = ft.Name
				fld.DataType = dataType
				fld.FieldType = ft.Type
				fld.sqlName = strings.TrimSpace(props[0])
				fld.classIdx = append(path, i)

				for i := 1; i < len(props); i++ {
					prop := strings.TrimSpace(props[i])
					prop = strings.ToLower(prop)

					e := propertyParser.ParseFieldProperty(prop, fld)
					if e != nil {
						return false, e
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
							res, err := c.extractGorbSchema(chType, []int{}, c)
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
			res, err := t.extractGorbSchema(ft.Type, append(path, i), propertyParser)
			if !res {
				return res, err
			}
		}

	}

	return true, nil
}
