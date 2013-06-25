package gorb

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type DataType uint

const (
	Unsupported DataType = iota
	Bool
	Int32
	Int64
	Float
	DateTime
	String
	Blob
)
const (
	TagPrefix string = "gorb"
	TagPK     string = "pk"    // field: primary key
	TagFK     string = "fk"    // field: foreign key
	TagIndex  string = "index" // field: index
	TagNull   string = "null"  // field: field accepts null
)

type (
	field struct {
		dataType   DataType
		fieldType  reflect.Type
		sqlName    string
		precision  uint16
		isIndex    bool
		isNullable bool
		classIdx   []int
	}

	table struct {
		tableName  string
		fields     []*field
		primaryKey *field
		parentKey  *field
		children   []*childTable
		rowClass   reflect.Type
		stmts      *tableStmts
	}

	childTable struct {
		table
		classIdx   []int
		childClass reflect.Type
	}

	Entity struct {
		table
		tokenField   *field
		teenantField *field
	}
)

type (
	GorbEntityEvents interface {
		OnEntitySave() (bool, error)
		OnEntityInit()
	}

	GorbManager struct {
		entities map[reflect.Type]*Entity
		names    map[string]reflect.Type
		db       *sql.DB
	}
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

func (t *table) init() {
	t.fields = make([]*field, 0, 32)
	t.children = make([]*childTable, 0, 8)
}

func (t *table) check() (bool, error) {
	if t.rowClass == nil {
		return false, errors.New(fmt.Sprintf("No storage class defined"))
	}
	if t.rowClass.Kind() != reflect.Struct {
		return false, errors.New(fmt.Sprintf("Storage class has invalid type: Struct expected"))
	}
	if len(t.fields) == 0 {
		return false, errors.New(fmt.Sprintf("No fields are found in %s.%s", t.rowClass.PkgPath(), t.rowClass.Name()))
	}
	if t.primaryKey == nil {
		return false, errors.New(fmt.Sprintf("table (%s.%s) has no primary key", t.rowClass.PkgPath(), t.rowClass.Name()))
	}
	for _, child := range t.children {
		if child.parentKey == nil {
			return false, errors.New(fmt.Sprintf("table (%s.%s) has no parent key", child.rowClass.PkgPath(), child.rowClass.Name()))
		}

		res, er := child.check()
		if !res {
			return res, er
		}
	}
	return true, nil
}

func (t *table) printGorbSchema() {
	fmt.Printf("CREATE TABLE %s (\n", t.tableName)
	for i, fld := range t.fields {
		if i > 0 {
			fmt.Printf(",\n")
		}
		var ft string
		switch fld.dataType {
		case Bool, Int32, Int64:
			ft = "INTEGER"
		case String:
			ft = "TEXT"
		case Float:
			ft = "REAL"
		case Blob:
			ft = "BLOB"
		case DateTime:
			ft = "DATETIME"
		}
		fmt.Printf("\t%s\t%s", fld.sqlName, ft)
		if t.primaryKey == fld {
			fmt.Print(" PRIMARY KEY")
			if fld.dataType == Int64 || fld.dataType == Int32 {
				fmt.Print(" AUTOINCREMENT")
			}
		} else {
			if fld.dataType != Blob {
				if fld.fieldType.Kind() != reflect.Ptr {
					fmt.Print(" NOT")
				}
			}
			fmt.Print(" NULL")
		}

		if fld.isIndex {
			defer fmt.Printf("CREATE INDEX %s_%s_IDX ON %s(%s);\n", strings.ToUpper(t.tableName), strings.ToUpper(fld.sqlName), t.tableName, fld.sqlName)
		}

	}
	fmt.Printf("\n);\n")

	for _, chld := range t.children {
		chld.printGorbSchema()

		defer fmt.Printf("CREATE INDEX %s_FK ON %s(%s);\n", strings.ToUpper(chld.tableName), chld.tableName, chld.parentKey.sqlName)
	}
}

func (t *table) extractGorbSchema(class reflect.Type, path []int) (bool, error) {
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
				fld := new(field)
				fld.dataType = dataType
				fld.fieldType = ft.Type
				fld.sqlName = strings.TrimSpace(props[0])
				fld.classIdx = append(path, i)

				for i := 1; i < len(props); i++ {
					prop := strings.TrimSpace(props[i])
					prop = strings.ToLower(prop)

					if prop == "pk" {
						if t.primaryKey != nil {
							return false, fmt.Errorf("Duplicate primary key definition")
						}
						t.primaryKey = fld
					} else if prop == "fk" {
						if t.parentKey != nil {
							return false, fmt.Errorf("Duplicate parent key definition")
						}
						t.parentKey = fld
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
				if fld.fieldType.Kind() == reflect.Ptr {
					fld.isNullable = true
				}
				t.fields = append(t.fields, fld)
			} else {
				switch ft.Type.Kind() {
				case reflect.Slice, reflect.Map, reflect.Ptr:
					{ // child entity
						chType := ft.Type.Elem()
						if ft.Type.Kind() != reflect.Ptr {
							chType = chType.Elem()
						}
						if chType.Kind() == reflect.Struct {
							c := new(childTable)
							c.init()
							c.tableName = props[0]
							c.childClass = ft.Type
							c.rowClass = chType
							c.classIdx = append(path, i)
							res, err := c.extractGorbSchema(chType, []int{})
							if res {
								t.children = append(t.children, c)
							} else {
								return res, err
							}
						}
					}
				default:
					return false, errors.New("Unsupported GORB field")
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

func (t *table) flatten(path []*table) []*table {
	path = append(path, t)
	for _, child := range t.children {
		path = child.flatten(path)
	}

	return path
}
func (ent *Entity) Flatten() []*table {
	return ent.table.flatten(make([]*table, 0, 16))
}

func (mgr *GorbManager) PrintGorbSchema(class reflect.Type) {
	ent := mgr.entities[class]
	if ent != nil {
		ent.printGorbSchema()
	} else {
		fmt.Printf("Class %s not registered", class.Name())
	}
}

func (mgr *GorbManager) LookupEntity(class reflect.Type) *Entity {
	if mgr.entities != nil {
		return mgr.entities[class]
	}
	return nil
}

func (mgr *GorbManager) LookupEntityType(tableName string) reflect.Type {
	return mgr.names[tableName]
}

func (mgr *GorbManager) RegisterEntity(class reflect.Type, tableName string) (*Entity, error) {
	if class.Kind() != reflect.Struct {
		return nil, errors.New(fmt.Sprintf("Invalid Gorb entity type: %s. Struct expected", class.Name()))
	}
	var ok bool
	if mgr.entities != nil {
		if _, ok = mgr.entities[class]; ok {
			return nil, errors.New(fmt.Sprintf("Type %s is already registered.", class.Name()))
		}
	}
	if mgr.names != nil {
		if _, ok = mgr.names[tableName]; ok {
			return nil, errors.New(fmt.Sprintf("SQL entity %s is already registered.", tableName))
		}
	}

	e := new(Entity)
	e.init()
	e.tableName = tableName
	e.rowClass = class
	res, err := e.extractGorbSchema(class, []int{})
	if res {
		res, err = e.check()
		if res {
			if mgr.entities == nil {
				mgr.entities = make(map[reflect.Type]*Entity, 16)
			}
			if mgr.names == nil {
				mgr.names = make(map[string]reflect.Type, 16)
			}
			mgr.entities[class] = e
			mgr.names[e.tableName] = class
		}
	}

	return e, err
}

func (mgr *GorbManager) EntityByType(class reflect.Type) (interface{}, error) {
	e, ok := mgr.entities[class]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Class not registered"))
	}

	ret := reflect.New(e.rowClass).Interface()
	initf, ok := ret.(interface {
		OnEntityInit()
	})
	if ok {
		initf.OnEntityInit()
	}

	return ret, nil
}

func (mgr *GorbManager) EntityByName(name string) (interface{}, error) {
	if len(name) == 0 {
		return nil, errors.New("Empty entity name")
	}
	class, ok := mgr.names[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Entity %s is not registered", name))
	}
	ret, err := mgr.EntityByType(class)
	return ret, err
}

func (mgr *GorbManager) SetDB(db *sql.DB) (bool, error) {
	mgr.db = db

	for _, ent := range mgr.entities {
		res, e := ent.createStatements(db, []*table{})
		if !res {
			return res, e
		}
	}

	return true, nil
}
