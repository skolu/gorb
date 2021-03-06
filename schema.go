package gorb

import (
	"fmt"
	"reflect"
)

type DataType uint32

type EntityType uint32

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

type (
	FieldPropertyParser interface {
		ParseFieldProperty(property string, field *Field) error
	}

	Field struct {
		FieldName  string
		DataType   DataType
		FieldType  reflect.Type
		SqlName    string
		Precision  uint16
		IsNullable bool
		IsIndex    bool
		IsRequired bool
		ClassIdx   []int
	}

	Table struct {
		TableName  string
		Fields     []*Field
		PrimaryKey *Field
		Children   []*ChildTable
		RowClass   reflect.Type
		IsPkSerial bool
		tableNo    int32
		stmts      *tableStmts
	}

	ChildTable struct {
		Table
		ParentKey  *Field
		ClassIdx   []int
		ChildClass reflect.Type
	}

	Entity struct {
		Table
		TokenField   *Field
		selectFields string
	}
)

func (t *Table) init() {
	t.Fields = make([]*Field, 0, 32)
	t.Children = make([]*ChildTable, 0, 8)
}

func (t *Table) FieldByName(name string) *Field {
	for _, f := range t.Fields {
		if f.FieldName == name {
			return f
		}
	}
	return nil
}

func (t *Table) ChildByName(name string) *ChildTable {
	for _, ch := range t.Children {
		if ch.TableName == name {
			return ch
		}
	}
	return nil
}

func (t *Table) check() (bool, error) {
	if t.RowClass == nil {
		return false, fmt.Errorf("No storage class defined")
	}
	if t.RowClass.Kind() != reflect.Struct {
		return false, fmt.Errorf("Storage class has invalid type: Struct expected")
	}
	if len(t.Fields) == 0 {
		return false, fmt.Errorf("No fields are found in %s.%s", t.RowClass.PkgPath(), t.RowClass.Name())
	}
	if t.PrimaryKey == nil {
		return false, fmt.Errorf("table (%s.%s) has no primary key", t.RowClass.PkgPath(), t.RowClass.Name())
	}
	for _, child := range t.Children {
		if child.ParentKey == nil {
			return false, fmt.Errorf("table (%s.%s) has no parent key", child.RowClass.PkgPath(), child.RowClass.Name())
		}

		res, er := child.check()
		if !res {
			return res, er
		}
	}
	return true, nil
}

func (t *ChildTable) flatten(path []*ChildTable) []*ChildTable {
	path = append(path, t)
	for _, child := range t.Children {
		path = child.flatten(path)
	}

	return path
}
func (ent *Entity) FlattenChildren() []*ChildTable {
	path := make([]*ChildTable, 0, 16)
	for _, child := range ent.Children {
		path = child.flatten(path)
	}

	return path
}
