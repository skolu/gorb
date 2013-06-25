package gorb

import (
	"bytes"
	"fmt"
)

func (t *Table) getSelectQuery(tablePath []*Table) string {
	var buffer bytes.Buffer

	buffer.WriteString("SELECT ")
	for i, f := range t.Fields {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(fmt.Sprintf("t%d.%s", t.tableNo, f.sqlName))
	}

	buffer.WriteString(fmt.Sprintf(" FROM %s t%d ", t.TableName, t.tableNo))
	if len(tablePath) > 1 {
		fullPath := append(tablePath, t)
		for i := len(fullPath) - 2; i >= 1; i-- {
			t1 := fullPath[i]
			t2 := fullPath[i+1]
			buffer.WriteString(fmt.Sprintf(" INNER JOIN %s t%d ON t%d.%s = t%d.%s", t1.TableName, t1.tableNo, t1.tableNo, t1.PrimaryKey.sqlName, t2.tableNo, t2.ParentKey.sqlName))
		}
		buffer.WriteString(fmt.Sprintf(" WHERE t%d.%s = ?", tablePath[1].tableNo, tablePath[1].ParentKey.sqlName))
	} else {
		var fld *Field
		if len(tablePath) == 0 {
			fld = t.PrimaryKey
		} else {
			fld = t.ParentKey
		}
		buffer.WriteString(fmt.Sprintf(" WHERE t%d.%s = ?", t.tableNo, fld.sqlName))
	}

	return buffer.String()
}

func (t *Table) getInsertQuery() string {
	var buffer bytes.Buffer

	buffer.WriteString("INSERT INTO ")
	buffer.WriteString(t.TableName)
	buffer.WriteString("(")

	i := 0
	for _, f := range t.Fields {
		if i > 0 {
			buffer.WriteString(", ")
		}
		if f != t.PrimaryKey {
			buffer.WriteString(f.sqlName)
			i++
		}
	}
	buffer.WriteString(") VALUES (")
	for j := 0; j < i; j++ {
		if j > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString("?")
	}
	buffer.WriteString(")")

	return buffer.String()
}

func (t *Table) getUpdateQuery() string {
	var buffer bytes.Buffer

	buffer.WriteString("UPDATE ")
	buffer.WriteString(t.TableName)
	buffer.WriteString(" SET ")

	i := 0
	for _, f := range t.Fields {
		if i > 0 {
			buffer.WriteString(", ")
		}
		if f != t.PrimaryKey {
			i++
			buffer.WriteString(f.sqlName)
			buffer.WriteString("=?")
		}
	}
	buffer.WriteString(" WHERE ")
	buffer.WriteString(t.PrimaryKey.sqlName)
	buffer.WriteString("=?")

	return buffer.String()
}

func (t *Table) getDeleteQuery(tablePath []*Table) string {
	var buffer bytes.Buffer

	if len(tablePath) == 0 {
		buffer.WriteString(fmt.Sprintf("DELETE FROM %s WHERE %s = ?", t.TableName, t.PrimaryKey.sqlName))
	} else {
		tables := append(tablePath, t)

		buffer.WriteString(fmt.Sprintf("DELETE t%d FROM %s t%d", len(tables), t.TableName, len(tables)))
		if len(tablePath) > 0 {
			for i := len(tablePath) - 1; i > 0; i-- {
				buffer.WriteString(fmt.Sprintf("INNER JOIN %s t%d ON t%d.%s = t%d.%s", tables[i-1].TableName, i, i, tables[i-1].PrimaryKey.sqlName, i, tables[i].ParentKey.sqlName))
			}
		}
		buffer.WriteString(fmt.Sprintf(" WHERE t1.%s = ?", tables[0].PrimaryKey.sqlName))
	}

	return buffer.String()
}
