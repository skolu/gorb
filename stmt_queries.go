package gorb

import (
	"bytes"
	"fmt"
)

func (c *ChildTable) getInfoQuery(tablePath []*ChildTable) string {
	if len(tablePath) == 0 {
		return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", c.PrimaryKey.SqlName, c.TableName, c.ParentKey.SqlName)
	}

	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("SELECT t%d.%s FROM %s t%d", c.tableNo, c.PrimaryKey.SqlName, c.TableName, c.tableNo))

	fullPath := append(tablePath, c)
	for i := len(fullPath) - 2; i >= 0; i-- {
		t1 := fullPath[i]
		t2 := fullPath[i+1]
		buffer.WriteString(fmt.Sprintf(" INNER JOIN %s t%d ON t%d.%s = t%d.%s", t1.TableName, t1.tableNo, t1.tableNo, t1.PrimaryKey.SqlName, t2.tableNo, t2.ParentKey.SqlName))
	}

	buffer.WriteString(fmt.Sprintf(" WHERE t%d.%s = ?", tablePath[0].tableNo, tablePath[0].ParentKey.SqlName))

	return buffer.String()
}

func (e *Entity) getInfoQuery() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("SELECT %s", e.PrimaryKey.SqlName))
	if e.TokenField != nil {
		buffer.WriteString(fmt.Sprintf(", %s", e.TokenField.SqlName))
	} else {
		buffer.WriteString(", 0")
	}
	buffer.WriteString(fmt.Sprintf(" FROM %s WHERE %s = ?", e.TableName, e.PrimaryKey.SqlName))

	return buffer.String()
}

func (c *ChildTable) getSelectQuery(tablePath []*ChildTable) string {
	var buffer bytes.Buffer

	buffer.WriteString("SELECT ")
	for i, f := range c.Fields {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(fmt.Sprintf("t%d.%s", c.tableNo, f.SqlName))
	}

	buffer.WriteString(fmt.Sprintf(" FROM %s t%d ", c.TableName, c.tableNo))
	if len(tablePath) > 1 {
		fullPath := append(tablePath, c)
		for i := len(fullPath) - 2; i >= 1; i-- {
			t1 := fullPath[i]
			t2 := fullPath[i+1]
			buffer.WriteString(fmt.Sprintf(" INNER JOIN %s t%d ON t%d.%s = t%d.%s", t1.TableName, t1.tableNo, t1.tableNo, t1.PrimaryKey.SqlName, t2.tableNo, t2.ParentKey.SqlName))
		}
		buffer.WriteString(fmt.Sprintf(" WHERE t%d.%s = ?", tablePath[1].tableNo, tablePath[1].ParentKey.SqlName))
	} else {
		buffer.WriteString(fmt.Sprintf(" WHERE t%d.%s = ?", c.tableNo, c.ParentKey.SqlName))
	}

	return buffer.String()
}

func (e *Entity) getSelectFields() string {
	var buffer bytes.Buffer

	buffer.WriteString("SELECT ")
	for i, f := range e.Fields {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(f.SqlName)
	}

	buffer.WriteString(fmt.Sprintf(" FROM %s", e.TableName))

	return buffer.String()
}

func (e *Entity) getSelectQuery() string {
	var buffer bytes.Buffer

	buffer.WriteString("SELECT ")
	for i, f := range e.Fields {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(f.SqlName)
	}

	buffer.WriteString(fmt.Sprintf(" FROM %s WHERE %s = ?", e.TableName, e.PrimaryKey.SqlName))

	return buffer.String()
}

func (c *ChildTable) getInsertQuery() string {
	var buffer bytes.Buffer

	buffer.WriteString("INSERT INTO ")
	buffer.WriteString(c.TableName)
	buffer.WriteString("(")

	i := 0
	if !c.IsPkSerial { // put PK first
		buffer.WriteString(c.PrimaryKey.SqlName)
		i++
	}
	for _, f := range c.Fields {
		if f != c.PrimaryKey {
			if i > 0 {
				buffer.WriteString(", ")
			}
			buffer.WriteString(f.SqlName)
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

func (e *Entity) getInsertQuery() string {
	var buffer bytes.Buffer

	buffer.WriteString("INSERT INTO ")
	buffer.WriteString(e.TableName)
	buffer.WriteString("(")

	i := 0
	if !e.IsPkSerial { // put PK first
		buffer.WriteString(e.PrimaryKey.SqlName)
		i++
	}
	for _, f := range e.Fields {
		if f != e.PrimaryKey {
			if i > 0 {
				buffer.WriteString(", ")
			}
			buffer.WriteString(f.SqlName)
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
			buffer.WriteString(f.SqlName)
			buffer.WriteString("=?")
		}
	}
	buffer.WriteString(" WHERE ")
	buffer.WriteString(t.PrimaryKey.SqlName)
	buffer.WriteString("=?")

	return buffer.String()
}

func (t *Table) getRemoveQuery() string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = ?", t.TableName, t.PrimaryKey.SqlName)
}

func (c *ChildTable) getDeleteQuery(tablePath []*ChildTable) string {
	var buffer bytes.Buffer

	if len(tablePath) == 0 {
		buffer.WriteString(fmt.Sprintf("DELETE FROM %s WHERE %s = ?", c.TableName, c.ParentKey.SqlName))
	} else {
		open := 1
		buffer.WriteString(fmt.Sprintf("DELETE FROM %s WHERE %s IN (", c.TableName, c.ParentKey.SqlName))
		for i := len(tablePath) - 1; i >= 0; i-- {
			tbl := tablePath[i]
			if i > 1 {
				open++
				buffer.WriteString(fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (", tbl.PrimaryKey.SqlName, tbl.TableName, tbl.ParentKey.SqlName))

			} else {
				buffer.WriteString(fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", tbl.PrimaryKey.SqlName, tbl.TableName, tbl.ParentKey.SqlName))
			}

		}
		for open > 0 {
			buffer.WriteString(")")
			open--
		}
	}

	return buffer.String()
}

func (e *Entity) getDeleteQuery() string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = ?", e.TableName, e.PrimaryKey.SqlName)
}
