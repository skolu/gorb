package gorb

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

type WhereOperation uint32

const (
	OpEqual WhereOperation = iota
	OpLess
	OpGreater
	OpLike
)

type (
	SortCriteria struct {
		Field *Field
		IsAsc bool
	}

	WhereCriteria struct {
		field     *Field
		operation WhereOperation
		isExclude bool
		value     interface{}
	}

	WhereClause [][]*WhereCriteria

	RequestQuery struct {
		ent          *Entity
		IsHeaderOnly bool
		Limit        uint32
		Offset       uint32
		WhereClause  WhereClause
		SortClause   []*SortCriteria
	}
)

func (mgr *GorbManager) QueryForType(eType reflect.Type) (*RequestQuery, error) {
	var ent *Entity
	if eType.Kind() == reflect.Ptr {
		eType = eType.Elem()
	}
	ent = mgr.LookupEntity(eType)
	if ent == nil {
		return nil, fmt.Errorf("Unsupported entity %s", eType.Name())
	}

	var rq *RequestQuery = new(RequestQuery)
	rq.ent = ent

	return rq, nil
}

func (wc *WhereCriteria) Exclude() *WhereCriteria {
	(*wc).isExclude = true
	return wc
}

func (rq *RequestQuery) NewWhereCriteria(fieldName string, op WhereOperation, value interface{}) (*WhereCriteria, error) {
	var e error = nil
	var fld *Field = nil

	for _, f := range rq.ent.Fields {
		if f.FieldName == fieldName {
			fld = f
			break
		}

		if f.sqlName == fieldName {
			fld = f
			break
		}
	}
	if fld == nil {
		e = fmt.Errorf("Field name \"%s\" not found in entity \"%s\"", fieldName, rq.ent.TableName)
		return nil, e
	}

	if value != nil {
		if op != OpEqual {
			e = fmt.Errorf("Invalid where criteria: Compare to NULL")
		}
	}

	var wc *WhereCriteria = new(WhereCriteria)
	wc.field = fld
	wc.isExclude = false
	wc.operation = op
	wc.value = value

	return wc, nil
}

func (rq *RequestQuery) Where(criteria *WhereCriteria) WhereClause {
	rq.WhereClause = make([][]*WhereCriteria, 0, 4)
	rq.WhereClause = append(rq.WhereClause, make([]*WhereCriteria, 0, 4))
	if criteria != nil {
		rq.WhereClause[0] = append(rq.WhereClause[0], criteria)
	}

	return rq.WhereClause
}

func (wc *WhereClause) And(criteria *WhereCriteria) WhereClause {
	var lastIdx int = len(*wc) - 1
	if lastIdx >= 0 {
		(*wc)[lastIdx] = append((*wc)[lastIdx], criteria)
	}

	return *wc
}

func (wc *WhereClause) Or(criteria *WhereCriteria) WhereClause {
	*wc = append(*wc, make([]*WhereCriteria, 0, 4))
	var lastIdx int = len(*wc) - 1
	if lastIdx >= 0 {
		(*wc)[lastIdx] = append((*wc)[lastIdx], criteria)
	}

	return *wc
}

func (wc *WhereClause) Dump() {
	where, params := wc.createWhereClause()
	fmt.Println(where)
	fmt.Printf("%q\n", params)
}

func (wc *WhereClause) createWhereClause() (string, []interface{}) {
	var buffer bytes.Buffer
	var params []interface{} = make([]interface{}, 0, 8)

	for i, a := range *wc {
		if i > 0 {
			buffer.WriteString(" OR ")
		}
		var doOrParentheses bool = len(a) > 1
		if doOrParentheses {
			buffer.WriteString("(")
		}
		for j, c := range a {
			if j > 0 {
				buffer.WriteString(" AND ")
			}
			buffer.WriteString("(")
			buffer.WriteString(c.field.sqlName)
			if c.value == nil {
				buffer.WriteString(" IS")
				if c.isExclude {
					buffer.WriteString(" NOT")
				}
				buffer.WriteString(" NULL")
			} else {
				switch c.operation {
				case OpEqual:
					if c.isExclude {
						buffer.WriteString(" <>")
					} else {
						buffer.WriteString(" =")
					}
				case OpLess:
					if c.isExclude {
						buffer.WriteString(" >=")
					} else {
						buffer.WriteString(" <")
					}
				case OpGreater:
					if c.isExclude {
						buffer.WriteString(" <=")
					} else {
						buffer.WriteString(" >")
					}
				case OpLike:
					if c.isExclude {
						buffer.WriteString(" NOT")
					}
					buffer.WriteString(" LIKE")
				default:
					buffer.WriteString(" ### ")
				}

				buffer.WriteString(" ?")
				switch p := c.value.(type) {
				case time.Time:
					c.value = p.UTC()
				}
				params = append(params, c.value)
			}
			buffer.WriteString(")")
		}
		if doOrParentheses {
			buffer.WriteString(")")
		}
	}

	return buffer.String(), params
}

func (mgr *GorbManager) EntityQueryIds(request *RequestQuery) ([]int64, error) {
	if mgr.db == nil {
		return nil, fmt.Errorf("Database connection is not set")
	}

	var e error = nil

	var query bytes.Buffer
	var whereClause string
	var whereParams []interface{}

	query.WriteString(fmt.Sprintf("SELECT %s FROM %s", request.ent.PrimaryKey.sqlName, request.ent.TableName))

	if len(request.WhereClause) > 0 {
		whereClause, whereParams = request.WhereClause.createWhereClause()
		query.WriteString(" WHERE ")
		query.WriteString(whereClause)
	}

	if request.Limit > 0 {
		query.WriteString(fmt.Sprintf(" LIMIT %d", request.Limit))
		if request.Offset > 0 {
			query.WriteString(fmt.Sprintf(" OFFSET %d", request.Offset))
		}
	}

	var rows *sql.Rows
	rows, e = mgr.db.Query(query.String(), whereParams...)
	if e != nil {
		return nil, e
	}

	var ids []int64 = make([]int64, 0, 64)
	for rows.Next() {
		var id int64
		e = rows.Scan(&id)
		if e != nil {
			break
		}
		ids = append(ids, id)
	}

	rows.Close()
	if e != nil {
		return nil, e
	}

	return ids, nil
}

func (mgr *GorbManager) EntityQuery(request *RequestQuery) ([]interface{}, error) {
	if mgr.db == nil {
		return nil, fmt.Errorf("Database connection is not set")
	}

	var e error = nil

	var query bytes.Buffer
	var whereClause string
	var whereParams []interface{}

	query.WriteString(request.ent.selectFields)

	if len(request.WhereClause) > 0 {
		whereClause, whereParams = request.WhereClause.createWhereClause()
		query.WriteString(" WHERE ")
		query.WriteString(whereClause)
	}

	if request.Limit > 0 {
		query.WriteString(fmt.Sprintf(" LIMIT %d", request.Limit))
		if request.Offset > 0 {
			query.WriteString(fmt.Sprintf(" OFFSET %d", request.Offset))
		}
	}

	var queryStr string = query.String()
	var rows *sql.Rows
	rows, e = mgr.db.Query(queryStr, whereParams...)
	if e != nil {
		return nil, e
	}

	var recordSet []interface{} = make([]interface{}, 0, 64)
	var fields []interface{} = make([]interface{}, len(request.ent.Fields))
	for i, _ := range request.ent.Fields {
		fields[i] = new(gorbScanner)
	}

	for rows.Next() {
		var pV reflect.Value = reflect.New(request.ent.RowClass)
		var v = pV.Elem()
		for i, f := range request.ent.Fields {
			var gs *gorbScanner
			var ok bool
			gs, ok = fields[i].(*gorbScanner)
			if ok {
				fV := v.FieldByIndex(f.classIdx)
				gs.ptr = fV.Addr().Interface()
			} else {
				e = fmt.Errorf("Should not happen")
				break
			}
		}
		if e != nil {
			break
		}

		e = rows.Scan(fields...)
		if e != nil {
			break
		}

		if !request.IsHeaderOnly {
			e = request.ent.populateChildren(v)
		}

		if e != nil {
			break
		}
		recordSet = append(recordSet, pV.Interface())
	}
	rows.Close()
	if e != nil {
		return nil, e
	}

	return recordSet, nil
}
