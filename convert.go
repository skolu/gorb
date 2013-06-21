package gorb

import (
	"fmt"
	"strconv"
	"time"
)

type (
	gorbScanner struct {
		ptr interface{}
	}
)

func parseDateTimeStr(str string) (t time.Time, err error) {
	switch len(str) {
	case 10:
		t, err = time.Parse(timeFormat[:10], str)
	case 19:
		t, err = time.Parse(timeFormat, str)
	default:
		err = fmt.Errorf("Invalid Time-String: %s", str)
		return
	}

	return
}

func parseTime(value interface{}, dst *time.Time) (err error) {
	err = nil
	switch v := value.(type) {
	case time.Time:
		*dst = v
	case *time.Time:
		*dst = *v
	case []byte:
		*dst, err = parseDateTimeStr(string(v))
	case string:
		*dst, err = parseDateTimeStr(v)
	case int64:
		*dst = time.Unix(value.(int64), 0)
	case uint64:
		*dst = time.Unix(int64(value.(uint64)), 0)
	default:
		err = fmt.Errorf("Can't convert %T to time.Time", value)
	}
	return err
}

func parseString(value interface{}) (sVal string, err error) {
	sVal = ""
	err = nil

	switch src := value.(type) {
	case string:
		{
			sVal = src
		}
	case []byte:
		{
			sVal = string(src)
		}
	default:
		{
			err = fmt.Errorf("Cannot convert to string: %v %T", src, src)
		}
	}
	return
}

func parseBoolean(value interface{}) (bVal bool, err error) {
	bVal = false
	err = nil

	switch src := value.(type) {
	case bool:
		{
			bVal = src
		}
	case uint64, int64, int, uint, uint32, int32, uint16, int16, uint8, int8:
		{
			var iVal int64
			iVal, err = parseInt(value)
			if err == nil {
				bVal = iVal != 0
			}
		}
	case string:
		{
			bVal, err = strconv.ParseBool(src)
		}
	case []byte:
		{
			bVal, err = strconv.ParseBool(string(src))
		}
	default:
		{
			err = fmt.Errorf("Cannot convert to boolean: %v %T", src, src)
		}
	}
	return
}

func parseFloat(value interface{}) (fVal float64, err error) {
	fVal = 0.0
	err = nil
	switch src := value.(type) {
	case float32:
		{
			fVal = float64(src)
		}
	case float64:
		{
			fVal = src
		}
	case uint64, int64, int, uint, uint32, int32, uint16, int16, uint8, int8:
		{
			var iVal int64
			iVal, err = parseInt(value)
			if err == nil {
				fVal = float64(iVal)
			}
		}
	case string:
		{
			fVal, err = strconv.ParseFloat(src, 64)
		}
	case []byte:
		{
			fVal, err = strconv.ParseFloat(string(src), 64)
		}
	default:
		{
			err = fmt.Errorf("Cannot convert to float64: %v %T", src, src)
		}
	}
	return
}

func parseInt(value interface{}) (iVal int64, err error) {
	iVal = 0
	err = nil
	switch src := value.(type) {
	case int64:
		{
			iVal = src
		}
	case uint64:
		{
			iVal = int64(src)
		}
	case int:
		{
			iVal = int64(src)
		}
	case uint:
		{
			iVal = int64(src)
		}
	case int32:
		{
			iVal = int64(src)
		}
	case uint32:
		{
			iVal = int64(src)
		}
	case int16:
		{
			iVal = int64(src)
		}
	case uint16:
		{
			iVal = int64(src)
		}
	case int8:
		{
			iVal = int64(src)
		}
	case uint8:
		{
			iVal = int64(src)
		}
	case string:
		{
			iVal, err = strconv.ParseInt(src, 10, 64)
		}
	case []byte:
		{
			iVal, err = strconv.ParseInt(string(src), 10, 64)
		}
	default:
		{
			err = fmt.Errorf("Cannot convert to int64: %v %T", src, src)
		}
	}

	return
}

func (gs *gorbScanner) Scan(value interface{}) (err error) {
	var iVal int64

	err = nil
	switch dst := gs.ptr.(type) {
	case **time.Time:
		{
			if value == nil {
				*dst = nil
			} else {
				err = parseTime(value, *dst)
			}
		}
	case *time.Time:
		{
			if value == nil {
				*dst = time.Unix(0, 0)
			} else {
				err = parseTime(value, dst)
			}
		}
	case **int64:
		{
			if value == nil {
				*dst = nil
			} else {
				if *dst == nil {
					*dst = new(int64)
				}
				**dst, err = parseInt(value)
			}
		}
	case *int64:
		{
			if value == nil {
				*dst = 0
			} else {
				*dst, err = parseInt(value)
			}
		}
	case **uint64:
		{
			if value == nil {
				*dst = nil
			} else {
				if *dst == nil {
					*dst = new(uint64)
				}
				iVal, err = parseInt(value)
				**dst = uint64(iVal)
			}
		}
	case *uint64:
		{
			if value == nil {
				*dst = 0
			} else {
				iVal, err = parseInt(value)
				*dst = uint64(iVal)
			}
		}
	case **int32:
		{
			if value == nil {
				*dst = nil
			} else {
				if *dst == nil {
					*dst = new(int32)
				}
				iVal, err = parseInt(value)
				**dst = int32(iVal)
			}
		}
	case *int32:
		{
			if value == nil {
				*dst = 0
			} else {
				iVal, err = parseInt(value)
				*dst = int32(iVal)
			}
		}
	case **uint32:
		{
			if value == nil {
				*dst = nil
			} else {
				if *dst == nil {
					*dst = new(uint32)
				}
				iVal, err = parseInt(value)
				**dst = uint32(iVal)
			}
		}
	case *uint32:
		{
			if value == nil {
				*dst = 0
			} else {
				iVal, err = parseInt(value)
				*dst = uint32(iVal)
			}
		}
	case **int:
		{
			if value == nil {
				*dst = nil
			} else {
				if *dst == nil {
					*dst = new(int)
				}
				iVal, err = parseInt(value)
				**dst = int(iVal)
			}
		}
	case *int:
		{
			if value == nil {
				*dst = 0
			} else {
				iVal, err = parseInt(value)
				*dst = int(iVal)
			}
		}

	case **bool:
		{
			if value == nil {
				*dst = nil
			} else {
				if *dst == nil {
					*dst = new(bool)
				}
				**dst, err = parseBoolean(value)
			}
		}
	case *bool:
		{
			if value == nil {
				*dst = false
			} else {
				*dst, err = parseBoolean(value)
			}
		}

	case **string:
		{
			if value == nil {
				*dst = nil
			} else {
				if *dst == nil {
					*dst = new(string)
				}
				**dst, err = parseString(value)
			}
		}
	case *string:
		{
			if value == nil {
				*dst = ""
			} else {
				*dst, err = parseString(value)
			}
		}

	}
	return err
}
