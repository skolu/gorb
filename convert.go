package gorb

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unicode"
)

const (
	timeFormat = "2006-01-02 15:04:05"
)

type (
	gorbScanner struct {
		ptr interface{}
	}
)

func isZeroDateStr(str string) bool {
	var hasDigit = false
	for _, r := range str {
		if unicode.IsDigit(r) {
			if r != '0' {
				return false
			}
			hasDigit = true
		} else if unicode.IsPunct(r) {
		} else if unicode.IsSpace(r) {
		} else {
			return false
		}
	}
	return hasDigit
}
func parseDateTimeStr(str string) (t time.Time, err error) {
	switch len(str) {
	case 10:
		if !isZeroDateStr(str) {
			t, err = time.Parse(timeFormat[:10], str)
		}
	case 19:
		if !isZeroDateStr(str) {
			t, err = time.ParseInLocation(timeFormat, str, time.UTC)
		}
	default:
		err = fmt.Errorf("Invalid Time-String: %s", str)
		return
	}

	return
}

func parseTime(value interface{}, dst *time.Time) (err error) {
	err = nil
	switch v := value.(type) {
	case nil:
		*dst = time.Unix(0, 0)
	case time.Time:
		*dst = v
	case *time.Time:
		*dst = *v
	case []byte:
		*dst, err = parseDateTimeStr(string(v))
	case string:
		*dst, err = parseDateTimeStr(v)
	case int64:
		*dst = time.Unix(v, 0)
	case uint64:
		*dst = time.Unix(int64(v), 0)
	default:
		err = fmt.Errorf("Can't convert %T to time.Time", value)
	}
	return err
}

func parseString(value interface{}) (sVal string, err error) {
	sVal = ""
	err = nil

	switch src := value.(type) {
	case nil:
	case string:
		sVal = src
	case []byte:
		sVal = string(src)
	default:
		err = fmt.Errorf("Cannot convert to string: %v %T", src, src)
	}
	return
}

func parseBoolean(value interface{}) (bVal bool, err error) {
	bVal = false
	err = nil

	switch src := value.(type) {
	case nil:
	case bool:
		bVal = src
	case uint64, int64, int, uint, uint32, int32, uint16, int16, uint8, int8:
		var iVal int64
		iVal, err = parseInt(value)
		if err == nil {
			bVal = iVal != 0
		}
	case string:
		bVal, err = strconv.ParseBool(src)
	case []byte:
		if len(src) == 1 {
			if src[0] == 0x0 {
				bVal = false
			} else {
				bVal = true
			}

		} else {
			bVal, err = strconv.ParseBool(string(src))
		}
	default:
		err = fmt.Errorf("Cannot convert to boolean: %v %T", src, src)
	}
	return
}

func parseFloat(value interface{}) (fVal float64, err error) {
	fVal = 0.0
	err = nil
	switch src := value.(type) {
	case nil:
	case float32:
		fVal = float64(src)
	case float64:
		fVal = src
	case uint64, int64, int, uint, uint32, int32, uint16, int16, uint8, int8:
		var iVal int64
		iVal, err = parseInt(value)
		if err == nil {
			fVal = float64(iVal)
		}
	case string:
		fVal, err = strconv.ParseFloat(src, 64)
	case []byte:
		fVal, err = strconv.ParseFloat(string(src), 64)
	default:
		err = fmt.Errorf("Cannot convert to float64: %v %T", src, src)
	}
	return
}

func parseInt(value interface{}) (iVal int64, err error) {
	iVal = 0
	err = nil
	switch src := value.(type) {
	case nil:
	case int64:
		iVal = src
	case uint64:
		iVal = int64(src)
	case int:
		iVal = int64(src)
	case uint:
		iVal = int64(src)
	case int32:
		iVal = int64(src)
	case uint32:
		iVal = int64(src)
	case int16:
		iVal = int64(src)
	case uint16:
		iVal = int64(src)
	case int8:
		iVal = int64(src)
	case uint8:
		iVal = int64(src)
	case string:
		iVal, err = strconv.ParseInt(src, 10, 64)
	case []byte:
		iVal, err = strconv.ParseInt(string(src), 10, 64)
	default:
		err = fmt.Errorf("Cannot convert to int64: %v %T", src, src)
	}

	return
}

func parseBlob(value interface{}) (blobVal []byte, err error) {
	blobVal = nil
	err = nil
	switch src := value.(type) {
	case nil:
	case []byte:
		blobVal = src
	case string:
		blobVal = []byte(src)
	default:
		err = fmt.Errorf("Cannot convert to []byte: %v %T", src, src)
	}
	return
}

func (gs *gorbScanner) Scan(value interface{}) (err error) {
	var iVal int64

	err = nil
	switch dst := gs.ptr.(type) {
	case **time.Time:
		if value == nil {
			*dst = nil
		} else {
			err = parseTime(value, *dst)
		}
	case *time.Time:
		if value == nil {
			*dst = time.Unix(0, 0)
		} else {
			err = parseTime(value, dst)
		}
	case **int64:
		if value == nil {
			*dst = nil
		} else {
			if *dst == nil {
				*dst = new(int64)
			}
			**dst, err = parseInt(value)
		}
	case *int64:
		*dst, err = parseInt(value)
	case **uint64:
		if value == nil {
			*dst = nil
		} else {
			if *dst == nil {
				*dst = new(uint64)
			}
			iVal, err = parseInt(value)
			**dst = uint64(iVal)
		}
	case *uint64:
		iVal, err = parseInt(value)
		*dst = uint64(iVal)
	case **int32:
		if value == nil {
			*dst = nil
		} else {
			if *dst == nil {
				*dst = new(int32)
			}
			iVal, err = parseInt(value)
			**dst = int32(iVal)
		}
	case *int32:
		iVal, err = parseInt(value)
		*dst = int32(iVal)
	case **uint32:
		if value == nil {
			*dst = nil
		} else {
			if *dst == nil {
				*dst = new(uint32)
			}
			iVal, err = parseInt(value)
			**dst = uint32(iVal)
		}
	case *uint32:
		iVal, err = parseInt(value)
		*dst = uint32(iVal)
	case **int:
		if value == nil {
			*dst = nil
		} else {
			if *dst == nil {
				*dst = new(int)
			}
			iVal, err = parseInt(value)
			**dst = int(iVal)
		}
	case *int:
		iVal, err = parseInt(value)
		*dst = int(iVal)

	case **float64:
		if value == nil {
			*dst = nil
		} else {
			if *dst == nil {
				*dst = new(float64)
			}
			**dst, err = parseFloat(value)
		}

	case *float64:
		*dst, err = parseFloat(value)

	case **float32:
		if value == nil {
			*dst = nil
		} else {
			if *dst == nil {
				*dst = new(float32)
			}

			var fVal float64
			fVal, err = parseFloat(value)
			if err == nil {
				**dst = float32(fVal)
			}
		}

	case *float32:
		var fVal float64
		fVal, err = parseFloat(value)
		if err == nil {
			*dst = float32(fVal)
		}

	case **bool:
		if value == nil {
			*dst = nil
		} else {
			if *dst == nil {
				*dst = new(bool)
			}
			**dst, err = parseBoolean(value)
		}
	case *bool:
		*dst, err = parseBoolean(value)

	case **string:
		if value == nil {
			*dst = nil
		} else {
			if *dst == nil {
				*dst = new(string)
			}
			**dst, err = parseString(value)
		}
	case *string:
		if value == nil {
			*dst = ""
		} else {
			*dst, err = parseString(value)
		}

	case **[]byte:
		if value == nil {
			*dst = nil
		} else {
			if *dst == nil {
				*dst = new([]byte)
			}
			**dst, err = parseBlob(value)
		}
	case *[]byte:
		if value == nil {
			if len(*dst) > 0 {
				(*dst) = nil
			}
		} else {
			*dst, err = parseBlob(value)
		}

	default:
		v := reflect.ValueOf(gs.ptr)
		if v.Kind() == reflect.Ptr {
			if !v.IsNil() {
				v = v.Elem()
				var k reflect.Kind = v.Kind()
				switch k {
				case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16:
					{
						iVal, err = parseInt(value)
						if err == nil {
							v.SetInt(iVal)
							return nil
						}
					}
				case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16:
					{
						iVal, err = parseInt(value)
						if err == nil {
							v.SetUint(uint64(iVal))
							return nil
						}
					}
				}
			}
		}
		err = fmt.Errorf("Failed to covert: %T", dst)
	}
	return err
}
