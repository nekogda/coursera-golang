package main

import (
	"errors"
	"reflect"
)

func i2s(data interface{}, out interface{}) error {
	outVal := reflect.ValueOf(out)
	if !reflect.Indirect(outVal).CanSet() {
		return errors.New("expected settable out")
	}
	switch outVal.Elem().Type().Kind() {
	case reflect.Int:
		v, ok := data.(float64)
		if !ok {
			return errors.New("expect float, got: " + reflect.TypeOf(data).String())
		}
		outVal.Elem().SetInt(int64(v))
	case reflect.Bool:
		v, ok := data.(bool)
		if !ok {
			return errors.New("expect bool, got: " + reflect.TypeOf(data).String())
		}
		outVal.Elem().SetBool(v)
	case reflect.Slice:
		dataSlice, ok := data.([]interface{})
		if !ok {
			return errors.New("expected []interface{}")
		}
		var outSlice reflect.Value
		if outVal.Elem().IsNil() {
			outSlice = reflect.MakeSlice(outVal.Elem().Type(), 0, 0)
		} else {
			outSlice = outVal.Elem()
		}

		for i := range dataSlice {
			v := dataSlice[i]
			elementPtr := reflect.New(outSlice.Type().Elem())
			err := i2s(v, elementPtr.Interface())
			if err != nil {
				return err
			}
			outSlice = reflect.Append(outSlice, elementPtr.Elem())
		}
		outVal.Elem().Set(outSlice)
	case reflect.String:
		v, ok := data.(string)
		if !ok {
			return errors.New("expect string, got: " + reflect.TypeOf(data).String())
		}
		outVal.Elem().SetString(v)
	case reflect.Struct:
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return errors.New("expected map[string]interface{}")
		}
		for i := 0; i < outVal.Elem().NumField(); i++ {
			fieldName := outVal.Elem().Type().Field(i).Name
			fieldPtr := outVal.Elem().Field(i).Addr()
			v, ok := dataMap[fieldName]
			if !ok {
				return errors.New("there is no value for field: " + fieldName)
			}
			err := i2s(v, fieldPtr.Interface())
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected type: " + outVal.Elem().Type().String())
	}
	return nil
}
