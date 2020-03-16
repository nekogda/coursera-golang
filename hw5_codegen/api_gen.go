package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

type APIResponse struct {
	Error    string      `json:"error"`
	Response interface{} `json:"response,omitempty"`
}

func requiredCheck(fieldName, value string) error {
	if len(value) == 0 {
		return fmt.Errorf("%s must me not empty", fieldName)
	}
	return nil
}

func boundCheck(fieldName, value string, hasMin, hasMax bool, min, max int) (int, error) {
	val, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be int", fieldName)
	}
	if hasMin && val < min {
		return 0, fmt.Errorf("%s must be >= %d", fieldName, min)
	}
	if hasMax && val > max {
		return 0, fmt.Errorf("%s must be <= %d", fieldName, max)
	}
	return val, nil
}

func lenCheck(fieldName, value string, hasMin bool, min int) error {
	if hasMin && len(value) < min {
		return fmt.Errorf("%s len must be >= %d", fieldName, min)
	}
	return nil
}

func newResponse(result interface{}, err error) []byte {
	ar := APIResponse{}
	if err != nil {
		ar.Error = err.Error()
	}
	ar.Response = result
	buf, err := json.Marshal(ar)
	if err != nil {
		panic(err.Error())
	}
	return buf
}

func validateCreateParams(p *CreateParams, r *http.Request) error {
	if err := validateCreateParamsAge(p, r); err != nil {
		return err
	}
	if err := validateCreateParamsLogin(p, r); err != nil {
		return err
	}
	if err := validateCreateParamsName(p, r); err != nil {
		return err
	}
	if err := validateCreateParamsStatus(p, r); err != nil {
		return err
	}
	return nil
}

func validateOtherCreateParams(p *OtherCreateParams, r *http.Request) error {
	if err := validateOtherCreateParamsClass(p, r); err != nil {
		return err
	}
	if err := validateOtherCreateParamsLevel(p, r); err != nil {
		return err
	}
	if err := validateOtherCreateParamsName(p, r); err != nil {
		return err
	}
	if err := validateOtherCreateParamsUsername(p, r); err != nil {
		return err
	}
	return nil
}

func validateProfileParams(p *ProfileParams, r *http.Request) error {
	if err := validateProfileParamsLogin(p, r); err != nil {
		return err
	}
	return nil
}

func validateCreateParamsAge(p *CreateParams, r *http.Request) (err error) {
	valueRaw := r.FormValue("age")
	// default case
	if len(valueRaw) == 0 {
		valueRaw = ""
	}
	var value int
	if value, err = boundCheck("age", valueRaw, true, true, 0, 128); err != nil {
		return err
	}
	p.Age = value
	return nil
}

func validateCreateParamsLogin(p *CreateParams, r *http.Request) (err error) {
	valueRaw := r.FormValue("login")
	// default case
	if len(valueRaw) == 0 {
		valueRaw = ""
	}
	if err := requiredCheck("login", valueRaw); err != nil {
		return err
	}
	if err := lenCheck("login", valueRaw, true, 10); err != nil {
		return err
	}
	value := valueRaw
	p.Login = value
	return nil
}

func validateCreateParamsName(p *CreateParams, r *http.Request) (err error) {
	valueRaw := r.FormValue("full_name")
	// default case
	if len(valueRaw) == 0 {
		valueRaw = ""
	}
	if err := lenCheck("full_name", valueRaw, false, 0); err != nil {
		return err
	}
	value := valueRaw
	p.Name = value
	return nil
}

func validateCreateParamsStatus(p *CreateParams, r *http.Request) (err error) {
	valueRaw := r.FormValue("status")
	// default case
	if len(valueRaw) == 0 {
		valueRaw = "user"
	}
	if err := lenCheck("status", valueRaw, false, 0); err != nil {
		return err
	}
	value := valueRaw
	enum := map[string]struct{}{
		"user":      struct{}{},
		"moderator": struct{}{},
		"admin":     struct{}{},
	}
	if _, ok := enum[valueRaw]; !ok {
		variants := strings.Join([]string{"user", "moderator", "admin"}, ", ")
		return fmt.Errorf("%s must be one of [%s]",
			"status", variants)
	}
	p.Status = value
	return nil
}

func validateOtherCreateParamsClass(p *OtherCreateParams, r *http.Request) (err error) {
	valueRaw := r.FormValue("class")
	// default case
	if len(valueRaw) == 0 {
		valueRaw = "warrior"
	}
	if err := lenCheck("class", valueRaw, false, 0); err != nil {
		return err
	}
	value := valueRaw
	enum := map[string]struct{}{
		"warrior":  struct{}{},
		"sorcerer": struct{}{},
		"rouge":    struct{}{},
	}
	if _, ok := enum[valueRaw]; !ok {
		variants := strings.Join([]string{"warrior", "sorcerer", "rouge"}, ", ")
		return fmt.Errorf("%s must be one of [%s]",
			"class", variants)
	}
	p.Class = value
	return nil
}

func validateOtherCreateParamsLevel(p *OtherCreateParams, r *http.Request) (err error) {
	valueRaw := r.FormValue("level")
	// default case
	if len(valueRaw) == 0 {
		valueRaw = ""
	}
	var value int
	if value, err = boundCheck("level", valueRaw, true, true, 1, 50); err != nil {
		return err
	}
	p.Level = value
	return nil
}

func validateOtherCreateParamsName(p *OtherCreateParams, r *http.Request) (err error) {
	valueRaw := r.FormValue("account_name")
	// default case
	if len(valueRaw) == 0 {
		valueRaw = ""
	}
	if err := lenCheck("account_name", valueRaw, false, 0); err != nil {
		return err
	}
	value := valueRaw
	p.Name = value
	return nil
}

func validateOtherCreateParamsUsername(p *OtherCreateParams, r *http.Request) (err error) {
	valueRaw := r.FormValue("username")
	// default case
	if len(valueRaw) == 0 {
		valueRaw = ""
	}
	if err := requiredCheck("username", valueRaw); err != nil {
		return err
	}
	if err := lenCheck("username", valueRaw, true, 3); err != nil {
		return err
	}
	value := valueRaw
	p.Username = value
	return nil
}

func validateProfileParamsLogin(p *ProfileParams, r *http.Request) (err error) {
	valueRaw := r.FormValue("login")
	// default case
	if len(valueRaw) == 0 {
		valueRaw = ""
	}
	if err := requiredCheck("login", valueRaw); err != nil {
		return err
	}
	if err := lenCheck("login", valueRaw, false, 0); err != nil {
		return err
	}
	value := valueRaw
	p.Login = value
	return nil
}

func (h *MyApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/user/profile":
		h.handlerProfile(w, r)

	case "/user/create":
		h.handlerCreate(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("{\"error\": \"unknown method\"}"))
	}
}

func (h *OtherApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/user/create":
		h.handlerCreate(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("{\"error\": \"unknown method\"}"))
	}
}

func checkAuth(w http.ResponseWriter, r *http.Request) bool {
	return r.Header.Get("X-Auth") == "100500"
}

func checkMethod(method string, w http.ResponseWriter, r *http.Request) bool {
	return r.Method == method
}

func checkPanic(w http.ResponseWriter) {
	if e := recover(); e != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (srv *MyApi) handlerProfile(w http.ResponseWriter, r *http.Request) {
	defer checkPanic(w)
	p := ProfileParams{}

	err := validateProfileParams(&p, r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(newResponse(nil, err))
		return
	}

	result, err := srv.Profile(r.Context(), p)
	if err != nil {
		apiError, ok := err.(ApiError)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(apiError.HTTPStatus)
		}
		w.Write(newResponse(nil, err))
		return
	}
	w.Write(newResponse(result, err))
}

func (srv *MyApi) handlerCreate(w http.ResponseWriter, r *http.Request) {
	defer checkPanic(w)
	if !checkAuth(w, r) {
		w.WriteHeader(http.StatusForbidden)
		w.Write(newResponse(nil, fmt.Errorf("unauthorized")))
		return
	}

	if !checkMethod("POST", w, r) {
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write(newResponse(nil, fmt.Errorf("bad method")))
		return
	}

	p := CreateParams{}

	err := validateCreateParams(&p, r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(newResponse(nil, err))
		return
	}

	result, err := srv.Create(r.Context(), p)
	if err != nil {
		apiError, ok := err.(ApiError)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(apiError.HTTPStatus)
		}
		w.Write(newResponse(nil, err))
		return
	}
	w.Write(newResponse(result, err))
}

func (srv *OtherApi) handlerCreate(w http.ResponseWriter, r *http.Request) {
	defer checkPanic(w)
	if !checkAuth(w, r) {
		w.WriteHeader(http.StatusForbidden)
		w.Write(newResponse(nil, fmt.Errorf("unauthorized")))
		return
	}

	if !checkMethod("POST", w, r) {
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write(newResponse(nil, fmt.Errorf("bad method")))
		return
	}

	p := OtherCreateParams{}

	err := validateOtherCreateParams(&p, r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(newResponse(nil, err))
		return
	}

	result, err := srv.Create(r.Context(), p)
	if err != nil {
		apiError, ok := err.(ApiError)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(apiError.HTTPStatus)
		}
		w.Write(newResponse(nil, err))
		return
	}
	w.Write(newResponse(result, err))
}
