package main

import (
	"encoding/json"
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type SearchServer struct {
	path string
}

type UserFromDS struct {
	Id     int    `xml:"id"`
	Age    int    `xml:"age"`
	FName  string `xml:"first_name", json:"-"`
	LName  string `xml:"last_name", json:"-"`
	Name   string
	About  string `xml:"about"`
	Gender string `xml:"gender"`
}

type dataset struct {
	Data []UserFromDS `xml:"row"`
}

type message struct {
	orderField string
	query      string
	limit      int
	orderBy    int
}

type BadOrderFieldError struct{}

func (e BadOrderFieldError) Error() string {
	return "ErrorBadOrderField"
}

type UnknownBadOrderFieldError struct{}

func (e UnknownBadOrderFieldError) Error() string {
	return "unknown bad orderField error"
}

type BadJSONError struct{}

func (e BadJSONError) Error() string {
	return "json is bad"
}

type BadJSONRequestError struct{}

func (e BadJSONRequestError) Error() string {
	return "json from request is bad"
}

type ServerError struct{}

func (e ServerError) Error() string {
	return "server error"
}

const (
	badJSON           string = "bad json"
	invalidOrderField        = "order field invalid"
	serverErr                = "server error"
	longWork                 = "long work"
	correctToken             = "correctToken"
	badToken                 = "badToken"
)

func parseOrderField(orderField string) error {
	switch strings.ToLower(orderField) {
	case "id", "name", "age":
	case "":
		orderField = "name"
	case invalidOrderField:
		return BadOrderFieldError{}
	case badJSON:
		return BadJSONRequestError{}
	default:
		return UnknownBadOrderFieldError{}
	}
	return nil
}

func parseQuery(query string) error {
	switch query {
	case badJSON:
		return BadJSONError{}
	case serverErr:
		return ServerError{}
	case longWork:
		time.Sleep(time.Second)
	}
	return nil
}

func parseLimit(limit string) (int, error) {
	return strconv.Atoi(limit)
}

func parseOrderBy(order string) (int, error) {
	return strconv.Atoi(order)
}

func parseRequest(r *http.Request) (*message, error) {
	var err error
	order := r.FormValue("order_field")
	if err = parseOrderField(order); err != nil {
		return nil, err
	}
	query := r.FormValue("query")
	if err = parseQuery(query); err != nil {
		return nil, err
	}
	limitStr := r.FormValue("limit")
	limit, err := parseLimit(limitStr)
	if err != nil {
		return nil, err
	}
	orderByStr := r.FormValue("order_by")
	orderBy, err := parseOrderBy(orderByStr)
	if err != nil {
		return nil, err
	}
	result := message{order, query, limit, orderBy}

	return &result, nil
}

type byId []UserFromDS
type byName []UserFromDS
type byAge []UserFromDS

func (t byId) Len() int           { return len(t) }
func (t byId) Less(i, j int) bool { return (t[i]).Id < (t[j]).Id }
func (t byId) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

func (t byName) Len() int           { return len(t) }
func (t byName) Less(i, j int) bool { return (t[i]).Name < (t[j]).Name }
func (t byName) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

func (t byAge) Len() int           { return len(t) }
func (t byAge) Less(i, j int) bool { return (t[i]).Age < (t[j]).Age }
func (t byAge) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

func sortResult(orderBy int, orderField string, data []UserFromDS) {
	var order func(s sort.Interface) sort.Interface
	switch orderBy {
	case -1:
		order = sort.Reverse
	case 1:
		order = func(s sort.Interface) sort.Interface { return s }
	case 0:
		return
	}
	switch orderField {
	case "id":
		sort.Sort(order(byId(data)))
	case "name":
		sort.Sort(order(byName(data)))
	case "age":
		sort.Sort(order(byAge(data)))
	}
}

func searchBy(query string, path string) ([]UserFromDS, error) {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	users := dataset{}

	err = xml.Unmarshal(file, &users)
	if err != nil {
		return nil, err
	}
	var result []UserFromDS
	if query == "" {
		return users.Data, nil
	}
	for i, user := range users.Data {
		users.Data[i].Name = users.Data[i].FName + " " + users.Data[i].LName
		if strings.Contains(users.Data[i].Name, query) ||
			strings.Contains(users.Data[i].About, query) {
			result = append(result, user)
		}
	}
	return result, nil
}

func limitResult(limit int, u []UserFromDS) []UserFromDS {
	if limit >= len(u) {
		return u
	}
	return u[:limit]
}

func isAuthorized(r *http.Request) bool {
	token := r.Header.Get("AccessToken")
	if token == "correctToken" {
		return true
	}
	return false
}

func (ss *SearchServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !isAuthorized(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	msg, err := parseRequest(r)
	if err != nil {
		switch err.(type) {
		case ServerError:
			w.WriteHeader(http.StatusInternalServerError)
		case BadOrderFieldError, UnknownBadOrderFieldError:
			w.WriteHeader(http.StatusBadRequest)
			s := SearchErrorResponse{err.Error()}
			resp, _ := json.Marshal(s)
			w.Write(resp)
		case BadJSONRequestError:
			w.WriteHeader(http.StatusBadRequest)
		case BadJSONError:
		}
		return
	}
	result, _ := searchBy(msg.query, ss.path)
	sortResult(msg.orderBy, msg.orderField, result)
	result = limitResult(msg.limit, result)
	b, _ := json.Marshal(result)
	w.Write(b)
}

func setup() SearchClient {
	ss := SearchServer{"dataset.xml"}
	srv := httptest.NewServer(&ss)
	return SearchClient{
		AccessToken: correctToken, URL: srv.URL,
	}
}

func TestBaseOk(t *testing.T) {
	cl := setup()
	req := SearchRequest{26, 1, "W", "name", 1}
	result, err := cl.FindUsers(req)
	if len(result.Users) != 4 {
		t.Errorf("expected 4, got %d", len(result.Users))
	}
	if err != nil {
		t.Error(err)
	}
}

func TestLimitOk(t *testing.T) {
	cl := setup()
	req := SearchRequest{3, 1, "W", "name", 1}
	res, err := cl.FindUsers(req)
	if len(res.Users) != 3 {
		t.Errorf("wrong len of users, must be 3, have %d", len(res.Users))
	}
	if err != nil {
		t.Error(err)
	}
}

func TestLimitNeg(t *testing.T) {
	cl := setup()
	req := SearchRequest{-1, 1, "W", "name", 1}
	_, err := cl.FindUsers(req)
	errResult := "limit must be > 0"
	if err.Error() != errResult {
		t.Errorf("expected %s, got %v", errResult, err)
	}
}

func TestOfsetNeg(t *testing.T) {
	cl := setup()
	req := SearchRequest{10, -1, "W", "name", 1}
	_, err := cl.FindUsers(req)
	errResult := "offset must be > 0"
	if err.Error() != errResult {
		t.Errorf("expected %s, got %v", errResult, err)
	}
}

func TestTokenBad(t *testing.T) {
	cl := setup()
	cl.AccessToken = badToken
	req := SearchRequest{26, 1, "W", "name", 1}
	_, err := cl.FindUsers(req)
	errResult := "Bad AccessToken"
	if err.Error() != errResult {
		t.Errorf("expected %s, got %v", errResult, err)
	}
}

func TestOrderFieldBad(t *testing.T) {
	cl := setup()
	req := SearchRequest{26, 1, "W", invalidOrderField, 1}
	_, err := cl.FindUsers(req)
	errResult := "OrderFeld"
	if !strings.Contains(err.Error(), errResult) {
		t.Errorf("expected %s, got %v", errResult, err)
	}
}

func TestUnknownOrderField(t *testing.T) {
	cl := setup()
	req := SearchRequest{26, 1, "W", "something bad", 1}
	_, err := cl.FindUsers(req)
	errResult := "unknown bad request error"
	if !strings.Contains(err.Error(), errResult) {
		t.Errorf("expected %s, got %v", errResult, err)
	}
}

func TestBadJsonRequest(t *testing.T) {
	cl := setup()
	req := SearchRequest{26, 1, "W", badJSON, 1}
	_, err := cl.FindUsers(req)
	errResult := "cant unpack error json"
	if !strings.Contains(err.Error(), errResult) {
		t.Errorf("expected %s, got %v", errResult, err)
	}
}

func TestBadJsonResult(t *testing.T) {
	cl := setup()
	req := SearchRequest{5, 1, badJSON, "age", 1}
	_, err := cl.FindUsers(req)
	errResult := "cant unpack result json"
	if !strings.Contains(err.Error(), errResult) {
		t.Errorf("expected %s, got %v", errResult, err)
	}
}

func TestServerFatalError(t *testing.T) {
	cl := setup()
	req := SearchRequest{5, 1, serverErr, "age", 1}
	_, err := cl.FindUsers(req)
	errResult := "SearchServer fatal error"
	if err.Error() != errResult {
		t.Errorf("expected %s, got %v", errResult, err)
	}
}

func TestServerUnknownError(t *testing.T) {
	cl := setup()
	cl.URL = "smth"
	req := SearchRequest{5, 1, serverErr, "age", 1}
	_, err := cl.FindUsers(req)
	errResult := "unknown error"
	if !strings.Contains(err.Error(), errResult) {
		t.Errorf("expected %s, got %v", errResult, err)
	}
}

func TestServerSlow(t *testing.T) {
	cl := setup()
	req := SearchRequest{5, 1, longWork, "age", 1}
	_, err := cl.FindUsers(req)
	errResult := "timeout for"
	if !strings.Contains(err.Error(), errResult) {
		t.Errorf("expected %s, got %v", errResult, err)
	}
}
