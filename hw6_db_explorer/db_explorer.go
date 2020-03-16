package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

const (
	defaultLimit  int = 5
	defaultOffset int = 0
)

const (
	kindString kind = iota
	kindNullString
	kindInt64
	kindNullInt64
	kindFloat64
	kindNullFloat64
)

type kind int
type errInvalidType string
type wrapper func(h http.HandlerFunc) http.HandlerFunc
type segmentsMap string
type rowKey string

type route struct {
	re       *regexp.Regexp
	handler  http.Handler
	_methods []string
}

type httpRouter struct {
	routes []*route
}

type env struct {
	db   *sql.DB
	meta *dbMeta
}

type dbMeta struct {
	keys []string
	data map[string]tableSpec
}

type tableSpec struct {
	name string
	pk   *colSpec
	cols []*colSpec
}

type colSpec struct {
	name     string
	typ      kind
	nullable bool
}

type nullString struct {
	sql.NullString
}

type nullInt64 struct {
	sql.NullInt64
}

type nullFloat64 struct {
	sql.NullFloat64
}

func (e errInvalidType) Error() string {
	return string(e)
}

func (m *dbMeta) get(tableName string) tableSpec {
	val, ok := m.data[tableName]
	if !ok {
		panic("missing key: " + tableName)
	}
	return val
}

func (m *dbMeta) set(tableName string, spec tableSpec) {
	_, ok := m.data[tableName]
	if ok {
		panic("key already exists: " + tableName)
	}
	m.keys = append(m.keys, tableName)
	m.data[tableName] = spec
}

func makeSelectFromHandler(env *env) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tableName := getSegmentValue(r.Context(), "table")
		limitRaw := r.URL.Query().Get("limit")
		offsetRaw := r.URL.Query().Get("offset")
		limit, offset := parseLimitOffset(limitRaw, offsetRaw)
		q := fmt.Sprintf("SELECT * FROM %s LIMIT %d, %d", tableName, offset, limit)
		rows, err := env.db.Query(q)
		if err != nil {
			panic(err.Error())
		}
		defer func() {
			err := rows.Close()
			if err != nil {
				panic(err.Error())
			}
		}()

		tableSpec := env.meta.get(tableName)
		rowType := makeRowTypeFromSpec(tableSpec)
		var result []interface{}
		for rows.Next() {
			row, vals := newRowWithVals(rowType)
			err = rows.Scan(vals...)
			if err != nil {
				panic(err.Error())
			}
			result = append(result, row)
		}
		err = rows.Err()
		if err != nil {
			panic(err.Error())
		}

		response := map[string]interface{}{
			"response": map[string]interface{}{
				"records": result,
			},
		}

		err = writeResponse(w, response)
		if err != nil {
			panic(err.Error())
		}
	}
}

func makeSelectFromWhereHandler(env *env) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tableName := getSegmentValue(r.Context(), "table")
		idRaw := getSegmentValue(r.Context(), "id")
		id, err := strconv.Atoi(idRaw)
		if err != nil {
			panic(err.Error())
		}
		tableSpec := env.meta.get(tableName)
		q := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", tableSpec.name, tableSpec.pk.name)
		row := env.db.QueryRow(q, id)
		rowType := makeRowTypeFromSpec(tableSpec)
		result, vals := newRowWithVals(rowType)
		err = row.Scan(vals...)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			_, err := w.Write([]byte(`{"error": "record not found"}`))
			if err != nil {
				panic(err.Error())
			}
			return
		}
		response := map[string]interface{}{
			"response": map[string]interface{}{
				"record": result,
			},
		}
		err = writeResponse(w, response)
		if err != nil {
			panic(err.Error())
		}
	}
}

func (t tableSpec) getColNames() []string {
	var names []string
	for _, col := range t.cols {
		names = append(names, col.name)
	}
	return names
}

func prepareInsertQuery(t tableSpec, values map[string]interface{}) (string, []interface{}) {
	q := "INSERT INTO %s (%s) VALUES (%s)"
	var colNames []string
	var colVals []interface{}
	for colName, value := range values {
		colNames = append(colNames, colName)
		colVals = append(colVals, value)
	}
	names := strings.Join(colNames, ", ")
	placeHolders := "?" + strings.Repeat(",?", len(colVals)-1)
	return fmt.Sprintf(q, t.name, names, placeHolders), colVals
}

func prepareUpdateQuery(t tableSpec, values map[string]interface{}, pkVal int) (string, []interface{}) {
	q := "UPDATE %s SET %s WHERE %s = ?"
	var colNames []string
	var colVals []interface{}
	for colName, value := range values {
		colNames = append(colNames, colName+" = ?")
		colVals = append(colVals, value)
	}
	colVals = append(colVals, pkVal)
	colPlaceholders := strings.Join(colNames, ", ")
	return fmt.Sprintf(q, t.name, colPlaceholders, t.pk.name), colVals
}

func makeInsertHandler(env *env) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tableName := getSegmentValue(r.Context(), "table")
		tableSpec := env.meta.get(tableName)
		pm := r.Context().Value(rowKey(""))
		if pm == nil {
			panic("query parameters expected")
		}
		parsedParams, ok := pm.(map[string]interface{})
		if !ok {
			panic("type missmatch")
		}
		query, values := prepareInsertQuery(tableSpec, parsedParams)
		result, err := env.db.Exec(query, values...)
		if err != nil {
			panic(err.Error())
		}
		id, err := result.LastInsertId()
		if err != nil {
			panic(err.Error())
		}
		response := map[string]interface{}{
			"response": map[string]interface{}{
				tableSpec.pk.name: id,
			},
		}
		err = writeResponse(w, response)
		if err != nil {
			panic(err.Error())
		}
	}
}

func makeUpdateHandler(env *env) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tableName := getSegmentValue(r.Context(), "table")
		idRaw := getSegmentValue(r.Context(), "id")
		id, err := strconv.Atoi(idRaw)
		if err != nil {
			panic(err.Error())
		}
		tableSpec := env.meta.get(tableName)
		pm := r.Context().Value(rowKey(""))
		if pm == nil {
			panic("query parameters expected")
		}
		parsedParams, ok := pm.(map[string]interface{})
		if !ok {
			panic("type missmatch")
		}
		query, values := prepareUpdateQuery(tableSpec, parsedParams, id)
		result, err := env.db.Exec(query, values...)
		if err != nil {
			panic(err.Error())
		}
		affected, err := result.RowsAffected()
		if err != nil {
			panic(err.Error())
		}
		response := map[string]interface{}{
			"response": map[string]interface{}{
				"updated": affected,
			},
		}
		err = writeResponse(w, response)
		if err != nil {
			panic(err.Error())
		}
	}
}

func makeDeleteHandler(env *env) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tableName := getSegmentValue(r.Context(), "table")
		idRaw := getSegmentValue(r.Context(), "id")
		id, err := strconv.Atoi(idRaw)
		if err != nil {
			panic(err.Error())
		}
		tableSpec := env.meta.get(tableName)
		query := fmt.Sprintf(`DELETE FROM %s WHERE %s = ?`, tableName, tableSpec.pk.name)
		result, err := env.db.Exec(query, id)
		if err != nil {
			panic(err.Error())
		}
		affected, err := result.RowsAffected()
		if err != nil {
			panic(err.Error())
		}
		response := map[string]interface{}{
			"response": map[string]interface{}{
				"deleted": affected,
			},
		}
		err = writeResponse(w, response)
		if err != nil {
			panic(err.Error())
		}
	}
}

func getTypeOf(c *colSpec) reflect.Type {
	switch c.typ {
	case kindString:
		fallthrough
	case kindNullString:
		return reflect.TypeOf(nullString{})
	case kindInt64:
		fallthrough
	case kindNullInt64:
		return reflect.TypeOf(nullInt64{})
	case kindFloat64:
		fallthrough
	case kindNullFloat64:
		return reflect.TypeOf(nullFloat64{})
	default:
		panic("unknown type")
	}
}

func makeShowTablesHandler(meta *dbMeta) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := make(map[string]interface{})
		response["response"] = map[string]interface{}{"tables": meta.keys}
		buf, err := json.Marshal(response)
		if err != nil {
			panic(err.Error())
		}
		_, err = w.Write(buf)
		if err != nil {
			panic(err.Error())
		}
	}
}

func makeRowTypeFromSpec(ts tableSpec) reflect.Type {
	var fields []reflect.StructField
	for _, col := range ts.cols {
		field := reflect.StructField{
			Name: strings.Title(col.name),
			Type: getTypeOf(col),
			Tag:  reflect.StructTag(`json:"` + col.name + `"`),
		}
		fields = append(fields, field)
	}
	return reflect.StructOf(fields)
}

func validateJSON(t tableSpec, jsonRaw map[string]json.RawMessage, update bool) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	wasPK := false
	for _, col := range t.cols {
		rawField, ok := jsonRaw[col.name]
		colType := getTypeOf(col)
		valPtr := reflect.New(colType).Interface()
		if !ok {
			// default values for non-nullable fields (insert)
			if !col.nullable && col != t.pk && !update {
				reflect.ValueOf(valPtr).Elem().FieldByName("Valid").SetBool(true)
				result[col.name] = valPtr
			}
			continue
		}
		if col == t.pk {
			wasPK = true
			continue
		}
		err := json.Unmarshal([]byte(rawField), valPtr)
		if err != nil {
			return nil, errInvalidType("field " + col.name + " have invalid type")
		}
		if !col.nullable && !reflect.ValueOf(valPtr).Elem().FieldByName("Valid").Bool() {
			return nil, errInvalidType("field " + col.name + " have invalid type")
		}
		result[col.name] = valPtr
	}
	if wasPK && len(result) == 0 {
		return nil, errInvalidType("field " + t.pk.name + " have invalid type")
	}
	return result, nil
}

func getFieldPtr(row reflect.Value, idx int) interface{} {
	return row.Elem().Field(idx).Addr().Interface()
}

func newRowWithVals(typ reflect.Type) (row interface{}, vals []interface{}) {
	rowPtr := reflect.New(typ)
	vals = make([]interface{}, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		vals[i] = getFieldPtr(rowPtr, i)
	}
	return rowPtr.Interface(), vals
}

func writeResponse(w http.ResponseWriter, response map[string]interface{}) error {
	buf, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if _, err := w.Write(buf); err != nil {
		return err
	}
	return nil
}

func (h *httpRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var matchedRoute *route
	var matchedGroups []string
	for _, route := range h.routes {
		for _, method := range route._methods {
			if r.Method != method {
				continue
			}
			matches := route.re.FindStringSubmatch(r.URL.Path)
			if len(matches) == 0 {
				continue
			}
			matchedRoute = route
			matchedGroups = matches
		}
	}
	// no one route finded
	if matchedRoute == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	sm := make(map[string]string)
	for i, groupName := range matchedRoute.re.SubexpNames() {
		// first element contains fully matched text, just skip
		if i == 0 {
			continue
		}
		sm[groupName] = matchedGroups[i]
	}
	ctx := context.WithValue(r.Context(), segmentsMap("urlSegments"), sm)
	matchedRoute.handler.ServeHTTP(w, r.WithContext(ctx))
}

func getSegmentsMap(c context.Context) map[string]string {
	valueRaw := c.Value(segmentsMap("urlSegments"))
	value, ok := valueRaw.(map[string]string)
	if !ok {
		panic("missing url segments map")
	}
	return value
}

func getSegmentValue(c context.Context, segmentName string) string {
	m := getSegmentsMap(c)
	v, ok := m[segmentName]
	if !ok {
		panic("missing segment value: " + segmentName)
	}
	return v
}

func makeTableValidator(meta *dbMeta, segmentName string) (wrapper, error) {
	validator := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			tableSegment := getSegmentValue(r.Context(), segmentName)
			_, ok := meta.data[tableSegment]
			// call next handler in the chain
			if ok {
				h(w, r)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			_, err := w.Write([]byte(`{"error": "unknown table"}`))
			if err != nil {
				panic(err.Error())
			}
		}
	}
	return validator, nil
}

func getJSONRaw(body []byte) (map[string]json.RawMessage, error) {
	data := make(map[string]json.RawMessage)
	err := json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, errors.New("empty request body")
	}
	return data, nil
}

func makeJSONValidator(meta *dbMeta, segmentName string) wrapper {
	wrapper := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			tableName := getSegmentValue(r.Context(), segmentName)
			tableSpec := meta.get(tableName)
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err.Error())
			}
			jsonRaw, err := getJSONRaw(body)
			if err != nil {
				panic(err.Error())
			}
			if len(jsonRaw) == 0 {
				panic("there is no params in the request")
			}
			queryParams, err := validateJSON(tableSpec, jsonRaw, r.Method == http.MethodPost)
			if err != nil {
				switch err.(type) {
				case errInvalidType:
					w.WriteHeader(http.StatusBadRequest)
					_, err := w.Write([]byte(`{"error": "` + err.Error() + `"}`))
					if err != nil {
						panic(err.Error())
					}
					return
				default:
					panic(err.Error())
				}
			}

			// call next handler in the chain
			h(w, r.WithContext(context.WithValue(r.Context(), rowKey(""), queryParams)))
			return
		}
	}
	return wrapper
}

func (r *route) methods(methods ...string) {
	r._methods = methods
}

func parsePattern(pattern string) (*regexp.Regexp, error) {
	splits := strings.Split(pattern, "/")
	for i := range splits {
		if !strings.HasPrefix(splits[i], "{") {
			splits[i] = regexp.QuoteMeta(splits[i])
			continue
		}
		// trim prefix `{` and suffix `}`
		regexpRaw := splits[i][1 : len(splits[i])-1]
		var groupName, suffix string
		end := strings.Index(regexpRaw, ":")
		if end == -1 {
			groupName = regexpRaw
			suffix = `>[^/]+)`
		} else {
			groupName = regexpRaw[:end]
			suffix = `>` + regexpRaw[end+1:] + `)`
		}
		validName, _ := regexp.MatchString("[a-zA-Z]+", groupName)
		if !validName {
			return nil, fmt.Errorf("group name should contains only [a-zA-Z] characters: %s", groupName)
		}
		splits[i] = "(?P<" + groupName + suffix
	}
	return regexp.Compile(strings.Join(splits, `\/`))
}

func (r *httpRouter) HandleFunc(pattern string, f func(http.ResponseWriter, *http.Request)) *route {
	handler := http.HandlerFunc(f)
	re, err := parsePattern(pattern)
	if err != nil {
		panic("pattern parsing error: " + err.Error())
	}
	route := route{re, handler, nil}
	r.routes = append(r.routes, &route)

	return &route
}

func (n *nullString) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.String)
}

func (n *nullInt64) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.Int64)
}

func (n *nullFloat64) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.Float64)
}

func (n *nullString) UnmarshalJSON(b []byte) error {
	v := new(string)
	err := json.Unmarshal(b, &v)
	n.Valid = (err == nil && v != nil)
	if v != nil {
		n.String = *v
	}
	return err
}

func (n *nullInt64) UnmarshalJSON(b []byte) error {
	v := new(int64)
	err := json.Unmarshal(b, &v)
	n.Valid = (err == nil && v != nil)
	if v != nil {
		n.Int64 = *v
	}
	return err
}

func (n *nullFloat64) UnmarshalJSON(b []byte) error {
	v := new(float64)
	err := json.Unmarshal(b, &v)
	n.Valid = (err == nil && v != nil)
	if v != nil {
		n.Float64 = *v
	}
	return err
}

func newTableSpec(name string, pk *colSpec, cols []*colSpec) tableSpec {
	return tableSpec{
		name,
		pk,
		cols,
	}
}

func getAllTableSpecs(db *sql.DB) ([]tableSpec, error) {
	var tables []tableSpec
	tableNames, err := getTableNames(db)
	if err != nil {
		return nil, err
	}
	for _, name := range tableNames {
		table, err := getTableSpec(db, name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func getTableSpec(db *sql.DB, tableName string) (tableSpec, error) {
	table := newTableSpec(tableName, nil, nil)
	q := `SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, IS_NULLABLE 
FROM information_schema.columns WHERE TABLE_SCHEMA = database() AND TABLE_NAME = ?`
	rows, err := db.Query(q, tableName)
	if err != nil {
		return table, err
	}
	defer rows.Close()
	var colName, typeName, key, nullable string
	for rows.Next() {
		err = rows.Scan(&colName, &typeName, &key, &nullable)
		if err != nil {
			return table, err
		}
		col := newColSpec(colName, typeName, nullable)
		table.cols = append(table.cols, col)
		if key == "PRI" {
			if table.pk != nil {
				panic("only one PK expected")
			}
			table.pk = col
		}
	}
	err = rows.Err()
	if err != nil {
		return table, err
	}
	return table, nil
}

func getTableNames(db *sql.DB) ([]string, error) {
	var tableName string
	var result []string
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}
		result = append(result, tableName)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func newColSpec(colName, typeName, null string) *colSpec {
	var typeKind kind
	var nullable = null == "YES"
	switch {
	case strings.HasPrefix(typeName, "text"):
		fallthrough
	case strings.HasPrefix(typeName, "char"):
		fallthrough
	case strings.HasPrefix(typeName, "varchar"):
		if nullable {
			typeKind = kindNullString
			break
		}
		typeKind = kindString
	case strings.HasPrefix(typeName, "int"):
		fallthrough
	case strings.HasPrefix(typeName, "bigint"):
		if nullable {
			typeKind = kindNullInt64
			break
		}
		typeKind = kindInt64
	case strings.HasPrefix(typeName, "float"):
		if nullable {
			typeKind = kindNullFloat64
			break
		}
		typeKind = kindFloat64
	default:
		panic("unknown type: " + typeName)
	}

	return &colSpec{colName, typeKind, nullable}
}

func newDBMeta() *dbMeta {
	meta := dbMeta{}
	meta.data = make(map[string]tableSpec)
	return &meta
}

func getDBMeta(db *sql.DB) (*dbMeta, error) {
	meta := newDBMeta()
	specs, err := getAllTableSpecs(db)
	if err != nil {
		return meta, err
	}
	for _, spec := range specs {
		meta.set(spec.name, spec)
	}
	return meta, nil
}

func parseLimitOffset(limitRaw, offsetRaw string) (limit, offset int) {
	var err error
	limit, err = strconv.Atoi(limitRaw)
	if limitRaw == "" || err != nil {
		limit = defaultLimit
	}
	offset, err = strconv.Atoi(offsetRaw)
	if offsetRaw == "" || err != nil {
		offset = defaultOffset
	}
	return
}

// NewDbExplorer ...
func NewDbExplorer(db *sql.DB) (http.Handler, error) {
	dbMeta, err := getDBMeta(db)
	if err != nil {
		panic(err.Error())
	}
	env := env{db: db, meta: dbMeta}

	router := httpRouter{}
	checkTable, err := makeTableValidator(dbMeta, "table")
	if err != nil {
		panic(err.Error())
	}
	parseJSON := makeJSONValidator(dbMeta, "table")

	showTables := makeShowTablesHandler(dbMeta)
	selectFrom := makeSelectFromHandler(&env)
	selectFromWhere := makeSelectFromWhereHandler(&env)
	insertInto := makeInsertHandler(&env)
	updateWhere := makeUpdateHandler(&env)
	deleteFrom := makeDeleteHandler(&env)

	router.HandleFunc("/", showTables).methods("GET")
	router.HandleFunc("/{table}", checkTable(selectFrom)).methods("GET")
	router.HandleFunc("/{table}/{id:[0-9]+}", checkTable(selectFromWhere)).methods("GET")

	router.HandleFunc("/{table}", checkTable(parseJSON(insertInto))).methods("PUT")
	router.HandleFunc("/{table}/{id:[0-9]+}", checkTable(parseJSON(updateWhere))).methods("POST")

	router.HandleFunc("/{table}/{id:[0-9]+}", checkTable(deleteFrom)).methods("DELETE")
	return &router, nil
}
