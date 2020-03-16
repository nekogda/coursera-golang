package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strconv"
	"strings"
	"text/template"
)

type tmplData struct {
	PackageName string
	Methods     []*ast.FuncDecl
	MethodsCfg  map[string]*methodConfig
	StructsCfg  map[string]map[string]*fieldConfig
}

type methodConfig struct {
	URL        string `json:"url"`
	Auth       bool   `json:"auth"`
	HTTPMethod string `json:"method"`
}

type fieldConfig struct {
	Required bool
	HasMin   bool
	HasMax   bool
	Min      int
	Max      int
	Enum     []string
	Alias    string
	Default  string
}

type mWalker struct {
	methods []*ast.FuncDecl
}

func getPackageName(file *ast.File) string {
	return file.Name.Name
}

func GetMethodName(method *ast.FuncDecl) string {
	return method.Name.Name
}

func GetMethodRecvName(method *ast.FuncDecl) string {
	if len(method.Recv.List[0].Names) > 0 {
		return method.Recv.List[0].Names[0].Name
	}
	return ""
}

func GetMethodRecvTypeName(method *ast.FuncDecl) string {
	return getTypeNameFromExpr(method.Recv.List[0].Type)
}

func getTypeNameFromExpr(expr ast.Expr) string {
	var name string
	switch node := expr.(type) {
	case *ast.Ident:
		name = node.Name
	case *ast.StarExpr:
		name = getTypeNameFromExpr(node.X)
	case *ast.SelectorExpr:
		name = selectorExprToStr(node)
	default:
		panic("unknown type")
	}
	return name
}

func GetStructTypes(methods []*ast.FuncDecl) map[string]*ast.StructType {
	structs := make(map[string]*ast.StructType)
	for _, method := range methods {
		expr := getMethodParamTypeExpr(method, 1)
		structName := GetMethodParamTypeName(method, 1)
		structs[structName] = getStructTypeFromExpr(expr)
	}
	return structs
}

func GetRecvTypes(methods []*ast.FuncDecl) map[string][]*ast.FuncDecl {
	result := make(map[string][]*ast.FuncDecl)
	for _, method := range methods {
		recvName := GetMethodRecvTypeName(method)
		result[recvName] = append(result[recvName], method)
	}
	return result
}

func GetStructFields(s *ast.StructType) map[string]*ast.Field {
	if len(s.Fields.List) != s.Fields.NumFields() {
		// Ignore corner case like this. For simplicity reasons.
		// type s struct { a, b, c int }
		panic("should be equal")
	}
	result := make(map[string]*ast.Field)
	for _, field := range s.Fields.List {
		name := field.Names[0].Name
		result[name] = field
	}
	return result
}

func GetFieldTypeName(field *ast.Field) string {
	return getTypeNameFromExpr(field.Type)
}

func getStructTypeFromExpr(expr ast.Expr) *ast.StructType {
	var st *ast.StructType
	switch node := expr.(type) {
	case *ast.Ident:
		v, ok := node.Obj.Decl.(*ast.TypeSpec)
		if !ok {
			panic("should be ok")
		}
		st, ok = v.Type.(*ast.StructType)
		if !ok {
			panic("should be ok")
		}
	case *ast.StarExpr:
		st = getStructTypeFromExpr(node.X)
	default:
		panic("unknown type, expected only ast.Ident or ast.StarExpr")
	}
	return st
}

func checkMethodParamIdx(method *ast.FuncDecl, idx int) {
	if idx >= method.Type.Params.NumFields() {
		panic("index is greater then size of parameters list")
	}
	if method.Type.Params.NumFields() != len(method.Type.Params.List) {
		// Ignore corner case like this. For simplicity reasons.
		// something like that: foo(a,b,c int)
		panic("non uniform parameter list")
	}
}

func GetMethodParamTypeName(method *ast.FuncDecl, idx int) string {
	checkMethodParamIdx(method, idx)
	return getTypeNameFromExpr(method.Type.Params.List[idx].Type)
}

func (t *tmplData) GetMethodConfig(methodName string) *methodConfig {
	cfg, ok := t.MethodsCfg[methodName]
	if !ok {
		panic("no such method, but should: " + methodName)
	}
	return cfg
}

func (t *tmplData) GetFieldConfig(structName, fieldName string) *fieldConfig {
	fields, ok := t.StructsCfg[structName]
	if !ok {
		panic("no such struct, but should: " + structName)
	}
	for field, cfg := range fields {
		if field == fieldName {
			return cfg
		}
	}
	panic("can't find field with name: " + fieldName)
}

func selectorExprToStr(se *ast.SelectorExpr) string {
	ident := se.X.(*ast.Ident)
	return ident.Name + "." + se.Sel.Name
}

func parseMethodConfig(method *ast.FuncDecl) (*methodConfig, error) {
	configRaw := strings.TrimPrefix(method.Doc.Text(), "apigen:api")
	config := methodConfig{}
	err := json.Unmarshal([]byte(configRaw), &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func newTmplDataFrom(methods []*ast.FuncDecl, pkgName string) (*tmplData, error) {
	methodConfigs := make(map[string]*methodConfig)
	for _, method := range methods {
		cfg, err := parseMethodConfig(method)
		if err != nil {
			return nil, err
		}
		methodConfigs[GetMethodName(method)] = cfg
	}
	fieldConfigs := make(map[string]map[string]*fieldConfig)
	for _, method := range methods {
		// skip first parameter (ctx)
		expr := getMethodParamTypeExpr(method, 1)
		paramStruct := getStructTypeFromExpr(expr)
		paramTypeName := GetMethodParamTypeName(method, 1)
		_, ok := fieldConfigs[paramTypeName]
		if ok {
			continue
		}
		fieldConfigs[paramTypeName] = make(map[string]*fieldConfig)
		for _, field := range paramStruct.Fields.List {
			cfg, err := parseFieldConfig(field)
			if err != nil {
				return nil, err
			}
			if cfg == nil {
				continue
			}
			fieldConfigs[paramTypeName][field.Names[0].Name] = cfg
		}
	}
	return &tmplData{pkgName, methods, methodConfigs, fieldConfigs}, nil
}

func parseFieldConfig(field *ast.Field) (*fieldConfig, error) {
	if field.Tag == nil || !strings.HasPrefix(field.Tag.Value, "`apivalidator:") {
		return nil, nil

	}
	tag := field.Tag.Value
	r, _ := regexp.Compile(`apivalidator:"(([^\\]*?)|(.*?[^\\]))"`)
	submatch := r.FindStringSubmatch(tag)
	if len(submatch) == 0 {
		return nil, fmt.Errorf("Non valid tag: %s", tag)
	}
	cfg := fieldConfig{}
	for _, token := range strings.Split(submatch[1], ",") {
		switch {
		case strings.HasPrefix(token, "required"):
			cfg.Required = true
		case strings.HasPrefix(token, "paramname"):
			cfg.Alias = strings.Split(token, "=")[1]
		case strings.HasPrefix(token, "enum"):
			vals := strings.Split(token, "=")[1]
			for _, v := range strings.Split(vals, "|") {
				cfg.Enum = append(cfg.Enum, v)
			}
		case strings.HasPrefix(token, "min"):
			cfg.HasMin = true
			min, err := strconv.Atoi(strings.Split(token, "=")[1])
			if err != nil {
				return nil, err
			}
			cfg.Min = min
		case strings.HasPrefix(token, "max"):
			cfg.HasMax = true
			max, err := strconv.Atoi(strings.Split(token, "=")[1])
			if err != nil {
				return nil, err
			}
			cfg.Max = max
		case strings.HasPrefix(token, "default"):
			cfg.Default = strings.Split(token, "=")[1]
		default:
			panic(fmt.Sprintf("unknown token: %s", token))
		}
	}
	if len(cfg.Alias) == 0 {
		cfg.Alias = strings.ToLower(field.Names[0].Name)
	}
	return &cfg, nil
}

func getMethodParamTypeExpr(method *ast.FuncDecl, idx int) ast.Expr {
	checkMethodParamIdx(method, idx)
	return method.Type.Params.List[idx].Type
}

func (mw *mWalker) Visit(n ast.Node) ast.Visitor {
	if n == nil {
		return nil
	}

	f, ok := n.(*ast.FuncDecl)
	if !ok || f.Recv == nil {
		// skip functions without recievers
		return mw
	}
	if !strings.HasPrefix(f.Doc.Text(), "apigen:api") {
		// skip methods without apigen comment
		return mw
	}

	mw.methods = append(mw.methods, f)
	return mw
}

func parseArgs(args []string) (src, dst string, err error) {
	if len(args) < 3 {
		err = fmt.Errorf("not enouth arguments")
		return
	}
	src = args[1]
	dst = args[2]
	return
}

func parseSrc(src string) (data *tmplData, err error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, src, nil, parser.ParseComments)
	if err != nil {
		return
	}
	mw := mWalker{}
	ast.Walk(&mw, node)
	tmplData, err := newTmplDataFrom(mw.methods, getPackageName(node))
	if err != nil {
		return nil, err
	}
	return tmplData, nil
}

func generateCode(buf bytes.Buffer, data *tmplData) (bytes.Buffer, error) {
	funcMap := make(template.FuncMap)
	funcMap["GetStructTypes"] = GetStructTypes
	funcMap["GetStructFields"] = GetStructFields
	funcMap["GetFieldTypeName"] = GetFieldTypeName
	funcMap["GetRecvTypes"] = GetRecvTypes
	funcMap["GetMethodName"] = GetMethodName
	funcMap["GetMethodParamTypeName"] = GetMethodParamTypeName
	funcMap["GetMethodRecvName"] = GetMethodRecvName

	tmpl := template.New("handlers").Funcs(funcMap)
	tmpl, err := tmpl.Parse(tmplHandlers)
	if err != nil {
		return buf, err
	}
	err = tmpl.Execute(&buf, data)
	if err != nil {
		return buf, err
	}
	return buf, nil
}

func formatCode(buf bytes.Buffer) (bytes.Buffer, error) {
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return buf, err
	}
	return *bytes.NewBuffer(formatted), nil
}

func writeToFile(dst string, buf bytes.Buffer) error {
	fd, err := os.Create(dst)
	if err != nil {
		return err
	}
	_, err = fd.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func checkErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		panic(nil)
	}
}

func run() {
	// parse args
	src, dst, err := parseArgs(os.Args)
	checkErr(err)
	// parse source code
	data, err := parseSrc(src)
	checkErr(err)
	// prepare and execute template
	buf := bytes.Buffer{}
	buf, err = generateCode(buf, data)
	checkErr(err)
	// format output from template
	buf, err = formatCode(buf)
	checkErr(err)
	// write generated code
	err = writeToFile(dst, buf)
	checkErr(err)
}

func main() {
	run()
}

var tmplHandlers = `
package {{.PackageName}}

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"encoding/json"
)

type APIResponse struct {
	Error string ` + "`json:\"error\"`" + `
	Response interface{} ` + "`json:\"response,omitempty\"`" + `
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

{{range $structName, $struct := GetStructTypes .Methods}}
func validate{{$structName}}(p *{{$structName}}, r *http.Request) error {
	{{range $fieldName, $field := GetStructFields $struct -}}
	if err := validate{{$structName}}{{$fieldName}}(p, r); err != nil {
		return err
	}
	{{end -}}
	return nil
}
{{end}}

{{range $structName, $struct := GetStructTypes .Methods}}
{{range $fieldName, $field := GetStructFields $struct}}
func validate{{$structName}}{{$fieldName}}(p *{{$structName}}, r *http.Request) (err error) {
	{{$fieldCfg := $.GetFieldConfig $structName $fieldName -}}
	valueRaw := r.FormValue("{{$fieldCfg.Alias}}")
	// default case
	if len(valueRaw) == 0 {
		valueRaw = "{{$fieldCfg.Default}}"
	}
	{{if $fieldCfg.Required -}}
	if err := requiredCheck("{{$fieldCfg.Alias}}", valueRaw); err != nil {
		return err
	}
	{{end -}}
	{{$fieldTypeName := GetFieldTypeName $field -}}
	{{if eq $fieldTypeName "int" -}}
	var value int
	if value, err = boundCheck("{{$fieldCfg.Alias}}", valueRaw, {{$fieldCfg.HasMin}}, {{$fieldCfg.HasMax}}, {{$fieldCfg.Min}}, {{$fieldCfg.Max}}); err != nil {
		return err
	}
	{{end -}}
	{{if eq $fieldTypeName "string" -}}
	if err := lenCheck("{{$fieldCfg.Alias}}", valueRaw, {{$fieldCfg.HasMin}}, {{$fieldCfg.Min}}); err != nil {
		return err
	}
	value := valueRaw
	{{end -}}
	{{if $fieldCfg.Enum -}}
	enum := map[string]struct{}{
		{{range $v := $fieldCfg.Enum -}}
		"{{$v}}": struct{}{},
		{{end -}}
	}
	if _, ok := enum[valueRaw]; !ok {
		variants := strings.Join({{printf "%#v" $fieldCfg.Enum}}, ", ")
		return fmt.Errorf("%s must be one of [%s]",
			"{{$fieldCfg.Alias}}", variants)
	}
	{{end -}}
	p.{{$fieldName}} = value
	return nil
}
{{end}}
{{end}}


{{range $recvName, $methods := GetRecvTypes .Methods}}
func (h *{{$recvName}}) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {

	{{- range $method := $methods -}}
	{{$methodName := GetMethodName $method}}
	{{$methodCfg := $.GetMethodConfig $methodName -}}
		
	case "{{$methodCfg.URL}}":
		h.handler{{$methodName}}(w, r)
	{{end -}}
	default:
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("{\"error\": \"unknown method\"}"))
	}
}
{{end}}

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

{{range $recvTypeName, $methods := GetRecvTypes .Methods}}
{{range $method := $methods}}
{{$methodName := GetMethodName $method}}
{{$methodCfg := $.GetMethodConfig $methodName}}
{{$methodParamTypeName := GetMethodParamTypeName $method 1}}
{{$recvName := GetMethodRecvName $method}}
func ({{$recvName}} *{{$recvTypeName}}) handler{{$methodName}}(w http.ResponseWriter, r *http.Request) {
	defer checkPanic(w)
	{{- if $methodCfg.Auth}}
	if !checkAuth(w, r) {
		w.WriteHeader(http.StatusForbidden)
		w.Write(newResponse(nil, fmt.Errorf("unauthorized")))
		return
	}
	{{end}}
	{{- if $methodCfg.HTTPMethod}}
	if !checkMethod("{{$methodCfg.HTTPMethod}}", w, r) {
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write(newResponse(nil, fmt.Errorf("bad method")))
		return
	}
	{{end}}
	p := {{$methodParamTypeName}}{}
	
	err := validate{{$methodParamTypeName}}(&p, r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(newResponse(nil, err))
		return
	}
	
	result, err := {{$recvName}}.{{$methodName}}(r.Context(), p)
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
{{end}}
{{end}}
`
