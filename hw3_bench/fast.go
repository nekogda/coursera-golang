package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	android string = "Android"
	msie           = "MSIE"
)

type User struct {
	Name     string   `json:"name"`
	Email    string   `json:"email"`
	Browsers []string `json:"browsers"`
}

func FastSearch(out io.Writer) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	seenBrowsers := make(map[string]struct{}, 150)
	bufReader := bufio.NewReader(file)

	androidB := []byte(android)
	msieB := []byte(msie)
	user := User{}
	index := -1
	fmt.Fprintln(out, "found users:")
	for {
		index++
		segment, err := bufReader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		if !(bytes.Contains(segment, androidB) || bytes.Contains(segment, msieB)) {
			continue
		}
		if err := json.Unmarshal(segment, &user); err != nil {
			panic(err)
		}
		isAndroid := false
		isMSIE := false
		for _, browser := range user.Browsers {
			isAndroidFinded := strings.Contains(browser, android)
			isMSIEFinded := strings.Contains(browser, msie)
			if isAndroidFinded || isMSIEFinded {
				isAndroid = isAndroid || isAndroidFinded
				isMSIE = isMSIE || isMSIEFinded
				_, ok := seenBrowsers[browser]
				if !ok {
					seenBrowsers[browser] = struct{}{}
				}
			}
		}
		if !(isAndroid && isMSIE) {
			continue
		}
		atIdx := strings.Index(user.Email, "@")
		if atIdx == -1 || atIdx == len(user.Email)-1 {
			panic("malformed email")
		}
		fmt.Fprintf(out, "[%d] %s <%s [at] %s>\n",
			index, user.Name, user.Email[:atIdx], user.Email[atIdx+1:])
	}
	fmt.Fprintln(out, "\nTotal unique browsers", len(seenBrowsers))
}
