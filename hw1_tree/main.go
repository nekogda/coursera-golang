package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
)

const (
	prefixBase1 string = `├───`
	prefixBase2 string = `│`
	prefixLast  string = `└───`
	prefixFill  string = "\t"
)

type node os.FileInfo
type tree [][]node // stack of levels

func (t *tree) push(nodes []node) {
	*t = append(*t, nodes)
	return
}

func (t *tree) pop() (node, error) {
	n, ok := t.take()
	if !ok {
		return nil, fmt.Errorf("pop from empty slice")
	}
	// remove last element and empty levels from tree
	for i := len(*t) - 1; i >= 0; i-- {
		level := (*t)[i]
		// removing element from level
		(*t)[i] = level[:len(level)-1]
		if len((*t)[i]) != 0 {
			break
		}
		// remove last/empty level
		*t = (*t)[:i]
	}
	return n, nil
}

func (t *tree) take() (n node, ok bool) {
	if len(*t) == 0 {
		return nil, false
	}
	// get last level
	lastLevel := (*t)[len(*t)-1]
	// get last node from level
	n = lastLevel[len(lastLevel)-1]
	return n, true
}

func (t *tree) getPrefix() []bool {
	var result []bool
	for i := range *t {
		result = append(result, len((*t)[i]) == 1)
	}
	return result
}

func (t *tree) getPath(root string) string {
	result := root
	// take last node from each level of the tree
	for i := range *t {
		result = path.Join(result, (*t)[i][len((*t)[i])-1].Name())
	}
	return result
}

func nodeToA(n node) string {
	if n.IsDir() {
		return fmt.Sprintf("%s", n.Name())
	}
	return fmt.Sprintf("%s %s", n.Name(), sizeToA(n.Size()))
}

func printNode(w io.Writer, prefix []bool, n node) error {
	_, err := fmt.Fprintf(w, "%s%s\n", prefixToA(prefix), nodeToA(n))
	return err
}

func prefixToA(prefix []bool) string {
	var result string
	for _, isLast := range prefix[:len(prefix)-1] {
		if isLast {
			result += prefixFill
		} else {
			result += prefixBase2 + prefixFill
		}
	}
	// last part of the prefix
	if prefix[len(prefix)-1] {
		result += prefixLast
	} else {
		result += prefixBase1
	}
	return result
}

func sizeToA(size int64) string {
	if size == 0 {
		return "(empty)"
	}
	return "(" + strconv.Itoa(int(size)) + "b)"
}

func getNodesUtil(filePath string, withFiles bool) ([]node, error) {
	var result []node
	fileInfos, err := ioutil.ReadDir(filePath)
	if err != nil {
		return nil, err
	}
	for i := range fileInfos {
		if !fileInfos[i].IsDir() && !withFiles {
			// skip files if it's not needed
			continue
		}
		result = append(result, (node)(fileInfos[i]))
	}
	return result, nil
}

func sortNodes(nodes []node) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Name() > nodes[j].Name()
	})
}

func getNodes(filePath string, withFiles bool) ([]node, error) {
	nodes, err := getNodesUtil(filePath, withFiles)
	if err != nil {
		return nil, err
	}
	sortNodes(nodes)
	return nodes, nil
}

func dirTree(out io.Writer, filePath string, withFiles bool) (err error) {
	var t tree
	var nodes []node
	if nodes, err = getNodes(filePath, withFiles); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}
	t.push(nodes)
	for len(t) > 0 {
		lastNode, _ := t.take()
		if err = printNode(out, t.getPrefix(), lastNode); err != nil {
			return err
		}
		if !lastNode.IsDir() {
			_, _ = t.pop()
			continue
		}
		if nodes, err = getNodes(t.getPath(filePath), withFiles); err != nil {
			return err
		}
		// for empty directories
		if len(nodes) == 0 {
			_, _ = t.pop()
		} else {
			t.push(nodes)
		}
	}
	return nil
}

func run(args []string) {
	out := os.Stdout
	if !(len(args) == 2 || len(args) == 3) {
		panic("usage go run main.go . [-f]")
	}
	path := args[1]
	printFiles := len(args) == 3 && args[2] == "-f"
	err := dirTree(out, path, printFiles)
	if err != nil {
		panic(err.Error())
	}
}

func main() {
	run(os.Args)
}
