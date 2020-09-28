package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// encode two regular values into a string
// that can be saved in a file.
func enc(x1 int, x2 string, x3 map[string]string) string {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(x1)
	e.Encode(x2)
	e.Encode(x3)
	return string(w.Bytes())
}

// decode a string originally produced by enc() and
// return the original values.
func dec(buf string) (int, string, map[string]string) {
	r := bytes.NewBuffer([]byte(buf))
	d := gob.NewDecoder(r)
	var x1 int
	var x2 string
	var x3 map[string]string
	d.Decode(&x1)
	d.Decode(&x2)
	d.Decode(&x3)
	return x1, x2, x3
}

func main() {
	m := make(map[string]string)
	m["a"] = "1"
	m["b"] = "2"
	m["c"] = "3"
	buf := enc(99, "hello", m)
	x1, x2, x3 := dec(buf)
	fmt.Printf("%v %v %v\n", x1, x2, x3)
}
