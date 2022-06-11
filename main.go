package main

import (
	"fmt"
	"github.com/xnzone/minisql/util"
)

func main() {
	fmt.Println("hello world")
	//fmt.Println(util.SizeOf(100))
	a := 100000000
	bs := util.Int2Byte(a)
	fmt.Println(bs)
	fmt.Println(len(bs))
	b := []byte("1")
	fmt.Println(b)
}
