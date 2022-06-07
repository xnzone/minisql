package main

import (
	"fmt"
	"github.com/xnzone/minisql/com"
)

func main() {
	fmt.Println("hello world")
	//fmt.Println(com.SizeOf(100))
	a := 100000000
	bs := com.Int2Byte(a)
	fmt.Println(bs)
	fmt.Println(len(bs))
}
