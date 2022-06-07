package main

import (
	"fmt"
	"unsafe"
)

func main() {
	fmt.Println("hello world")
	//fmt.Println(com.SizeOf(100))
	fmt.Println(unsafe.Sizeof(('1')))
}
