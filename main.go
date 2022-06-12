package main

import "github.com/xnzone/minisql/interpreter"

func main() {
	app := interpreter.Init()
	app.Run()
}
