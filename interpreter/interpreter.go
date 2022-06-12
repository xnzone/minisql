package interpreter

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/xnzone/minisql/api"
	"io"
	"os"
	"strings"
	"time"
)

type Interpreter struct {
}

func Init() *Interpreter {
	b := &Interpreter{}
	// 初始化api
	api.Init()
	return b
}

func (b *Interpreter) Run() {
	for {
		str := getCmdStr()
		cmd := tokenizer(str)
		parse(cmd)
	}
}

func getCmdStr() string {
	var cmd string
	fmt.Println("MiniSQL>")
	ir := bufio.NewReader(os.Stdin)
	for {
		b, _ := ir.ReadSlice('\n')
		b = bytes.ReplaceAll(b, []byte("\n"), []byte(" "))
		nb := handleSpecial(b)
		cmd += string(nb)
		b = bytes.TrimRight(b, " ")

		if bytes.HasSuffix(b, []byte(";")) {
			break
		}
		cmd += " "
		fmt.Print("    -> ")
	}
	return strings.TrimSpace(cmd)
}

func tokenizer(str string) []string {
	var res []string
	if str == "" || len(str) <= 0 {
		return res
	}
	str = strings.ToLower(str)
	res = strings.Split(str, " ")
	return res
}

func parse(strvec []string) {
	if len(strvec) <= 0 {
		fmt.Println("parse cmd error: cmd is too short")
		return
	}
	switch strvec[0] {
	case "create":
		if len(strvec) < 2 {
			fmt.Println("parse cmd error: cmd is too short")
			return
		}
		if strvec[1] == "table" {
			_ = parseCreateTable(strvec)
		} else if strvec[1] == "index" {
			_ = parseCreateIndex(strvec)
		} else {
			fmt.Println("ERROR: You have an error in your SQL syntax; cmd is not supported now.")
		}
	case "drop":
		if len(strvec) < 2 {
			fmt.Println("parse cmd error: cmd is too short")
			return
		}
		if strvec[1] == "table" {
			_ = parseDropTable(strvec)
		} else if strvec[1] == "index" {
			_ = parseDropIndex(strvec)
		} else {
			fmt.Println("ERROR: You have an error in your SQL syntax; cmd is not supported now.")
		}
	case "insert":
		_ = parseInsert(strvec)
	case "delete":
		_ = parseDelete(strvec)
	case "select":
		_ = parseSelect(strvec)
	case "execfile":
		_ = parseExec(strvec)
	case "quit", "exit":
		api.Flush()
		os.Exit(0)
	default:
		fmt.Println("ERROR: You have an error in your SQL syntax; cmd is not supported now.")
	}
}

func parseExec(strvec []string) error {
	if len(strvec) < 2 || strvec[2] != ";" {
		fmt.Println("ERROR: You have an error in your SQL syntax; the cmd is too short")
		return fmt.Errorf("cmd is too short")
	}
	fileName := fmt.Sprintf("test/%s", strvec[1])
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println("open file err: ", err, ", filename:", fileName)
		return fmt.Errorf("open file error")
	}
	defer func() { _ = fd.Close() }()

	start := time.Now().Unix()
	var str string
	var cmdNum int
	br := bufio.NewReader(fd)
	for {
		b, _, rerr := br.ReadLine()
		if rerr == io.EOF {
			break
		}
		b = bytes.ReplaceAll(b, []byte("\n"), []byte(" "))
		nb := handleSpecial(b)
		str += string(nb)
		b = bytes.TrimRight(b, " ")

		if bytes.HasSuffix(b, []byte(";")) {
			cmdNum++
			fmt.Println("Command ", cmdNum)
			cmd := tokenizer(str)
			parse(cmd)
			str = ""
			continue
		}
		str += " "
	}
	end := time.Now().Unix()
	fmt.Printf("Command was successfully executed and took %ds. \n", end-start)
	return nil
}

func handleSpecial(b []byte) []byte {
	nb := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		if b[i] == '*' || b[i] == '=' || b[i] == ',' || b[i] == '(' || b[i] == ')' || b[i] == '<' || b[i] == '>' || b[i] == ';' {
			if nb[len(nb)-1] == ' ' {
				nb = append(nb, b[i])
				continue
			}
			if i > 0 && i < len(b)-1 && b[i-1] != ' ' && b[i+1] != ' ' {
				nb = append(nb, []byte{' ', b[i], ' '}...)
				continue
			}
			if i > 0 && b[i-1] != ' ' {
				nb = append(nb, []byte{' ', b[i]}...)
				continue
			}
			if i < len(b)-1 && b[i+1] != ' ' {
				nb = append(nb, []byte{b[i], ' '}...)
				continue
			}
			nb = append(nb, b[i])
		} else {
			nb = append(nb, b[i])
		}
	}
	return nb
}
