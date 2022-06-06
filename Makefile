# GOPATH:=$(shell go env GOPATH)

#需要修改部分
BINARY=minisql
MAIN_FILE=main.go

#不要修改下列脚本
REPO=minisql/

VERSION=$(shell git describe --tags --always --long --dirty)
GIT_PATH=$(shell git remote -v | head -n 1 | awk '{print $$2}')

BUILD_TIME=$(shell date +%FT%T%z)
LDFLAGS=-ldflags "-X ${REPO}util.BuildVersion=${VERSION} -X ${REPO}util.BuildTime=${BUILD_TIME} -X ${REPO}util.RemoteGitPath=${GIT_PATH}"

REQUESTS=$(shell pip3 list | grep requests)
PRECOMMIT=$(shell pip3 list | grep pre-commit)

.PHONY: build build_local build_64linux install clean version

build: 
	go fmt ./...
	go vet ./...
	go build -o bin/${BINARY} ${LDFLAGS} ${MAIN_FILE}

build_local:
	go fmt ./...
	go vet ./...
	go build -o bin/${BINARY} ${MAIN_FILE}

build_64linux:
	go fmt ./...
	go vet ./...
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/${BINARY} ${LDFLAGS} ${MAIN_FILE}

test:
	go test -v ./... -cover

install:
	go install ${LDFLAGS} ./...

version:
	@if [ -f bin/${BINARY} ] ; then bin/${BINARY} -buildinfo; fi

clean:
	@if [ -f bin/${BINARY} ] ; then rm bin/${BINARY} ; fi

