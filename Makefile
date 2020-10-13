all: build

#usage
#

#预定义变量
ENV ?= dev
OS ?= linux
DOCKER_VERSION ?= latest

packstatic:
	go-bindata -pkg config -o config/bindata.go config/*

build:
	GOOS=$(OS) go build -v 

docker:
	docker build docker build -t hub.mfwdev.com/paas/mkube:$(DOCKER_VERSION) .
