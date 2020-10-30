all: build

#usage
#

#预定义变量
ENV ?= dev
OS ?= linux
DOCKER_VERSION ?= latest

build:
	GOOS=$(OS) go build -v --race

docker:
	docker build docker build -t hub.mfwdev.com/paas/mkube:$(DOCKER_VERSION) .
