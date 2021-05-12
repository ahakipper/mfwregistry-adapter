all: build

#usage
#

#预定义变量
ENV ?= dev
OS ?= linux
DOCKER_VERSION ?= latest

build:
	CGO_ENABLED=1 GOOS=$(OS) go build --race -o mfwregistry-adapter

docker:
	docker build docker build -t hub.mfwdev.com/paas/mkube:$(DOCKER_VERSION) .
