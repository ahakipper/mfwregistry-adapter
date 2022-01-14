all: build

#usage
#

#预定义变量
ENV ?= dev
OS ?= linux
DOCKER_VERSION ?= latest

build:
	GOOS=$(OS) go build -o mfwregistry-adapter