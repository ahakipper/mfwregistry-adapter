all: build

#usage
#

# Predefined variables
ENV ?= dev
OS ?= linux
DOCKER_VERSION ?= latest

build:
	GOOS=$(OS) go build -o spotter