PROJECTNAME=mfwregistryr

MAKEFLAGS += --silent

.PHONY: all
all: help

## project:print project name
project:
	@echo $(PROJECTNAME)

## TestClient_Sync:test sync instance to mfwregistry
TestClient_Sync:
	@echo "		>  	test sync instance to mfwregistry"
	go test -v -test.run TestClient_Sync --count=1

## TestClient_GetAllOfProviderK8s:test get all k8s instances from mfwregistry
TestClient_GetAllOfProviderK8s:
	@echo "		>  	test get all instances from mfwregistry"
	go test -v -test.run TestClient_GetAllOfProviderK8s --count=1

## TestClient_GetAllOfProviderEcs:test get all ecs instances from mfwregistry
TestClient_GetAllOfProviderEcs:
	@echo "		>  	test get all instances from mfwregistry"
	go test -v -test.run TestClient_GetAllOfProviderEcs --count=1


.PHONY: help
help: Makefile
	@echo
	@echo "  Choose a command run in "$(PROJECTNAME)":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo
