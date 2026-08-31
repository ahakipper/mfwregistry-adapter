PROJECTNAME=spotter

MAKEFLAGS += --silent

.PHONY: all
all: help

## project:print project name
project:
	@echo $(PROJECTNAME)

## TestClient_Sync:test sync instance to the discovery center
TestClient_Sync:
	@echo "		>  	test sync instance to the discovery center"
	go test -v -test.run TestClient_Sync --count=1

## TestClient_GetAllOfProviderK8s:test get all k8s instances from the discovery center
TestClient_GetAllOfProviderK8s:
	@echo "		>  	test get all K8s instances from the discovery center"
	go test -v -test.run TestClient_GetAllOfProviderK8s --count=1

## TestClient_GetAllOfProviderEcs:test get all ecs instances from the discovery center
TestClient_GetAllOfProviderEcs:
	@echo "		>  	test get all Ecs instances from the discovery center"
	go test -v -test.run TestClient_GetAllOfProviderEcs --count=1


.PHONY: help
help: Makefile
	@echo
	@echo "  Choose a command run in "$(PROJECTNAME)":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo
