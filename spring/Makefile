DOCKERIMAGENAME ?= snowpiperest

help:           ## Show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'
build:          ## Build the Spring Boot application
	mvn clean package

run_java:       ## Run locally with Java
	java -jar target/SnowpipeRest-0.0.1-SNAPSHOT.jar

run:            ## Start the Docker image
	docker compose up

docker:         ## Build the Docker image for the local environment
	docker build -t $(DOCKERIMAGENAME) .

docker_amd64:   ## Build the Docker image for linux/amd64
	docker build --platform linux/amd64 -t $(DOCKERIMAGENAME) .
