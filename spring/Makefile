DOCKERIMAGENAME ?= snowpiperest

build:
	mvn clean package

docker_native:
	DOCKER_BUILDKIT=1 docker build -f Dockerfile --no-cache --progress=plain  -t $(DOCKERIMAGENAME) .

docker:
	DOCKER_DEFAULT_PLATFORM=linux/amd64 DOCKER_BUILDKIT=1 docker build -f Dockerfile --no-cache --progress=plain  -t $(DOCKERIMAGENAME) .

