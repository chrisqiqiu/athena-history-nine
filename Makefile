CONTAINER = athena-history
OUT_DIR = temp
BUILD_DIR = build
KEY_DIR = keys
REGISTRY = docker-registry.mi9cdn.com:5000
ECR_REGISTRY = 462463595486.dkr.ecr.ap-southeast-2.amazonaws.com/athena-history

PWD = $(shell pwd)
APP_IMAGE = $(REGISTRY)/$(CONTAINER)
AWS_KEYS = creds.yml

all: clean build
.PHONY: all

# build app
build:
	echo "Building container"
	docker build -t  $(APP_IMAGE) .
.PHONY: build

push: build
	echo "Pushing container"
	docker push  $(APP_IMAGE)
.PHONY: push

run: push
	echo "Running container"
	docker pull $(APP_IMAGE)
	docker run --rm -t -v $(PWD)/creds:/app/creds -e CONTROLCONFIGPATH=$(CONTROLCONFIGPATH) $(APP_IMAGE) $(CMD)
.PHONY: run


# build image for ecr
build_ds_ecr_image:
	echo "Building container"
	docker build -t  $(ECR_REGISTRY) .
.PHONY: build_ecr_image

# push image to ecr
push_to_ds_ecr:
	echo "Pushing container"
	docker push  $(ECR_REGISTRY)
.PHONY: push_to_ecr



# Remove all build artifacts.
#
# Example:
#   make clean
clean:
	sudo rm -rf $(OUT_DIR)
	sudo rm -rf $(KEY_DIR)
	sudo rm -rf $(BUILD_DIR)
	sudo rm -rf tmp
.PHONY: clean

