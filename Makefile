.PHONY: all
all: build-all run

.PHONY: run
run: 
	docker-compose up -d

.PHONY: down
down: 
	docker-compose down

build-all: build-consumer build-producer build-validator build-data_generator build-pgsql_client build-kafka

push-all: push-consumer push-producer push-validator push-data_generator push-pgsql_client push-kafka

build-consumer:
	podman build \
		-f ./workload/consumer/Dockerfile \
		-t us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/consumer:latest \
		-t consumer:latest \
		workload/consumer

build-producer:
	podman build \
		-f ./workload/producer/Dockerfile \
		-t us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/producer:latest \
		-t producer:latest \
		workload/producer

build-validator:
	podman build \
		-f ./workload/validator/Dockerfile \
		-t us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/validator:latest \
		-t validator:latest \
		workload/validator

build-data_generator:
	podman build \
		-f ./data-generator/faker.Dockerfile \
		-t us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/data_generator:latest \
		-t data_generator:latest \
		data-generator

build-pgsql_client:
	podman build \
		-f ./data-generator/pgsql_client.Dockerfile \
		-t us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/pgsql_client:latest \
		-t pgsql_client:latest \
		data-generator

build-config:
	podman build \
		-f ./config.Dockerfile \
		-t us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/demo-data-streaming-config:latest \
		-t demo-data-streaming-config:latest \
		.

build-kafka:
	podman build \
		-f ./kafka/kafka.Dockerfile \
		-t us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/kafka:latest \
		-t kafka:latest \
		kafka

push-consumer:
	customer credentials_shell -c "podman push us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/consumer:latest"

push-producer:
	customer credentials_shell -c "podman push us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/producer:latest"

push-validator:
	customer credentials_shell -c "podman push us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/validator:latest"

push-data_generator:
	customer credentials_shell -c "podman push us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/data_generator:latest"

push-pgsql_client:
	customer credentials_shell -c "podman push us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/pgsql_client:latest"

push-config:
	customer credentials_shell -c "podman push us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/demo-data-streaming-config:latest"

push-kafka:
	customer credentials_shell -c "podman push us-central1-docker.pkg.dev/molten-verve-216720/demo-repository/kafka:latest"

