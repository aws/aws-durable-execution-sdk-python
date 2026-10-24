.PHONY: build-Telemetry1Success

build-Telemetry1Success:
	python3 -m pip install --upgrade \
		--target "$(ARTIFACTS_DIR)" \
		--platform manylinux2014_x86_64 \
		--implementation cp \
		--python-version 3.13 \
		--only-binary=:all: \
		-r requirements.txt
	cp ./*.py "$(ARTIFACTS_DIR)/"
