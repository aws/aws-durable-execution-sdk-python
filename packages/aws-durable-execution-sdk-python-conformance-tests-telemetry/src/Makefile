.PHONY: build-Telemetry1Success build-Telemetry2Failure build-Telemetry3ResultSelect build-handlers

# Every function builds from the same src/ directory with the same recipe.
# SAM calls one target per logical ID, so each target points at the shared one.
build-Telemetry1Success: build-handlers

build-Telemetry2Failure: build-handlers

build-Telemetry3ResultSelect: build-handlers

build-handlers:
	python3 -m pip install --upgrade \
		--target "$(ARTIFACTS_DIR)" \
		--platform manylinux2014_x86_64 \
		--implementation cp \
		--python-version 3.13 \
		--only-binary=:all: \
		-r requirements.txt
	cp ./*.py "$(ARTIFACTS_DIR)/"
