.PHONY: build-Otel1Success build-Otel2WaitResume build-Otel3Retry build-Otel4TerminalFailure \
	build-Otel5ChildContext build-Otel6Parallel build-Otel7Map build-Otel8HandledFailure \
	build-Otel9WaitForCondition build-Otel10WaitForCallback build-Otel11ChainedInvoke \
	build-Otel11InvokeTarget build-Otel12ChildContextFailure build-Otel13ParallelFailure \
	build-Otel14MapFailure build-Otel15WaitInterrupted build-Otel16WaitForConditionFailure \
	build-Otel17WaitForCallbackFailure build-Otel18ChainedInvokeFailure \
	build-Otel18InvokeTarget build-Otel19ExecutionFailure build-Otel20VirtualContext \
	build-OtelExecution1Success \
	build-OtelExecution2WaitResume build-OtelExecution3Retry \
	build-OtelExecution4TerminalFailure build-OtelExecution5ChildContext \
	build-OtelExecution6Parallel build-OtelExecution7Map build-OtelExecution8HandledFailure \
	build-OtelExecution9WaitForCondition build-OtelExecution10WaitForCallback \
	build-OtelExecution11ChainedInvoke build-OtelExecution11InvokeTarget \
	build-OtelExecution12ChildContextFailure build-OtelExecution13ParallelFailure \
	build-OtelExecution14MapFailure build-OtelExecution15WaitInterrupted \
	build-OtelExecution16WaitForConditionFailure \
	build-OtelExecution17WaitForCallbackFailure build-OtelExecution18ChainedInvokeFailure \
	build-OtelExecution18InvokeTarget build-OtelExecution19ExecutionFailure \
	build-OtelExecution20VirtualContext \
	build-OtelLongRunning1Wait \
	build-OtelLongRunning2Retry build-OtelLongRunning3Callback \
	build-OtelLongRunning4ChainedInvoke build-OtelLongRunning4InvokeTarget

build-Otel1Success build-Otel2WaitResume build-Otel3Retry build-Otel4TerminalFailure \
build-Otel5ChildContext build-Otel6Parallel build-Otel7Map build-Otel8HandledFailure \
build-Otel9WaitForCondition build-Otel10WaitForCallback build-Otel11ChainedInvoke \
build-Otel11InvokeTarget build-Otel12ChildContextFailure build-Otel13ParallelFailure \
build-Otel14MapFailure build-Otel15WaitInterrupted build-Otel16WaitForConditionFailure \
build-Otel17WaitForCallbackFailure build-Otel18ChainedInvokeFailure \
build-Otel18InvokeTarget build-Otel19ExecutionFailure build-Otel20VirtualContext \
build-OtelExecution1Success \
build-OtelExecution2WaitResume build-OtelExecution3Retry \
build-OtelExecution4TerminalFailure build-OtelExecution5ChildContext \
build-OtelExecution6Parallel build-OtelExecution7Map build-OtelExecution8HandledFailure \
build-OtelExecution9WaitForCondition build-OtelExecution10WaitForCallback \
build-OtelExecution11ChainedInvoke build-OtelExecution11InvokeTarget \
build-OtelExecution12ChildContextFailure build-OtelExecution13ParallelFailure \
build-OtelExecution14MapFailure build-OtelExecution15WaitInterrupted \
build-OtelExecution16WaitForConditionFailure \
build-OtelExecution17WaitForCallbackFailure build-OtelExecution18ChainedInvokeFailure \
build-OtelExecution18InvokeTarget build-OtelExecution19ExecutionFailure \
build-OtelExecution20VirtualContext \
build-OtelLongRunning1Wait \
build-OtelLongRunning2Retry build-OtelLongRunning3Callback \
build-OtelLongRunning4ChainedInvoke build-OtelLongRunning4InvokeTarget:
	python3 -m pip install --upgrade \
		--target "$(ARTIFACTS_DIR)" \
		--platform manylinux2014_x86_64 \
		--implementation cp \
		--python-version 3.13 \
		--only-binary=:all: \
		-r requirements.txt
	cp ./*.py "$(ARTIFACTS_DIR)/"
