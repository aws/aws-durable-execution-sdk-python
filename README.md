# AWS Durable Execution SDK for Python

[![PyPI - Version](https://img.shields.io/pypi/v/aws-durable-execution-sdk-python.svg)](https://pypi.org/project/aws-durable-execution-sdk-python)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/aws-durable-execution-sdk-python.svg)](https://pypi.org/project/aws-durable-execution-sdk-python)

-----

## Table of Contents

- [Installation](#installation)
- [License](#license)

## Installation

```console
pip install aws-durable-execution-sdk-python
```

## Developers
Please see [CONTRIBUTING.md](CONTRIBUTING.md). It contains the testing guide, sample commands and instructions
for how to contribute to this package.

tldr; use `hatch` and it will manage virtual envs and dependencies for you, so you don't have to do it manually.

## Core Architecture
The entry-point that consumers of the SDK interact with is the DurableContext.

### DurableContext Operations
- **Core Methods**: `set_logger`, `step`, `invoke`, `map`, `parallel`, `run_in_child_context`, `wait`, `create_callback`, `wait_for_callback`, `wait_for_condition`
- **Thread Safety**: Uses `OrderedCounter` for generating sequential step IDs
- **State Management**: Delegates to `ExecutionState` for checkpointing

### Concurrency Implementation
- **Map/Parallel**: Both inherit from `ConcurrentExecutor` abstract base class
- **Thread Pool**: Uses `ThreadPoolExecutor` for concurrent execution
- **State Tracking**: `ExecutableWithState` manages individual task lifecycle
- **Completion Logic**: `ExecutionCounters` tracks success/failure criteria
- **Suspension**: `TimerScheduler` handles timed suspensions and resumptions

### Configuration System
- **Modular Configs**: Separate config classes for each operation type
- **Completion Control**: `CompletionConfig` defines success/failure criteria
- **Serialization**: `SerDes` interface for custom serialization

### Operation Handlers
- **Separation of Concerns**: Each operation has dedicated handler function
- **Checkpointing**: All operations integrate with execution state checkpointing
- **Error Handling**: Consistent error handling and retry logic across operations


```mermaid
classDiagram
    class DurableContext {
        -ExecutionState state
        -Any lambda_context
        -str _parent_id
        -OrderedCounter _step_counter
        -LogInfo _log_info
        -Logger logger
        
        +set_logger(LoggerInterface new_logger)
        +step(Callable func, str name, StepConfig config) T
        +invoke(str function_name, P payload, str name, InvokeConfig config) R
        +map(Sequence inputs, Callable func, str name, MapConfig config) BatchResult
        +parallel(Sequence functions, str name, ParallelConfig config) BatchResult
        +run_in_child_context(Callable func, str name, ChildConfig config) T
        +wait(int seconds, str name)
        +create_callback(str name, CallbackConfig config) Callback
        +wait_for_callback(Callable submitter, str name, WaitForCallbackConfig config) Any
        +wait_for_condition(Callable check, WaitForConditionConfig config, str name) T
    }

    class DurableContextProtocol {
        <<interface>>
        +step(Callable func, str name, StepConfig config) T
        +run_in_child_context(Callable func, str name, ChildConfig config) T
        +map(Sequence inputs, Callable func, str name, MapConfig config) BatchResult
        +parallel(Sequence functions, str name, ParallelConfig config) BatchResult
        +wait(int seconds, str name)
        +create_callback(str name, CallbackConfig config) Callback
    }

    class OrderedCounter {
        -OrderedLock _lock
        -int _counter
        +increment() int
        +decrement() int
        +get_current() int
    }

    class ExecutionState {
        +str durable_execution_arn
        +get_checkpoint_result(str operation_id) CheckpointedResult
        +create_checkpoint(OperationUpdate operation_update)
    }

    class Logger {
        +LoggerInterface logger
        +LogInfo info
        +with_log_info(LogInfo info) Logger
        +from_log_info(LoggerInterface logger, LogInfo info) Logger
    }

    DurableContext ..|> DurableContextProtocol : implements
    DurableContext --> ExecutionState : uses
    DurableContext --> OrderedCounter : contains
    DurableContext --> Logger : contains
```

## Operation Handlers
The `DurableContext` calls operation handlers, which contain the execution logic for each operation.

```mermaid
classDiagram
    class DurableContext {
        +step(Callable func, str name, StepConfig config) T
        +invoke(str function_name, P payload, str name, InvokeConfig config) R
        +map(Sequence inputs, Callable func, str name, MapConfig config) BatchResult
        +parallel(Sequence functions, str name, ParallelConfig config) BatchResult
        +run_in_child_context(Callable func, str name, ChildConfig config) T
        +wait(int seconds, str name)
        +create_callback(str name, CallbackConfig config) Callback
        +wait_for_callback(Callable submitter, str name, WaitForCallbackConfig config) Any
        +wait_for_condition(Callable check, WaitForConditionConfig config, str name) T
    }

    class step_handler {
        <<function>>
        +step_handler(Callable func, ExecutionState state, OperationIdentifier op_id, StepConfig config, Logger logger) T
    }

    class invoke_handler {
        <<function>>
        +invoke_handler(str function_name, P payload, ExecutionState state, OperationIdentifier op_id, InvokeConfig config) R
    }

    class map_handler {
        <<function>>
        +map_handler(Sequence items, Callable func, MapConfig config, ExecutionState state, Callable run_in_child_context) BatchResult
    }

    class parallel_handler {
        <<function>>
        +parallel_handler(Sequence callables, ParallelConfig config, ExecutionState state, Callable run_in_child_context) BatchResult
    }

    class child_handler {
        <<function>>
        +child_handler(Callable func, ExecutionState state, OperationIdentifier op_id, ChildConfig config) T
    }

    class wait_handler {
        <<function>>
        +wait_handler(int seconds, ExecutionState state, OperationIdentifier op_id)
    }

    class create_callback_handler {
        <<function>>
        +create_callback_handler(ExecutionState state, OperationIdentifier op_id, CallbackConfig config) str
    }

    class wait_for_callback_handler {
        <<function>>
        +wait_for_callback_handler(DurableContext context, Callable submitter, str name, WaitForCallbackConfig config) Any
    }

    class wait_for_condition_handler {
        <<function>>
        +wait_for_condition_handler(Callable check, WaitForConditionConfig config, ExecutionState state, OperationIdentifier op_id, Logger logger) T
    }

    DurableContext --> step_handler : calls
    DurableContext --> invoke_handler : calls
    DurableContext --> map_handler : calls
    DurableContext --> parallel_handler : calls
    DurableContext --> child_handler : calls
    DurableContext --> wait_handler : calls
    DurableContext --> create_callback_handler : calls
    DurableContext --> wait_for_callback_handler : calls
    DurableContext --> wait_for_condition_handler : calls
```

## Configuration Module Classes

```mermaid
classDiagram
    class StepConfig {
        +Callable retry_strategy
        +StepSemantics step_semantics
        +SerDes serdes
    }

    class InvokeConfig~P,R~ {
        +int timeout_seconds
        +SerDes~P~ serdes_payload
        +SerDes~R~ serdes_result
    }

    class MapConfig {
        +int max_concurrency
        +ItemBatcher item_batcher
        +CompletionConfig completion_config
        +SerDes serdes
    }

    class ParallelConfig {
        +int max_concurrency
        +CompletionConfig completion_config
        +SerDes serdes
    }

    class ChildConfig~T~ {
        +SerDes serdes
        +OperationSubType sub_type
        +Callable~T,str~ summary_generator
    }

    class CallbackConfig {
        +int timeout_seconds
        +int heartbeat_timeout_seconds
        +SerDes serdes
    }

    class WaitForCallbackConfig {
        +Callable retry_strategy
    }

    class WaitForConditionConfig~T~ {
        +Callable wait_strategy
        +T initial_state
        +SerDes serdes
    }

    class CompletionConfig {
        +int min_successful
        +int tolerated_failure_count
        +float tolerated_failure_percentage
        +first_successful()$ CompletionConfig
        +all_completed()$ CompletionConfig
        +all_successful()$ CompletionConfig
    }

    class ItemBatcher~T~ {
        +int max_items_per_batch
        +float max_item_bytes_per_batch
        +T batch_input
    }

    WaitForCallbackConfig --|> CallbackConfig : extends
    MapConfig --> CompletionConfig : contains
    MapConfig --> ItemBatcher : contains
    ParallelConfig --> CompletionConfig : contains
```

## Types and Protocols Module

```mermaid
classDiagram
    class DurableContextProtocol {
        <<interface>>
        +step(Callable func, str name, StepConfig config) T
        +run_in_child_context(Callable func, str name, ChildConfig config) T
        +map(Sequence inputs, Callable func, str name, MapConfig config) BatchResult
        +parallel(Sequence functions, str name, ParallelConfig config) BatchResult
        +wait(int seconds, str name)
        +create_callback(str name, CallbackConfig config) Callback
    }

    class LoggerInterface {
        <<interface>>
        +debug(object msg, *args, Mapping extra)
        +info(object msg, *args, Mapping extra)
        +warning(object msg, *args, Mapping extra)
        +error(object msg, *args, Mapping extra)
        +exception(object msg, *args, Mapping extra)
    }

    class CallbackProtocol~C_co~ {
        <<interface>>
        +str callback_id
        +result() C_co
    }

    class BatchResultProtocol~T~ {
        <<interface>>
        +get_results() list~T~
    }

    class StepContext {
        +LoggerInterface logger
    }

    class WaitForConditionCheckContext {
        +LoggerInterface logger
    }

    class OperationContext {
        +LoggerInterface logger
    }

    StepContext --|> OperationContext : extends
    WaitForConditionCheckContext --|> OperationContext : extends
```

## SerDes Module Classes

```mermaid
classDiagram
    class SerDes~T~ {
        <<abstract>>
        +serialize(T value, SerDesContext context) str
        +deserialize(str data, SerDesContext context) T
    }

    class JsonSerDes~T~ {
        +serialize(T value, SerDesContext context) str
        +deserialize(str data, SerDesContext context) T
    }

    class SerDesContext {
        +str operation_id
        +str durable_execution_arn
    }

    class serialize {
        <<function>>
        +serialize(SerDes serdes, T value, str operation_id, str durable_execution_arn) str
    }

    class deserialize {
        <<function>>
        +deserialize(SerDes serdes, str data, str operation_id, str durable_execution_arn) T
    }

    JsonSerDes ..|> SerDes : implements
    serialize --> SerDes : uses
    deserialize --> SerDes : uses
    SerDes --> SerDesContext : uses
```

## Concurrency Architecture - Map and Parallel Operations

```mermaid
classDiagram
    class ConcurrentExecutor~CallableType,ResultType~ {
        <<abstract>>
        +list~Executable~ executables
        +int max_concurrency
        +CompletionConfig completion_config
        +ExecutionCounters counters
        +list~ExecutableWithState~ executables_with_state
        +Event _completion_event
        +SuspendExecution _suspend_exception
        
        +execute(ExecutionState state, Callable run_in_child_context) BatchResult~ResultType~
        +execute_item(DurableContext child_context, Executable executable)* ResultType
        +should_execution_suspend() SuspendResult
        -_on_task_complete(ExecutableWithState exe_state, Future future, TimerScheduler scheduler)
        -_create_result() BatchResult~ResultType~
    }

    class MapExecutor~T,R~ {
        +Sequence~T~ items
        +execute_item(DurableContext child_context, Executable executable) R
        +from_items(Sequence items, Callable func, MapConfig config)$ MapExecutor
    }

    class ParallelExecutor {
        +execute_item(DurableContext child_context, Executable executable) R
        +from_callables(Sequence callables, ParallelConfig config)$ ParallelExecutor
    }

    class Executable~CallableType~ {
        +int index
        +CallableType func
    }

    class ExecutableWithState~CallableType,ResultType~ {
        +Executable~CallableType~ executable
        -BranchStatus _status
        -Future _future
        -float _suspend_until
        -ResultType _result
        -Exception _error
        
        +run(Future future)
        +suspend()
        +suspend_with_timeout(float timestamp)
        +complete(ResultType result)
        +fail(Exception error)
        +reset_to_pending()
        +can_resume() bool
        +is_running() bool
    }

    class ExecutionCounters {
        +int total_tasks
        +int min_successful
        +int success_count
        +int failure_count
        -Lock _lock
        
        +complete_task()
        +fail_task()
        +should_complete() bool
        +is_all_completed() bool
        +is_min_successful_reached() bool
        +is_failure_tolerance_exceeded() bool
    }

    class TimerScheduler {
        +Callable resubmit_callback
        -list _pending_resumes
        -Lock _lock
        -Event _shutdown
        -Thread _timer_thread
        
        +schedule_resume(ExecutableWithState exe_state, float resume_time)
        +shutdown()
        -_timer_loop()
    }

    class BatchResult~R~ {
        +list~BatchItem~R~~ all
        +CompletionReason completion_reason
        +succeeded() list~BatchItem~R~~
        +failed() list~BatchItem~R~~
        +get_results() list~R~
        +throw_if_error()
    }

    class BatchItem~R~ {
        +int index
        +BatchItemStatus status
        +R result
        +ErrorObject error
    }

    MapExecutor --|> ConcurrentExecutor : extends
    ParallelExecutor --|> ConcurrentExecutor : extends
    ConcurrentExecutor --> ExecutableWithState : manages
    ConcurrentExecutor --> ExecutionCounters : uses
    ConcurrentExecutor --> TimerScheduler : uses
    ConcurrentExecutor --> BatchResult : creates
    ExecutableWithState --> Executable : contains
    BatchResult --> BatchItem : contains
```

## Concurrency Flow

```mermaid
sequenceDiagram
    participant DC as DurableContext
    participant MH as map_handler
    participant ME as MapExecutor
    participant CE as ConcurrentExecutor
    participant TP as ThreadPoolExecutor
    participant TS as TimerScheduler
    participant EC as ExecutionCounters

    DC->>MH: map(inputs, func, config)
    MH->>ME: MapExecutor.from_items()
    ME->>CE: execute(state, run_in_child_context)
    
    CE->>TP: ThreadPoolExecutor(max_workers)
    CE->>TS: TimerScheduler(resubmitter)
    CE->>EC: ExecutionCounters(total, min_successful)
    
    loop For each executable
        CE->>TP: submit_task(executable_with_state)
        TP->>CE: execute_item_in_child_context()
        CE->>DC: run_in_child_context(child_func)
        DC->>ME: execute_item(child_context, executable)
    end
    
    par Task Completion Handling
        TP->>CE: on_task_complete(future)
        CE->>EC: complete_task() / fail_task()
        CE->>CE: should_execution_suspend()
        alt Should Complete
            CE->>CE: _completion_event.set()
        else Should Suspend
            CE->>TS: schedule_resume(exe_state, timestamp)
        end
    end
    
    CE->>CE: _completion_event.wait()
    CE->>CE: _create_result()
    CE->>DC: BatchResult
```

## Threading and Locking

```mermaid
classDiagram
    class OrderedLock {
        -Lock _lock
        -deque~Event~ _waiters
        -bool _is_broken
        -Exception _exception
        
        +acquire() bool
        +release()
        +reset()
        +is_broken() bool
        +__enter__() OrderedLock
        +__exit__(exc_type, exc_val, exc_tb)
    }

    class OrderedCounter {
        -OrderedLock _lock
        -int _counter
        
        +increment() int
        +decrement() int
        +get_current() int
    }

    class Event {
        <<threading.Event>>
        +set()
        +wait()
    }

    class Lock {
        <<threading.Lock>>
        +acquire()
        +release()
    }

    OrderedCounter --> OrderedLock : uses
    OrderedLock --> Lock : contains
    OrderedLock --> Event : manages queue of
```

## Checkpointing System

The SDK invokes the AWS Lambda checkpoint API to persist execution state. Checkpoints are batched for efficiency and can be either 
synchronous (blocking) or asynchronous (non-blocking). Critical checkpoints are blocking,
meaning that execution will not proceed until the checkpoint call has successfully completed.

### Checkpoint Types

Checkpoints are categorized by their action (START, SUCCEED, FAIL) and whether they are critical to execution correctness:

| Operation Type | Action | Is Sync? | Rationale |
|---------------|--------|----------|-----------|
| Step (AtMostOncePerRetry) | START | Yes | Prevents duplicate execution - must wait for confirmation |
| Step (AtLeastOncePerRetry) | START | No | Performance optimization - idempotent operations can retry |
| Step | SUCCEED/FAIL | Yes | Ensures result persisted before returning to caller |
| Callback | START | Yes | Must wait for API to generate callback ID |
| Callback | SUCCEED/FAIL | Yes | Ensures callback result persisted |
| Invoke | START | Yes | Ensures chained invoke recorded before proceeding |
| Invoke | SUCCEED/FAIL | Yes | Ensures invoke result persisted |
| Context (Child) | START | No | Fire-and-forget for performance - parent tracks completion |
| Context (Child) | SUCCEED/FAIL | Yes | Ensures child result available to parent |
| Wait | START | No | Observability only - no blocking needed |
| Wait | SUCCEED | Yes | Ensures wait completion recorded |
| Wait for Condition | START | No | Observability only - condition check is idempotent |
| Wait for Condition | SUCCEED/FAIL | Yes | Ensures condition result persisted |
| Empty Checkpoint | N/A | Yes (default) | Refreshes checkpoint token and operations list |

### Synchronous vs Asynchronous Checkpoints

**Synchronous Checkpoints (is_sync=True, default)**:
- Block the caller until the checkpoint is processed by the background thread
- Ensure the checkpoint is persisted before continuing execution
- Safe default for correctness
- Used for critical operations where confirmation is required

**Asynchronous Checkpoints (is_sync=False, opt-in)**:
- Return immediately without waiting for the checkpoint to complete
- Performance optimization for specific use cases
- Used for observability checkpoints and fire-and-forget operations
- Only safe when the operation is idempotent or non-critical

### Checkpoint Batching

The SDK uses a background thread to batch multiple checkpoint operations into a single API call for efficiency. This reduces API overhead and
improves throughput.

```mermaid
sequenceDiagram
    participant MT as Main Thread
    participant Q as Checkpoint Queue
    participant BT as Background Thread
    participant API as Durable Functions API
    
    Note over MT,API: Synchronous Checkpoint Flow
    MT->>Q: Enqueue operation + completion event
    MT->>MT: Block on completion event
    BT->>Q: Collect batch (up to 1 second or 750KB)
    BT->>API: POST /checkpoint (batched operations)
    API-->>BT: New checkpoint token + operations
    BT->>BT: Update execution state
    BT->>MT: Signal completion event
    MT->>MT: Resume execution
    
    Note over MT,API: Asynchronous Checkpoint Flow
    MT->>Q: Enqueue operation (no event)
    MT->>MT: Continue immediately
    BT->>Q: Collect batch (up to 1 second or 750KB)
    BT->>API: POST /checkpoint (batched operations)
    API-->>BT: New checkpoint token + operations
    BT->>BT: Update execution state
```

### Batching Configuration

Checkpoint batching is controlled by `CheckpointBatcherConfig`:

```python
@dataclass(frozen=True)
class CheckpointBatcherConfig:
    max_batch_size_bytes: int = 750 * 1024  # 750KB
    max_batch_time_seconds: float = 1.0     # 1 second
    max_batch_operations: int | float = float("inf")  # No limit
```

The background thread collects operations until one of these limits is reached:
1. Batch size exceeds 750KB
2. 1 second has elapsed since the first operation
3. Maximum operation count is reached (unlimited by default)

### Concurrency Management

The checkpointing system handles concurrent operations (map/parallel) by tracking parent-child relationships:

1. When a CONTEXT operation completes (SUCCEED/FAIL), all descendant operations are marked as orphaned
2. Orphaned operations are rejected if they attempt to checkpoint
3. This prevents child operations from checkpointing after their parent has already completed
4. Uses a single lock (`_parent_done_lock`) to coordinate completion and checkpoint validation

### Error Handling

When a checkpoint fails in the background thread:

1. **Error Signaling**: The background thread creates a `BackgroundThreadError` wrapping the original exception
2. **Event Notification**: All completion events (both in the current batch and queued operations) are signaled with this error
3. **Immediate Propagation**: Synchronous callers waiting on `create_checkpoint(is_sync=True)` immediately receive the `BackgroundThreadError` 
4. **Future Prevention**: A failure event (`_checkpointing_failed`) is set to prevent any future checkpoint attempts
5. **Clean Termination**: The background thread exits cleanly after signaling all waiting operations

For **synchronous operations** (default `is_sync=True`):
- The main thread receives `BackgroundThreadError` immediately when calling `create_checkpoint()`
- This prevents further execution with corrupted state

For **asynchronous operations** (`is_sync=False`):
- The error is detected on the next synchronous checkpoint attempt
- The `_checkpointing_failed` event causes immediate failure before queuing

This ensures no code continues executing after a checkpoint failure, maintaining execution state integrity.

## License

This project is licensed under the [Apache-2.0 License](LICENSE).
