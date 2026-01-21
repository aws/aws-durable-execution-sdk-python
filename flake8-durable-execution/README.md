# flake8-durable-execution

Flake8 plugin for AWS Durable Execution SDK best practices.

## Installation

```bash
pip install flake8-durable-execution
```

## Usage

```bash
flake8 your_code.py
```

The plugin is automatically detected by Flake8.

## Rules

| Code | Description |
|------|-------------|
| DAR001 | Avoid random calls in durable functions - results differ on replay |
| DAR002 | Avoid datetime.now()/today() in durable functions - time differs on replay |
| DAR003 | Avoid uuid1()/uuid4() in durable functions - generates different values on replay |
| DAR004 | Avoid time.time()/monotonic() in durable functions - time differs on replay |
| DAR005 | Avoid os.environ access in durable functions - environment may differ on replay |
| DAR006 | Wrap network calls in ctx.step() - responses may differ on replay |
| DAR007 | Durable operations cannot be called inside @durable_step |
| DAR008 | Avoid mutating closure variables in steps - mutations are lost on replay |

## Why?

Durable functions may be replayed multiple times. Non-deterministic operations like `random.random()`, `datetime.now()`, or `uuid.uuid4()` will produce different values on each replay, causing unexpected behavior.

Instead, use durable steps to checkpoint these values so they remain consistent across replays.

## Examples

### Bad

```python
@durable_execution
def handler(event, ctx):
    # DAR001: random value changes on replay
    value = random.randint(1, 100)
    
    # DAR002: time changes on replay
    now = datetime.now()
    
    # DAR006: network response may differ
    response = requests.get("https://api.example.com")
```

### Good

```python
@durable_execution
def handler(event, ctx):
    # Checkpoint non-deterministic values
    value = ctx.step(lambda: random.randint(1, 100))
    now = ctx.step(lambda: datetime.now().isoformat())
    response = ctx.step(lambda: requests.get("https://api.example.com").json())
```
