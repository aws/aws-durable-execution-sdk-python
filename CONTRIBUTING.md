# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.

## Dependencies
Install [hatch](https://hatch.pypa.io/dev/install/).

## Developer workflow
These are all the checks you would typically do as you prepare a PR:
```
# just test
hatch test

# coverage
hatch run test:cov

# type checks
hatch run types:check

# static analysis
hatch fmt
```

There is a convenience script for the above that you can run from the root of the repo as you prepare your PR:
```
ops/ci-checks.sh
```

## Coding Standards
Consistency is important for maintainability. Please adhere to the house-style of the repo, unless there's a really
good reason to break pattern.

### General style
1. Follow the [Python Style Guide by Google](https://google.github.io/styleguide/pyguide.html) in general.
2. Standardize to [ruff](https://docs.astral.sh/ruff/) formatting and linting rules. CI checks enforce these too.
3. Avoid pulling in extra runtime dependencies. The only dependency is [boto3](https://boto3.amazonaws.com/). The
   reason is that this SDK adds size to the AWS Lambda function of the consumer, so we should keep it as light as
   possible.
4. Never use `RLock` when `Lock` would do. The reason is to highlight recursive calls that have the potential for deadlocking
   immediately, so that RLock is a deliberate and considered decision after having considered deadlocking concerns, rather
   than just the default.

### Organization
1. Do not allow circular references, even if you can get away with it by using `if TYPE_CHECKING`. Circular references are a
   sign that the structure of the code is not clear enough. It makes for inefficient memory management and it makes the
   code harder to understand and follow. Do use `config` and `types` as the lowest-level import if you run into circular
   reference issues.
2. Do not use `__init__` files for any meaningful code or even just type declarations. Why? Because the purpose of init is not
   to serve as a grab-bag of code that doesn't otherwise have a home.
3. Do not introduce `utils` or `helper` style modules as a grab-bag of ad hoc functions. Introduce domain-specific classes to
   encapsulate and model logic.

### Data Structures & Typing
1. Model data structure with immutable classes and precise type hints. (In other words, use frozen dataclasses with exact,
   narrow type hints.) Do not rely on unstructured dicts. Why immutable? These are inherently thread-safe, and it forces you
   to think carefully about when and where you need to mutate values.

2. A rare exception to the general rule to prefer immutable classes wherever possible, is `state.ExecutionState`, which maintains
   the state of the on-going Durable Execution and encapsulates thread-safe state mutations as the execution progresses.

3. Rely on exact and explicit type declarations rather than duck typing. Why? Yes, duck typing is very pythonic. However, this
   is a complex code-base, and exact and explicit type declarations signal intent clearly so that the type checker can help
   you catch errors more quickly. LLMs have an easier time understanding the intent of the code with the type hints, and it makes
   it easier for you to spot mistaken assumptions that the LLMs might make about the code. The other reason is that it makes the
   experience of developers much easier with intelligent and context-aware autocomplete hints in an IDE.

4. Declare a type definition wherever you declare a variable, even within a function scope and even where it's implied. For example,
   even though the `str` might be _implied_ because of the `call` return type, make it explicit:

```
def my_function() -> str:
  my_var: str = arb.call(1, 2, 3)
  return f"arb result: {my_var}"
```

5. To update a field in a frozen dataclass, prefer to use a `clone` or `with_field` class method constructor or reinitialization,
   rather than dataclass `replace`. There is no big technical reason for this, it's more a soft pattern. The philosophy of an update
   should be more about thoughfully and purposefully creating a _new_ instance than "in-place editing" an existing one.


### Initialization and conversion
1. Class constructors must be light and not do more than initialize the class. In a dataclass you shouldn't even need an `__init__`.
   Use a `@classmethod` factory method instead to encapsulate more advanced logic. For example, if a class depends on logic that
   might fail, encapsulate this in a `create` classmethod:

```python
@dataclass(frozen=True)
class MyClass:
    id: str
    name: str
    timeout: int
    
    @classmethod
    def create(cls, name: str, timeout: int = 30) -> Config:
        """Factory contains """
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        
        # Generate unique ID
        config_id: str = f"cfg_{uuid.uuid4().hex[:8]}"
        
        return cls(id=config_id, name=name, timeout=timeout)
```

2. Encapsulate conversion logic in a `from_x` factory and `to_x` method on a class.

```python
@dataclass(frozen=True)
class WaitOptions:
    wait_seconds: int = 0

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> WaitOptions:
        return cls(wait_seconds=data.get("WaitSeconds", 0))

    def to_dict(self) -> MutableMapping[str, Any]:
        return {"WaitSeconds": self.wait_seconds}
```

## Set up your IDE
Point your IDE at the hatch virtual environment to have it recognize dependencies
and imports.

You can find the path to the hatch Python interpreter like this:
```
echo "$(hatch env find)/bin/python"
```

### VS Code
If you're using VS Code, "Python: Select Interpreter" and use the hatch venv Python interpreter
as found with the `hatch env find` command.

Hatch uses Ruff for static analysis.

You might want to install the [Ruff extension for VS Code](https://github.com/astral-sh/ruff-vscode)
to have your IDE interactively warn of the same linting and formatting rules.

These `settings.json` settings are useful:
```
{
  "[python]": {
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
      "source.fixAll": "explicit",
      "source.organizeImports": "explicit"
    },
    "editor.defaultFormatter": "charliermarsh.ruff"
  },
  "ruff.nativeServer": "on"
}
```

## Testing
### How to run tests
To run all tests:
```
hatch test
```

To run a single test file:
```
hatch test tests/path_to_test_module.py
```

To run a specific test in a module:
```
hatch test tests/path_to_test_module.py::test_mytestmethod
```

To run a single test, or a subset of tests:
```
$ hatch test -k TEST_PATTERN
```

This will run tests which contain names that match the given string expression (case-insensitive),
which can include Python operators that use filenames, class names and function names as variables.

### Debug
To debug failing tests:

```
$ hatch test --pdb
```

This will drop you into the Python debugger on the failed test.

### Writing tests
Place test files in the `tests/` directory, using file names that end with `_test`.

Mimic the package structure in the src/aws_durable_execution_sdk_python directory.
Name your module so that src/mypackage/mymodule.py has a dedicated unit test file
tests/mypackage/mymodule_test.py

## Coverage
```
hatch run test:cov
```

## Linting and type checks
Type checking:
```
hatch run types:check
```

Static analysis (with auto-fix of known issues):
```
hatch fmt
```

To do static analysis without auto-fixes:
```
hatch fmt --check
```

## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check existing open, or recently closed, issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

* A reproducible test case or series of steps
* The version of our code being used
* Any modifications you've made relevant to the bug
* Anything unusual about your environment or deployment


## Contributing via Pull Requests
Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Ensure local tests pass.
4. Commit to your fork using clear commit messages.
5. Send us a pull request, answering any default questions in the pull request interface.
6. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

### Pull Request Title and Commit Message Format

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification for PR titles and commit messages. This helps us maintain a clear project history and enables automated tooling.

**Format:** `type: subject`

- **type**: The type of change (required)  
- **subject**: Brief description of the change (required, max 50 characters)

**Valid types:**
- `feat`: New features
- `fix`: Bug fixes
- `docs`: Documentation changes
- `test`: Adding or updating tests
- `refactor`: Code refactoring without functional changes
- `perf`: Performance improvements
- `style`: Code style/formatting changes
- `chore`: Maintenance tasks
- `ci`: CI/CD changes
- `build`: Build system changes
- `deps`: Dependency updates

**Examples:**
```
feat: add retry mechanism for operations
fix: resolve memory leak in execution state
docs: update API documentation for context
test: add integration tests for parallel exec
feat(sdk): implement new callback functionality
fix(examples): correct timeout handling
```

**Requirements:**
- Subject line must be 50 characters or less
- Body text should wrap at 72 characters for good terminal display
- Use lowercase for type and scope
- Use imperative mood in subject ("add" not "added" or "adds")
- No period at the end of the subject line
- Use conventional commit message format with clear, concise descriptions
- Body should provide detailed explanation of changes with bullet points when helpful

**Full commit message example:**
```
feat: add retry mechanism for operations

- Implement exponential backoff strategy for transient failures
- Add configurable retry limits and timeout settings
- Include comprehensive error logging for debugging
- Update documentation with retry configuration examples

Resolves issue with intermittent network failures causing
execution interruptions in production environments.
```

The PR title will be used as the commit message when your PR is merged, so please ensure it follows this format.

GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).


## Finding contributions to work on
Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), looking at any 'help wanted' issues is a great place to start.


## Code of Conduct
This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.


## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.


## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.
