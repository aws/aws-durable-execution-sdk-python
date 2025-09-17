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
