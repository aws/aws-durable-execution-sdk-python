#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from parse_sdk_branch import parse_sdk_branch


def test_parse_sdk_branch():
    test_cases = [
        # Basic cases
        ("TESTING_SDK_BRANCH = feature/test", "feature/test"),
        ("TESTING_SDK_BRANCH: feature/test", "feature/test"),
        ("TESTING_SDK_BRANCH=feature/test", "feature/test"),
        ("testing_sdk_branch: feature/test", "feature/test"),
        
        # Complex PR body with backticks and contractions
        ("""Updated the script to safely parse the testing SDK branch from the PR body, handling case insensitivity and whitespace.

The goal here is to fix the usage of backticks such as in `foo`, and contractions that we've been using such as `we've`

```
plus of course the usage of multiple backticks to include code
```

TESTING_SDK_BRANCH = main

By submitting this pull request, I confirm that you can use, modify, copy, and redistribute this contribution, under the terms of your choice.""", "main"),
        
        # Edge cases with markdown and special characters
        ("""# PR Title
        
Some `code` and we've got contractions here.

```python
def test():
    return "test"
```

TESTING_SDK_BRANCH: feature/fix-backticks

More text with `inline code` and don't forget contractions.""", "feature/fix-backticks"),
        
        # Multiple occurrences (should take first)
        ("""TESTING_SDK_BRANCH = first-branch
        
Some text here.

TESTING_SDK_BRANCH = second-branch""", "first-branch"),
        
        # Whitespace variations
        ("   TESTING_SDK_BRANCH   =   feature/spaces   ", "feature/spaces"),
        ("TESTING_SDK_BRANCH:feature/no-space", "feature/no-space"),
        
        # Default cases
        ("No branch specified", "main"),
        ("", "main"),
        ("Just some random text", "main"),
        
        # Case with backticks in branch name
        ("TESTING_SDK_BRANCH = feature/fix-`backticks`", "feature/fix-`backticks`"),
        
        # Case with contractions in surrounding text
        ("We've updated this and TESTING_SDK_BRANCH = feature/test and we're done", "feature/test"),
    ]
    
    for i, (input_text, expected) in enumerate(test_cases):
        result = parse_sdk_branch(input_text)
        if result != expected:
            print(f"FAIL Test {i+1}: Expected '{expected}', got '{result}'")
            print(f"Input: {repr(input_text[:100])}...")
            return False
        else:
            print(f"PASS Test {i+1}: {expected}")
    
    print(f"\nAll {len(test_cases)} tests passed!")
    return True


if __name__ == "__main__":
    success = test_parse_sdk_branch()
    sys.exit(0 if success else 1)
