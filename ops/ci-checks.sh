
#!/bin/sh

set -e

hatch run test:cov
echo SUCCESS: tests + coverage

# type checks
hatch run types:check
echo SUCCESS: typings

# static analysis
hatch fmt
echo SUCCESS: linting/fmt
