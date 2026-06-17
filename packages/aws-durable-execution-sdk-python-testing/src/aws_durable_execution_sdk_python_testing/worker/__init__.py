"""Per-execution serial actor model for the local runner.

Each execution is served by a single worker that runs all of its
operations (checkpoint, get-state, invoke, timer, callback, completion)
one at a time on its own lane. Serializing the operations of one
execution keeps its state consistent when several callers act on it at
once; different executions are served by different workers and run in
parallel.
"""
