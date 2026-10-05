/**
 * Local stand-in for the Lambda Invoke API, for the JS examples harness.
 *
 * The Python local runner invokes handlers by calling Lambda Invoke against
 * its --lambda-endpoint, which is this process. For each Invoke, the shim:
 *
 *   1. Looks up the FunctionName in the shim map, which run.py generates from
 *      the examples' template.yml. The entry names the bundle, the export,
 *      the Timeout and the environment variables.
 *   2. Runs the handler on a worker thread that belongs to that function.
 *      The worker's environment points AWS_ENDPOINT_URL_LAMBDA at the runner,
 *      so the durable SDK checkpoints to the runner.
 *   3. Answers with the handler's result, or with a function error
 *      (X-Amz-Function-Error: Unhandled) when the handler throws.
 *
 * Workers model Lambda sandboxes:
 *
 *   - A worker runs one invocation at a time. Concurrent invocations of one
 *     function get separate workers, and so do different qualifiers of it.
 *   - After an invocation, the worker goes back to an idle pool for its
 *     function and serves a later invocation. So module state carries over,
 *     as it does in a warm Lambda sandbox.
 *   - Lambda ends an invocation that runs past the function's Timeout. Some
 *     examples depend on that, for example a step that is interrupted
 *     mid-run. A handler on the shim's own thread could not be stopped. A
 *     worker can. So on Timeout the shim answers with Lambda's
 *     Sandbox.Timedout function error and terminates the worker. The next
 *     invocation of that function starts a new worker, as Lambda starts a new
 *     sandbox.
 *
 * Each worker logs one line per invocation to stderr: time, request ID,
 * function, duration, and "ok" or the error type.
 *
 * Usage: node lambda-shim.cjs --port <port> --map <shim-map.json>
 *          --dist <examples dist dir> --runner <runner base URL>
 */

"use strict";

const {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} = require("node:worker_threads");

// The runner emulates one region and one account. The invoke proxy uses the
// same values. The runner rejects chained invokes whose target ARN names a
// different region or account.
const REGION = "us-west-2";
// Lambda limits a function's initialisation phase to 10 seconds.
const INIT_TIMEOUT_MS = 10_000;
const ACCOUNT_ID = "123456789012";

if (isMainThread) {
  runServer();
} else {
  runSandbox();
}

// ---------------------------------------------------------------------------
// Main thread: the HTTP server and the worker pools.
// ---------------------------------------------------------------------------

function runServer() {
  const http = require("node:http");
  const fs = require("node:fs");
  const crypto = require("node:crypto");

  const port = Number(argValue("--port"));
  const mapPath = argValue("--map");
  const dist = argValue("--dist");
  const runner = argValue("--runner");
  if (!port || !mapPath || !dist || !runner) {
    console.error(
      "usage: lambda-shim.cjs --port <port> --map <file> --dist <dir> --runner <url>",
    );
    process.exit(2);
  }
  const functions = JSON.parse(fs.readFileSync(mapPath, "utf-8"));
  // Pool key -> idle workers. The key is the function name, qualifier and
  // tenant, because Lambda gives each version, and each tenant of a
  // tenant-isolated function, its own environments. So module state never
  // carries from one version or tenant to another.
  const idle = new Map();
  const invokePath = /^\/2015-03-31\/functions\/([^/]+)\/invocations\/?$/;

  const server = http.createServer((req, res) => {
    const url = req.url.split("?")[0];
    if (req.method === "GET" && url === "/health") {
      sendJson(res, 200, { status: "ok" });
      return;
    }
    const match = invokePath.exec(url);
    if (req.method !== "POST" || !match) {
      sendJson(res, 404, { message: `No route for ${req.method} ${url}` });
      return;
    }
    const { functionName, qualifier } = parseIdentifier(
      decodeURIComponent(match[1]),
    );
    const config = functions[functionName];
    if (!config) {
      // Lambda's error for an unknown function. boto raises it as
      // ResourceNotFoundException.
      sendJson(
        res,
        404,
        { Type: "User", message: `Function not found: ${functionName}` },
        { "x-amzn-ErrorType": "ResourceNotFoundException" },
      );
      return;
    }
    // Invoke carries the tenant of a tenant-isolated function in this
    // header. Lambda exposes it to the handler as context.tenantId.
    const tenantId = req.headers["x-amz-tenant-id"];
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      invoke(
        functionName,
        qualifier,
        tenantId,
        config,
        Buffer.concat(chunks).toString("utf-8"),
        res,
      );
    });
  });

  function startWorker(functionName, poolKey, config) {
    const worker = new Worker(__filename, {
      workerData: {
        bundle: `${dist}/${config.file}.js`,
        exportName: config.export,
        functionName,
      },
      // A worker's environment is a copy. So one function's variables never
      // reach another function's workers.
      env: {
        ...process.env,
        ...config.environment,
        AWS_ENDPOINT_URL_LAMBDA: runner,
        AWS_REGION: REGION,
        AWS_DEFAULT_REGION: REGION,
        AWS_LAMBDA_FUNCTION_NAME: functionName,
        AWS_LAMBDA_FUNCTION_TIMEOUT: String(config.timeoutSeconds),
      },
    });
    // Background work a handler left running can crash an idle worker. An
    // "error" event with no listener would crash the shim itself. So every
    // worker keeps a listener, and a worker that exits leaves the pool.
    worker.on("error", () => {});
    worker.on("exit", () => {
      const pool = idle.get(poolKey) ?? [];
      const i = pool.indexOf(worker);
      if (i >= 0) pool.splice(i, 1);
    });
    return worker;
  }

  function invoke(functionName, qualifier, tenantId, config, rawEvent, res) {
    const requestId = crypto.randomUUID();
    const timeoutMs = config.timeoutSeconds * 1000;
    const started = Date.now();
    // Lambda's tenant isolation gives each tenant its own environments too.
    const poolKey = JSON.stringify([functionName, qualifier ?? "$LATEST", tenantId ?? null]);
    const pool = idle.get(poolKey) ?? [];
    idle.set(poolKey, pool);
    const worker = pool.pop() ?? startWorker(functionName, poolKey, config);

    let answered = false;
    // text, when given, is the response body, already serialised by the
    // worker. Otherwise body is serialised here.
    const answer = (status, body, headers = {}, text = undefined) => {
      if (answered) return;
      answered = true;
      clearTimeout(timer);
      worker.off("message", onMessage);
      worker.off("error", onCrash);
      worker.off("exit", onExit);
      // Error payloads are built by the shim, so they always serialise.
      text ??= JSON.stringify(body);
      const outcome = headers["X-Amz-Function-Error"] ? body.errorType : "ok";
      console.error(
        `${new Date().toISOString()} ${requestId} ${functionName} ${Date.now() - started}ms ${outcome}`,
      );
      sendText(res, status, text, { "x-amzn-RequestId": requestId, ...headers });
    };
    const functionError = (payload) =>
      answer(200, payload, { "X-Amz-Function-Error": "Unhandled" });

    const onMessage = (msg) => {
      if (msg.requestId !== requestId) return;
      if (msg.initialized) {
        // The handler is loaded. Lambda does not count initialisation
        // against the function's timeout, so the timeout starts now.
        clearTimeout(timer);
        timer = setTimeout(timedOut, timeoutMs);
        return;
      }
      if (msg.ok) {
        answer(200, undefined, {}, msg.text);
      } else {
        functionError(msg.error);
      }
      pool.push(worker);
    };
    // An error that escapes the handler, for example from a timer callback,
    // ends the worker. Lambda also replaces the sandbox after such a crash.
    const onCrash = (err) => functionError(serializeError(err));
    const onExit = (code) =>
      functionError({
        errorType: "Runtime.ExitError",
        errorMessage: `RequestId: ${requestId} Error: Runtime exited with error: exit status ${code}`,
      });
    worker.on("message", onMessage);
    worker.on("error", onCrash);
    worker.on("exit", onExit);

    const timedOut = () => {
      functionError({
        errorType: "Sandbox.Timedout",
        errorMessage: `${new Date().toISOString()} ${requestId} Task timed out after ${config.timeoutSeconds.toFixed(2)} seconds`,
      });
      // terminate() stops the handler wherever it is. The worker is not
      // returned to the pool, so the next invocation gets a fresh one.
      worker.terminate();
    };
    // Until the worker reports that the handler is loaded, only a bundle
    // that never finishes loading can end the invocation.
    let timer = setTimeout(timedOut, INIT_TIMEOUT_MS);

    worker.postMessage({ requestId, qualifier, tenantId, rawEvent, timeoutMs });
  }

  function sendJson(res, status, body, headers = {}) {
    sendText(res, status, JSON.stringify(body), headers);
  }

  function sendText(res, status, text, headers = {}) {
    res.writeHead(status, {
      "Content-Type": "application/json",
      "Content-Length": Buffer.byteLength(text),
      ...headers,
    });
    res.end(text);
  }

  server.listen(port, "127.0.0.1", () => {
    console.error(`lambda-shim listening on 127.0.0.1:${port}, runner ${runner}`);
  });
}

/**
 * Split a Lambda function identifier into the function name and qualifier.
 *
 * Invoke accepts a name, "name:qualifier", or a function ARN with or without
 * a qualifier. The runner invokes "name:qualifier" for any qualifier other
 * than $LATEST. The shim map holds only bare names, and one function serves
 * every qualifier, as the runner's function configs do. So the lookup uses
 * the name, and the qualifier is kept for the Lambda context.
 */
function parseIdentifier(identifier) {
  const parts = identifier.split(":");
  if (parts[0] === "arn") {
    // arn:aws:lambda:<region>:<account>:function:<name>[:<qualifier>]
    return { functionName: parts[6], qualifier: parts[7] };
  }
  return { functionName: parts[0], qualifier: parts[1] };
}

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

// ---------------------------------------------------------------------------
// Worker thread: one sandbox for one function.
// ---------------------------------------------------------------------------

function runSandbox() {
  const { bundle, exportName, functionName } = workerData;
  let handler;
  parentPort.on(
    "message",
    async ({ requestId, qualifier, tenantId, rawEvent, timeoutMs }) => {
      try {
        // The bundle loads on the first invocation, like a Lambda cold start.
        // An error here is reported as the invocation's function error.
        if (handler === undefined) {
          const mod = require(bundle);
          handler = mod[exportName] ?? mod.default?.[exportName];
          if (typeof handler !== "function") {
            handler = undefined;
            throw new Error(`Export ${exportName} is not a function in ${bundle}`);
          }
        }
        const deadline = Date.now() + timeoutMs;
        parentPort.postMessage({ requestId, initialized: true });
        const context = {
          awsRequestId: requestId,
          functionName,
          // A numeric qualifier and $LATEST.PUBLISHED name a version, and the
          // runner reports them as the executed version. An alias names no
          // version the shim knows, so it is reported as $LATEST.
          functionVersion:
            qualifier === "$LATEST.PUBLISHED" || /^\d+$/.test(qualifier ?? "")
              ? qualifier
              : "$LATEST",
          invokedFunctionArn:
            `arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${functionName}` +
            (qualifier ? `:${qualifier}` : ""),
          memoryLimitInMB: "128",
          logGroupName: `/aws/lambda/${functionName}`,
          logStreamName: requestId,
          callbackWaitsForEmptyEventLoop: false,
          getRemainingTimeInMillis: () => Math.max(0, deadline - Date.now()),
        };
        // Lambda sets tenantId only for an invocation that names a tenant.
        if (tenantId !== undefined) context.tenantId = tenantId;
        const result = await callHandler(handler, parseEvent(rawEvent), context);
        // Lambda serialises the result with JSON.stringify, in the runtime.
        // So the worker does it here and sends the text. postMessage would
        // copy the object first, which drops class prototypes and their
        // toJSON methods, and fails on values JSON allows, such as functions.
        // A result JSON cannot represent, such as a BigInt, throws here, and
        // is reported below as the invocation's function error. A result
        // JSON has no text for, such as a function or a toJSON() that returns
        // undefined, gives undefined, and is sent as null, as an undefined
        // result is.
        const text = JSON.stringify(result) ?? "null";
        parentPort.postMessage({ requestId, ok: true, text });
      } catch (err) {
        parentPort.postMessage({ requestId, ok: false, error: serializeError(err) });
      }
    },
  );
}

function parseEvent(raw) {
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

// Supports async handlers and callback-style handlers.
function callHandler(handler, event, context) {
  return new Promise((resolve, reject) => {
    const callback = (err, result) => (err ? reject(err) : resolve(result));
    try {
      const ret = handler(event, context, callback);
      if (ret && typeof ret.then === "function") ret.then(resolve, reject);
    } catch (err) {
      reject(err);
    }
  });
}

function serializeError(err) {
  return {
    errorType: err?.name ?? "Error",
    errorMessage: err?.message ?? String(err),
    trace: err?.stack ? String(err.stack).split("\n") : [],
  };
}
