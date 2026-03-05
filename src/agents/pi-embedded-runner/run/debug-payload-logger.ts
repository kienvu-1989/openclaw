import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import type { StreamFn } from "@mariozechner/pi-agent-core";
import { safeJsonStringify } from "../../../utils/safe-json.js";

function getLogDir(): string {
  const homeDir = os.homedir();
  return path.join(homeDir, ".openclaw", "logs");
}

export function wrapStreamFnWithDebugLogger(
  baseFn: StreamFn,
  runId: string,
): StreamFn {
  return (model, context, options) => {
    // 1. Log what is being sent to the LLM (context.messages)
    const logDir = getLogDir();
    const reqFile = path.join(logDir, `${runId}-request.json`);
    const resFile = path.join(logDir, `${runId}-response.jsonl`);
    const streamFile = path.join(logDir, `${runId}-stream.jsonl`);
    
    // Fire-and-forget logging the request
    fs.mkdir(logDir, { recursive: true })
      .then(() => {
        return fs.writeFile(
          reqFile, 
          safeJsonStringify({ messages: context.messages }) || "{}",
          "utf-8"
        );
      })
      .catch((err) => console.error("Failed to write request log", err));

    // 2. Intercept options.onPayload to log raw LLM backend responses (if supported)
    const originalOnPayload = options?.onPayload;
    const nextOnPayload = (payload: unknown) => {
      const line = safeJsonStringify({ ts: new Date().toISOString(), payload });
      if (line) {
        fs.appendFile(resFile, `${line}\n`, "utf-8").catch(() => {});
      }
      originalOnPayload?.(payload);
    };

    const nextOptions = {
        ...options,
        onPayload: nextOnPayload,
    };

    // 3. Call base stream function
    const maybeStream = baseFn(model, context, nextOptions);

    // 4. Wrap the returned stream to log the Pi-AI events
    const wrapStream = (stream: any) => {
        const originalAsyncIterator = stream[Symbol.asyncIterator].bind(stream);
        stream[Symbol.asyncIterator] = function () {
            const iterator = originalAsyncIterator();
            return {
                async next() {
                    const result = await iterator.next();
                    if (!result.done && result.value) {
                         const line = safeJsonStringify({ ts: new Date().toISOString(), event: result.value });
                         if (line) {
                            fs.appendFile(streamFile, `${line}\n`, "utf-8").catch(() => {});
                         }
                    }
                    return result;
                },
                async return(value?: unknown) {
                    return iterator.return?.(value) ?? { done: true as const, value: undefined };
                },
                async throw(error?: unknown) {
                    return iterator.throw?.(error) ?? { done: true as const, value: undefined };
                },
            };
        };
        return stream;
    };

    if (maybeStream && typeof maybeStream === "object" && "then" in maybeStream) {
        return Promise.resolve(maybeStream).then(wrapStream);
    }
    return wrapStream(maybeStream);
  };
}
