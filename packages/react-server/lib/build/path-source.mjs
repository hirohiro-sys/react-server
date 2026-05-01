import { createHash } from "node:crypto";

/**
 * Streaming path-source primitives for the static exporter.
 *
 * The exporter consumes paths via a single AsyncIterable<ExportPath>. This
 * file normalizes everything users may pass — strings, descriptors,
 * functions, sync/async iterables, generators — into that uniform stream
 * without ever materializing the path list. With a generator path source,
 * memory cost is O(one path descriptor) regardless of total path count.
 *
 * Back-compat:
 *   - `options.exportPaths` accepts the historical array shape, plus new
 *     async-iterable / generator shapes.
 *   - `configRoot.export` as a *regular* function keeps array-in/array-out
 *     semantics (we materialize for it). As an `async function*` (or sync
 *     generator function) it receives the live AsyncIterable and is
 *     expected to yield ExportPath items lazily — true streaming transform.
 *     The constructor-name check (`AsyncGeneratorFunction` /
 *     `GeneratorFunction`) is the explicit opt-in: if you write
 *     `async function*`, you get streaming.
 */

/**
 * Normalize anything path-source-shaped into AsyncIterable<ExportPath>.
 *
 * Accepts:
 *   - `null` / `undefined` (yields nothing)
 *   - `string` (yields `{ path: string }`)
 *   - descriptor object (yields it as-is if it has `path` or `filename`)
 *   - function returning any of the above (called, result re-normalized)
 *   - sync iterable (Array, Set, generator) of any of the above
 *   - async iterable (async generator, ReadableStream-like) of any of the
 *     above
 *
 * Recursive: arrays-of-functions-returning-arrays etc. are flattened lazily.
 */
export async function* toPathStream(source) {
  if (source == null) return;

  if (typeof source === "string") {
    yield { path: source };
    return;
  }

  if (typeof source === "function") {
    yield* toPathStream(await source());
    return;
  }

  // Async iterables take precedence over sync iterables — some objects
  // implement both (e.g. ReadableStream in newer Node).
  if (typeof source[Symbol.asyncIterator] === "function") {
    for await (const item of source) {
      yield* toPathStream(item);
    }
    return;
  }

  if (typeof source[Symbol.iterator] === "function") {
    for (const item of source) {
      yield* toPathStream(item);
    }
    return;
  }

  if (typeof source === "object" && (source.path || source.filename)) {
    yield source;
    return;
  }

  throw new Error(
    `Invalid export path entry: ${JSON.stringify(source)} — expected string, descriptor object with "path" or "filename", function, or (async) iterable thereof.`
  );
}

/**
 * Detect whether a function is a generator or async generator. Detection is
 * by `constructor.name`, which is well-defined for the language's built-in
 * generator function constructors. Wrappers (e.g. memoization layers) that
 * return ordinary functions will fall through to the array-transform path —
 * users who need streaming should write `async function*` directly.
 */
function isGeneratorFunction(fn) {
  if (typeof fn !== "function") return false;
  const name = fn.constructor?.name;
  return name === "AsyncGeneratorFunction" || name === "GeneratorFunction";
}

/**
 * Compose `options.exportPaths` and `configRoot.export` into a single
 * AsyncIterable<ExportPath>. This is what the exporter consumes.
 *
 * Rules:
 *   - `options.exportPaths` is always normalized via `toPathStream` —
 *     anything goes, including async generators.
 *   - `configRoot.export` of array form is a static prelude that yields
 *     before user paths.
 *   - `configRoot.export` of regular-function form preserves the
 *     historical array-in/array-out contract: we materialize the user
 *     stream into an array, hand it to the function, then re-stream the
 *     return value. This is back-compat — the user opted into array
 *     semantics by writing a non-generator function.
 *   - `configRoot.export` of generator-function form (`async function*`)
 *     receives the live AsyncIterable<ExportPath> and is itself a
 *     streaming transform. No materialization.
 */
export async function* buildPathStream(options, configRoot) {
  const userStream = toPathStream(options.exportPaths);

  if (typeof configRoot.export === "function") {
    if (isGeneratorFunction(configRoot.export)) {
      // Streaming transform. The user yields paths derived from `userStream`
      // (or independent sources) without materializing.
      const transformed = configRoot.export(userStream);
      yield* toPathStream(transformed);
      return;
    }

    // Legacy array-transform contract: materialize, hand over, re-stream.
    // Users with millions of paths should switch to a generator form to
    // skip this materialization.
    const collected = [];
    for await (const p of userStream) collected.push(p);
    const result = await configRoot.export(collected);
    yield* toPathStream(result);
    return;
  }

  if (Array.isArray(configRoot.export)) {
    yield* toPathStream(configRoot.export);
  }
  yield* userStream;
}

/**
 * Validate-as-you-go. Wraps an AsyncIterable<ExportPath> with per-item
 * normalization (string → descriptor) and fail-fast validation. Any item
 * lacking both `path` and `filename` throws immediately, naming the
 * offending item — no count-and-report-at-end pass needed.
 */
export async function* validatedPathStream(stream) {
  for await (const item of stream) {
    const descriptor = typeof item === "string" ? { path: item } : item;
    if (!descriptor || (!descriptor.path && !descriptor.filename)) {
      throw new Error(
        `Export path entry is missing "path" (or "filename"): ${JSON.stringify(item)}`
      );
    }
    yield descriptor;
  }
}

/**
 * Stable JSON serialization. Keys are sorted recursively so semantically
 * equal objects produce identical strings regardless of property
 * insertion order. `undefined` values are omitted (treated as "not set"),
 * matching how the rest of the export pipeline interprets missing
 * descriptor fields.
 *
 * Not a general-purpose stable-stringify — no cycle detection, no Date /
 * Map / Set / RegExp special-casing. Descriptors are plain JSON-shaped
 * objects (the documented contract), so the simple recursion is safe.
 * If a user ever passes a `Headers` instance, it'll serialize as `{}`
 * and dedup that case incorrectly — document plain-object headers as
 * the contract; don't auto-detect, because guessing is worse than a
 * predictable miss.
 */
function stableStringify(value) {
  if (value === undefined) return undefined;
  if (value === null) return "null";
  if (typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) {
    return "[" + value.map((v) => stableStringify(v) ?? "null").join(",") + "]";
  }
  const parts = [];
  for (const k of Object.keys(value).sort()) {
    const sv = stableStringify(value[k]);
    if (sv === undefined) continue;
    parts.push(JSON.stringify(k) + ":" + sv);
  }
  return "{" + parts.join(",") + "}";
}

/**
 * Dedup key for an export descriptor: the *entire descriptor*, stably
 * serialized. Two descriptors are dedup-equivalent only when they are
 * structurally identical — same path, same filename, same headers, same
 * prerender/rsc/outlet/remote/origin/host, everything.
 *
 * Why exact match and not e.g. `filename`-only:
 *
 *   - `filename` collisions across distinct descriptors are a real case:
 *     two descriptors yielding the same output path but with different
 *     `prerender` settings produce different *sidecar* artifacts
 *     (postpone state, prerender cache). Deduping on filename alone
 *     would silently drop one of those renders.
 *   - Headers affect rendered HTML (content-negotiation, locale). Two
 *     descriptors differing only in `accept` headers should both render
 *     even if they share path + filename — the user is responsible for
 *     ensuring distinct filenames if they want both artifacts on disk.
 *
 * The conservative rule — "only skip work if every input is identical" —
 * means dedup never silently changes output. The case it actually catches
 * is the common bug: a generator yielding the same descriptor twice by
 * accident (overlapping CMS pages, doubly-walked manifest, etc.).
 */
function dedupeKey(item) {
  return stableStringify(item);
}

/**
 * Streaming dedup. Drops items whose `dedupeKey` was already emitted.
 *
 * Memory model: a `Set<string>` keyed on a 128-bit SHAKE256 digest of the
 * dedupe key (latin1-encoded for compactness — 16 bytes/key, no encoding
 * expansion). Bounded per-key cost regardless of path length, with a
 * collision probability around 10⁻²⁰ for 10M entries — below hardware
 * bit-flip rates, indistinguishable from exact dedup in practice.
 *
 * Soft cap: past `limit` unique entries we stop deduping and warn rather
 * than dropping anything. Correctness above all: the worst case is "we
 * emit a duplicate write" (the historic behavior), never "we silently
 * skip a unique page." If you hit the cap, your source likely has a bug
 * or you've outgrown a single-build static export — the warning routes
 * you to that conversation rather than failing silently.
 */
export async function* dedupedPathStream(
  stream,
  { limit = 1_000_000, onDuplicate, onCapExceeded } = {}
) {
  const seen = new Set();
  let capWarned = false;
  for await (const item of stream) {
    // SHAKE256 with 16-byte output = 128-bit hash. Native node:crypto, no
    // dep. latin1 keeps the Set key at 16 bytes/char — hex would double
    // it, base64 is 22 chars and slower to encode.
    const key = createHash("shake256", { outputLength: 16 })
      .update(dedupeKey(item))
      .digest("latin1");

    if (seen.has(key)) {
      onDuplicate?.(item);
      continue;
    }
    if (seen.size >= limit) {
      if (!capWarned) {
        capWarned = true;
        onCapExceeded?.(limit);
      }
      // Past the cap we yield without remembering — duplicates from here
      // on will pass through, but no unique page is ever dropped.
      yield item;
      continue;
    }
    seen.add(key);
    yield item;
  }
}
