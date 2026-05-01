import { apiReferenceIndex } from "../../../../lib/api-reference.mjs";

// Enumerate the SSG paths for `/api/:slug`. Each yielded entry maps the
// dynamic route param to its value; the file-router expands these into
// concrete paths like `/en/api/core`, `/en/api/client`, …
export default async function* () {
  for (const p of apiReferenceIndex()) {
    yield { slug: p.slug };
  }
}
