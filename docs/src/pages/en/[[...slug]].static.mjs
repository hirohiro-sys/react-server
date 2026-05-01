import { getPages } from "../../pages.mjs";

// Streaming SSG path source. Yielding lazily lets the exporter start
// rendering before the full path list is materialized; for the docs site
// the win is mostly demonstrative, but it exercises the async-generator
// loader path in the file-router.
export default async function* () {
  yield { path: "/" };
  yield { path: "/404" };

  for (const { category, pages } of getPages("/", "en")) {
    yield { path: `/${category.toLowerCase()}` };
    for (const { langHref: path } of pages) {
      yield { path };
    }
  }
}
