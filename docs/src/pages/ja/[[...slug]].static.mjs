import { getPages } from "../../pages.mjs";

// See the en/ counterpart — async generator form of the same enumeration.
export default async function* () {
  yield { path: "/ja" };
  yield { path: "/ja/404" };

  for (const { category, pages } of getPages("/", "ja")) {
    yield { path: `/ja/${category.toLowerCase()}` };
    for (const { langHref: path } of pages) {
      yield { path };
    }
  }
}
