import { apiReferenceIndex } from "../../../../lib/api-reference.mjs";

export default async function* () {
  for (const p of apiReferenceIndex()) {
    yield { slug: p.slug };
  }
}
