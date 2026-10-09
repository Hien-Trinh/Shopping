# Step 6h: Category text from Shopify's deeper names (mini PRD)

Status: approved Oct 4, with the test points, the download, committing the `.gz` (question 1) and the Jev rerun (question 2). You chose the source on Oct 4: Shopify's deeper Category names, not LLM-written definitions. Plan row: [plan-v1.md, PR steps, 6h](../plan-v1.md). It follows [step-6g.md](step-6g.md), whose result ([eval/recall.md](../../eval/recall.md)) is that 60 of Jev's 90 wrong answers were shortlist misses. It also follows the classification-methods research (Oct 4), whose step 3 is to describe each Category instead of embedding its bare path. Design: [Categorization](../design-commerce-ingestion-pipeline.md) ("Shopify's open-source product taxonomy, cut to 3 levels. Deeper nodes map to their ancestor"). Builds on [step-6a.md](step-6a.md) (`taxonomy`, `ancestor`), [step-6b.md](step-6b.md) (`EmbeddingClassifier`) and [step-6e.md](step-6e.md) (`evaluate`). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

The shortlist embeds each of the 1,862 Categories as its bare path, for example `Apparel & Accessories > Clothing > Activewear`. A Listing titled "Women's high-waist yoga leggings" has to land near that path with no other help. The right Category is in the top 50 for only 69.7% of Listings, and that caps any chooser.

Shopify's full release names about 12,700 Categories below our 3 levels, for example `Activewear > Activewear Pants > Leggings`. Those names are close to how products are titled, and they're free, exact and MIT-licensed. The design already says deeper nodes map to their ancestor; today that only happens when the taxonomy is trimmed, not when the shortlist is built.

## Source (checked Oct 4)

- Release asset `categories.en.txt.gz` from tag `v2026-08` of [Shopify/product-taxonomy](https://github.com/Shopify/product-taxonomy): 183,429 bytes, about 2.1 MB unzipped. It's the same file 6a trimmed, so the version matches.
- Its line format is the one `taxonomy.parse` already reads, but up to 8 levels deep.

## Solution

1. **The download, by hand, with your OK:** `categories.en.txt.gz`, committed as is to `src/catalog/data/shopify-taxonomy-full.txt.gz` (183 KB). This changes 6a's decision 1, which committed only the trimmed file (question 1).
2. `taxonomy.deeper(text, tax) -> dict[str, list[str]]`, a pure function. It maps each 3-level Category to the full paths of its descendants, in file order. It drops Shopify's `Uncategorized`, as `trim` does. It raises if the file's version differs from `tax.version`, or if a deeper node's 3-level ancestor isn't in `tax`. `taxonomy.load_deeper()` reads the committed `.gz` with `gzip`.
3. `EmbeddingClassifier` gains `texts: Sequence[tuple[str, str]] | None`, a list of (text, Category path) pairs. Each text is embedded once. A Category's similarity to a Listing is the **highest** over its texts. `classify` and `top` rank Categories by that score, so a Category appears at most once in a shortlist. The default (`None`) is one pair per path, `(path, path)`, so behaviour is unchanged.
4. Three text recipes in `classify.texts(tax, recipe, deeper)`:
   - `path`: today's, one text per Category.
   - `joined`: one text per Category, its path then the last name of each descendant, for example `Apparel & Accessories > Clothing > Activewear: Activewear Pants, Leggings, Sports Bras, …`. bge-small reads at most 512 tokens, so very broad Categories are cut off. The eval shows whether that matters.
   - `deeper`: the path itself plus every descendant's full path as its own text, each scoring for its 3-level ancestor. That's about 14,600 texts.
5. `evaluate recall` and `evaluate run` gain `--texts {path,joined,deeper}`, default `path`. I run recall for all three. Then, **with your OK on the spend (about $0.02)**, I rerun Jev with a shortlist of 50 on the recipe with the best recall@50, so the PR reports end-to-end accuracy and not just recall. The new result file names the recipe.
6. The workers and the Backfill don't change. Adopting a recipe in the pipeline is a separate step after this one, because it changes `taxonomy_version` and worker start time (see out of scope).

## User stories

1. As you, I see recall at 10, 50 and 200 for each recipe next to today's, so I know whether describing Categories helps before the pipeline changes.
2. As you, I see Jev's accuracy with the best recipe's shortlist next to the 54.0% baseline.
3. As the operator, the deeper names come from the same Shopify release as the trimmed taxonomy, and a mismatch is refused, not silently mixed.

## Failure scenarios

| Scenario | Expected |
|---|---|
| The full file's version header differs from the trimmed file's | ValueError naming both versions |
| A deeper node whose 3-level ancestor isn't in the trimmed taxonomy | ValueError naming the line: the two files disagree |
| Shopify's `Uncategorized` in the full file | Dropped, as `trim` drops it |
| The `.gz` is corrupt or truncated | `gzip`'s error, with the file named; the command exits 1 and writes nothing |
| A 3-level Category with no descendants | `joined` is its path alone; `deeper` has its path alone. It still competes |
| A `joined` text over 512 tokens | Cut by the model; the Category keeps its path at the front, so the path is never lost |
| Two texts of one Category both rank high | Counted once, at its best score: a shortlist of 50 is 50 distinct Categories |
| Two Categories tie | The stable order `top` already uses, so reruns agree |
| The `deeper` recipe embeds about 14,600 texts at start | About 8× today's taxonomy embedding (a few seconds), so about 20–40 s on this Mac. Fine for the eval; the pipeline step must handle it |
| Memory for `deeper` | 14,600 × 384 floats is about 22 MB on top of today's |
| A Jev rerun fails midway (429, network) | The result isn't written (`state.save` writes whole or not at all); rerun. The spend so far is under $0.02 |

## Implementation decisions

1. **Commit the release asset, gzipped.** 183 KB, read with stdlib `gzip`, no new dependency. The trimmed file stays the taxonomy of record; the full file only adds text. `--download`-style scripting stays out, as in 6a.
2. **Highest score per Category, not an average.** A product matches one specific deeper node ("Leggings"); averaging would dilute it with its siblings.
3. **Full paths for deeper texts, not leaf names alone.** It matches how `path` embeds today, and keeps "Pants" under "Activewear" distinct from "Pants" under "Clothing". The research found full paths help larger models and are neutral for tiny ones; bge-small is in between, and the eval doesn't sweep this.
4. **Eval only.** No `taxonomy_version`, worker or Backfill change here; the recipe isn't chosen until the numbers are in.
5. **One `texts` list, not three code paths.** Each recipe is just a different list of pairs fed to the same max-per-Category ranking.

## Testing decisions

- **Test points (seams), to confirm:**
  1. **`taxonomy.deeper`** (new, red first, `tests/integration/test_taxonomy.py`). Small hand-written full-release texts. Covers:
     - descendants grouped under their 3-level ancestor, in file order;
     - `Uncategorized` dropped;
     - a version mismatch raises naming both;
     - an orphan deeper node raises naming its line;
     - a Category with no descendants maps to an empty list.
  2. **`EmbeddingClassifier` with `texts`** (new, red first, `tests/integration/test_classify.py`, with hand-made vectors as the existing tests use). Covers:
     - a Category scores its best text;
     - `top` lists each Category once, ordered by best score;
     - `classify` answers the best Category with that score as confidence;
     - the default gives exactly today's answers.
  3. **`classify.texts` recipes** (new, red first, same file): `path`, `joined` (the path first, then the last names) and `deeper` (each descendant paired with its ancestor) on a tiny taxonomy.
  4. **The `--texts` flag** (new, red first, `tests/integration/test_evaluate.py`): `recall --texts deeper` with a fake embedding gives a different table than `--texts path`, and the heading names the recipe. `run --texts joined` records the recipe in the result.
- One `model`-marked test: `taxonomy.load_deeper()` on the committed file matches the committed trimmed taxonomy's version, and every ancestor exists. It reads only the data file, so it could run in the normal job. It's marked `model` only if it's slow; it should take under a second.
- **Coverage:** new lines covered by test points 1–4; the 90% gate holds.

## Questions

1. **Committing the full release asset** reverses 6a's "neither the raw file nor the download is committed". OK to commit the 183 KB `.gz`?
2. **The Jev rerun** in solution step 5: about 198 calls, about $0.02, with `TYPESAFE_API_KEY` as in 6e. OK?

## Out of scope

- Using a recipe in the workers and the Backfill: a later step. It adds the recipe to `taxonomy_version` (so the Backfill reclassifies everything) and handles the `deeper` start-up cost, probably by caching the text embeddings on disk keyed by model and taxonomy.
- LLM-written definitions, another embedding model, kNN over labelled Listings: the research's other steps.
- Changing 6f's shortlist size.

## Size

About 25 lines in `taxonomy.py`, 35 in `classify.py`, 10 in `evaluate.py`, 110 of tests, plus the committed `.gz` and the new `eval/results` and `eval/recall.md`.
