# Step 6a: Taxonomy loader (mini PRD)

Status: approved Oct 3, with the download (decision 1) and the test points. `trim` and `ancestor` stay off the 100% pure-module list. Added after approval, from the downloaded file: Shopify's own `Uncategorized` node is dropped (decision 7). Plan row: [plan-v1.md, PR steps, 6a](../plan-v1.md) ("asks before downloading the Shopify taxonomy"), and Phase 6 ("load and trim the Shopify taxonomy"). Design: [Categorization](../design-commerce-ingestion-pipeline.md) ("Shopify's open-source product taxonomy, cut to 3 levels. Deeper nodes map to their ancestor") and the `taxonomy_version` column. Code layout: `taxonomy.py`, "load Shopify taxonomy, trim to depth 3, version string". Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

Every Listing must get one Primary Category from the catalog taxonomy, but the repo has no taxonomy: `FakeClassifier` invents `Fake > X` categories and the version `fake-1`. 6b's embedding classifier needs the list of Category paths to embed and a real `taxonomy_version` to stamp on every row, and the eval (6d, 6e) needs the same list to label against. Tests and CI must not need the network (plan, "DuckDB's role" and CI).

## Source (checked Oct 3)

- [Shopify/product-taxonomy](https://github.com/Shopify/product-taxonomy), MIT License. Latest stable release `v2026-08` (Aug 24); releases are quarterly CalVer.
- Release asset `categories.en.txt.gz`, 183 KB. The committed `dist/` folder is deprecated and goes away Oct 31, 2026, so the release asset is the source.
- Format: two `#` header lines, the first ending in the version (`# Shopify Product Taxonomy - Categories: 2026-08`), then one line per Category, `{GID padded with spaces} : {Ancestor} > ... > {Name}`. 14,607 Categories, 1 to 8 levels deep; 1,863 are at depth 1 to 3. One top-level node is Shopify's own `Uncategorized` (`gid://shopify/TaxonomyCategory/na`, no children).

## Solution

1. **A one-time download, by hand, with your OK:** `categories.en.txt.gz` and the repo's `LICENSE` from tag `v2026-08`. Neither the raw file nor the download is committed or scripted.
2. `src/catalog/taxonomy.py`:
   - `trim(lines, depth=3)`: the release text in, the trimmed text out. Keeps the header and every Category at depth 1 to 3, drops deeper ones and Shopify's `Uncategorized`, preserves order. Pure.
   - `Taxonomy(version, paths)`: a frozen dataclass. `version` is `shopify-2026-08`; `paths` is the tuple of Category paths (`"Apparel & Accessories > Clothing > Shirts & Tops"`), in file order.
   - `load(path=DEFAULT)`: parses and validates a trimmed file into a `Taxonomy`. `DEFAULT` is the committed file, found through `importlib.resources` so it ships with the package.
   - `ancestor(path, depth=3)`: a deeper path cut to its depth-3 ancestor (the design's "deeper nodes map to their ancestor"), for 6e's labels.
   - `main`: `python -m catalog.taxonomy <raw.txt> > src/catalog/data/shopify-taxonomy.txt`, so the next quarterly bump is one command.
3. `src/catalog/data/shopify-taxonomy.txt` (the trimmed file, committed) and `src/catalog/data/LICENSE-shopify-taxonomy` (MIT asks for its notice to travel with copies).

## User stories

1. As 6b's classifier, I call `load()` and get every Category path to embed and the version to stamp, with no network.
2. As the operator, every row's `taxonomy_version` names the Shopify release it was classified against (`shopify-2026-08`), so a quarterly bump shows which rows 6c must reclassify.
3. As the eval (6e), I map a label at any depth to its depth-3 Category with `ancestor`.
4. As a maintainer, I bump the taxonomy with one download and one command, and the diff of the trimmed file shows what changed.
5. As the operator, a broken or hand-edited taxonomy file stops the process at load with a message naming the line, instead of classifying into a bad tree.

## Failure scenarios

The loader reads one committed, read-only file once at startup, so the process-level angles (crash mid-write, signals, two instances, restarts, full disk) don't apply: it writes nothing and holds no lock. What's left is bad input.

| Scenario | Expected |
|---|---|
| The real `v2026-08` file | Trimmed to depth 3: 1,862 Categories; every path's parent is also present; no duplicates; version `shopify-2026-08` |
| Shopify's own top-level `Uncategorized` node | Dropped by `trim` (decision 7) |
| A Category deeper than 3 levels | Dropped by `trim`; `ancestor` maps it to its depth-3 path |
| A leaf shallower than 3 levels (a depth-2 Category with no children) | Kept: it is a valid Primary Category |
| GID column padded with spaces; a name containing `:` or `&` | Split on the first ` : `, names kept as written |
| Blank lines | Skipped |
| Missing or changed header (no version) | `load` raises `ValueError` naming the file |
| A line without ` : `, an empty name, or a depth over 3 in the trimmed file | `load` raises `ValueError` naming the line number |
| The same path twice | `load` raises `ValueError` |
| A path whose parent isn't in the file | `load` raises `ValueError` (a bad trim or hand edit) |
| A Category named `Uncategorized` in the trimmed file | `load` raises `ValueError`: it would collide with the fallback |
| No Categories at all | `load` raises `ValueError` |
| Taxonomy bump to a later release | A new trimmed file replaces the old one in its own PR; the version string changes, and the existing plan rule reclassifies rows on the old version (6c backfills them) |

## Implementation decisions

1. **Commit the trimmed file, not the raw release, and don't fetch at runtime.** Tests and CI stay offline, the trimmed file is about 180 KB against 2.1 MB for the raw `.txt`, and its diff reviews a bump. The raw file is reproducible from the release tag. **This is the download that needs your OK.**
2. **Every Category at depth 1 to 3 is a classification target, not only depth-3 leaves.** Some branches end above depth 3, and those Listings still need a Primary Category. Whether 6b should prefer deeper answers is 6b's decision.
3. **The version is `shopify-` plus the release date from the header**, matching the `shopify-2025-01` style the tests already use. It comes from the file, so the file and its version can't drift apart.
4. **Paths are the Category identity, not GIDs.** The design and `primary_category` store paths (`Apparel > Shirts`), and 6b embeds path text. The trimmed file keeps the release's line format, GIDs included, so `trim` of it is a no-op and a bump's diff lines up with Shopify's; `load` drops the GIDs. Shopify renaming a Category changes its path, which the version bump already covers.
5. **Strict validation at load, `ValueError` on anything off.** The file is ours and committed, so any problem is a bug to see at startup, not data to tolerate.
6. **No wiring into the worker yet.** The worker keeps `FakeClassifier`; 6b's classifier calls `load()`.
7. **`trim` drops Shopify's `Uncategorized` node.** It means the same as our fallback, which the classifier answers below the threshold. Kept as a target, a confident answer of `Uncategorized` would store `needs_reclassify = false` and look like a real Category.

## Testing decisions

- **Test points (seams), to confirm:**
  1. **`taxonomy.trim` and `ancestor`** (new, red first, `tests/unit/test_taxonomy.py`, small inline text in the release format): depth 4+ dropped, Shopify's `Uncategorized` dropped, shallow leaves kept, order and header kept, padded GIDs, names with `:`; `ancestor` on depths 1, 3 and 6.
  2. **`taxonomy.load`** (new, red first, same file, fixtures written to `tmp_path`): one test per `ValueError` row in the table, and a good file parsed into the expected `Taxonomy`.
  3. **The committed file** (new, same file): `load()` with no argument gives `shopify-2026-08`, 1,862 Categories, no path deeper than 3, and `trim` of the committed file equals itself.
  4. **`main`** (new): run as `python -m catalog.taxonomy` on a small raw fixture, stdout equals `trim`'s output.
- **No network in any test.**
- **Coverage:** `taxonomy.py` stays under the 90% package gate. It isn't added to the 100% pure-module list, since `load` reads a file; say if you want `trim` and `ancestor` there.

## Out of scope

- The classifier, embeddings and threshold (6b).
- Backfill on a taxonomy bump (6c).
- Other languages: `categories.en` only.
- Shopify attributes and GIDs.
- Checking for a newer Shopify release automatically: bumps are done by hand.

## Size

About 50 lines of production code in `taxonomy.py`, about 100 of tests, plus the two data files (the trimmed file is about 1,860 lines) (generated, not reviewed line by line).
