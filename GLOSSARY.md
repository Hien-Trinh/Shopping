# Commerce Ingestion

How merchant product data enters the system, gets categorized, and becomes the current catalog. Serving and recommendations are downstream and out of scope.

## Language

**Merchant**:
A seller who pushes Listings into the system. One Merchant is one storefront in one market and currency.

**Listing**:
One Merchant's offer of one sellable variant (e.g. the medium red t-shirt).
_Avoid_: Product, item, offer

**Merchant product ID**:
The Merchant's own identifier for a Listing. Unique only within that Merchant, and trusted as-is.
_Avoid_: SKU, listing ID

**Listing key**:
Merchant plus Merchant product ID. Globally unique; the identity of a Listing.

**Variant group**:
The Listings a Merchant declares as variants of one another (sizes, colors of the same shirt).

**Tombstone**:
The record a deleted Listing leaves behind so that older changes cannot bring it back.

**Product** _(future)_:
The canonical thing that Listings from many Merchants are offers of (every Merchant's iPhone 17 256GB Black). Not modeled yet.

### Changes

**Change**:
One upsert or delete of one Listing, carrying the Merchant's source version. An upsert is the Listing's complete new state, never a partial edit.
_Avoid_: update, patch, event

**Source version**:
The Merchant's ordering number for a Listing's Changes. The highest one wins, whatever order Changes arrive in.

**Submission**:
One accepted request to the Ingestion API, tracked until every Change in it has an outcome.

**Outcome**:
What happened to one Change: written, already applied, stale (an older source version than stored), conflict (same source version, different content; first one wins), rejected, or failed.

### Categorization

**Category**:
A node in the hierarchical catalog taxonomy (e.g. Apparel > Shirts).
_Avoid_: Bucket

**Primary Category**:
The single Category a Listing is filed under. Every Listing has exactly one.

**Category membership** _(future)_:
An additional Category a Listing appears under for browsing or recommendations. Zero or more per Listing.

**Uncategorized**:
The Primary Category given when the classifier is not confident or not available. Never blocks ingestion.

### Components

**Ingestion API**:
The single entry point Merchants use to submit Changes. Accepts asynchronously.

**Catalog**:
The grouping that owns a Listing from acceptance to export: Landing log, Ingestion workers, Listing Store, Catalog Snapshots and Change Export.
_Avoid_: using Catalog to mean any single one of its parts

**Landing log**:
The raw, append-only record of every Change the Ingestion API accepted. The only place every intermediate version survives.

**Ingestion worker**:
A worker that validates, classifies and applies accepted Changes to the Listing Store. Generic across Categories.

**Listing Store**:
The source of truth for every Listing's current state.

**Catalog Snapshots**:
Periodic copies of the Listing Store. Never read to serve shoppers.
_Avoid_: calling snapshots "every version"

**Change Export**:
The frequent export of changed Listings from the Listing Store to the serving boundary. Only an export: no warehousing or recommendations.
