# Teacher bake-off: Claude Opus 5.5 vs Jev (step 6j)

Run Oct 6. Spec: [docs/specs/step-6j.md](../docs/specs/step-6j.md). Method: [docs/labeling.md](../docs/labeling.md).

**Decision: Opus 5.5 is the teacher.** It beats Jev by 29 points exact on the 1,020 after adjudication (19 points before), and by 19 points on the 198 after the bias guard. The gate needed 5 and "no worse".

**Correction (Oct 6, step 6l):** the Jev run on the 822 below read 500 description characters, not production's 200. Rerun at 200 it scores 66.9% (not 67.6%), 66.9% on the 1,020 (not 67.5%). The decision is unchanged.

## Results (exact match to the label, level 3)

| Set | Opus raw | Opus adjudicated | Jev raw | Jev after the bias guard |
|---|---|---|---|---|
| 198 (hand-checked) | 85.9% (170) | **95.5%** (189) | 66.7% (132) | **76.8%** (152) |
| 822 (Sonnet labels) | 86.9% (714) | **96.8%** (796) | 67.6% (556) | not run |
| 1,020 | 86.7% (884) | **96.6%** (985) | 67.5% (688) | |

- **Opus** answered every Listing (no `none`, no invalid path), so it is compared with Jev at threshold 0 (always answering): `jev-shortlist50-d200-deeper` on the 198 and `jev-shortlist50-d200-deeper-sonnet` on the 822, both with 6h's settings (shortlist 50, deeper texts, 200-character descriptions). At the production threshold of 0.40 Jev scores 66.7% and 66.5%.
- **Top level:** Opus 96.0% (198) and 96.8% (822); Jev 81.8% and 83.7%. **Two levels:** Opus 95.5% and 95.3%; Jev 78.3% and 79.3%.
- **Adjudicated:** each Listing where Opus differs from the label was sorted against Shopify's full taxonomy into Opus wrong, label wrong, or both fit; only Opus wrong counts against it.
  - 198: 28 differences, 8 Opus wrong, 3 label wrong, 17 both fit.
  - 822: 108 differences, 26 Opus wrong, 5 label wrong, 77 both fit.
- **Bias guard (198):** the labels were drafted by Claude (6e, then checked by the user), so Claude may agree with Claude where Jev is equally right. Every Listing where the label matches Opus and Jev differs (49) was rechecked: Jev counted right on 16 (15 both fit; 1 label wrong, the cat repellent spray 6i already found). The same both-fit credit went to Jev on 4 of the 28 above, where it matched a both-fit Opus answer or another one that fits equally. The guard was not run on the 822 (201 such Listings): Jev would need 29 more points there to close the gap, against the 10 it gained on the 198.

"Both fit" is generous to both models: most are format questions the text can't settle (DVD or digital download, CD or download, print or e-book), sibling categories (Vinyl or Records & LPs) and products that sit in two branches (a gaming laptop).

## What Opus gets wrong

1. **Stopping above level 3 when a level-3 Category fits** (21 of its 34 errors): magazines left at `Media > Magazines & Newspapers` though `Magazines` fits, phone apps at `Computer Software` though `Handheld & PDA Software` fits, toys at `Toys & Games > Toys`. Opus answers above level 3 on 88 Listings, the labels on 52. 6i saw the same with Sonnet.
2. **Subscription boxes:** 4 single-kind boxes (Funko figures, one classic book) put under `Subscription Services`, against the rule (Category of what's in the box).
3. **The wrong sibling or branch** (9): controllers under `Home Game Console Accessories` though `Video Game Controllers` fits (3), a gamepad and a PS5 grip under `Computer Components`, a bouquet under `Gift Giving`, a medical carry case under `Cosmetic & Toiletry Bags`, a model-railroad truck under `Collectibles`, and a guess at the kitchen for a Whirlpool part with no appliance named.

**Its `confident` flag is useful:** exact accuracy (raw) is 94.9% and 93.8% on the confident answers (156 and 634) against 52.4% and 63.3% on the unsure ones (42 and 188). For 6k, label with Opus, then recheck the unsure answers and every answer above level 3.

## Adjudication

### Opus wrong on the 198 (8)

| id | Title | Label | Opus |
|---|---|---|---|
| B007V47DL2 | WarmlyYours Repair/Splice Kit (Twin Conductor) | Hardware > Power & Electrical Supplies > Wire Terminals & Connectors | Hardware > Power & Electrical Supplies |
| B01L9309MK | Prestige Medical Compact Carry Case, Ribbons and Hearts Pink | Business & Industrial > Medical > Medical Supplies | Luggage & Bags > Cosmetic & Toiletry Bags |
| B001817562 | Globe-Weis/Pendaflex 13 Pocket File, Black, (27895BLK) | Office Supplies > Filing & Organization > Folders & Report Covers | Office Supplies > Filing & Organization |
| B09R2LDK24 | Funko Marvel Collector Corp Subscription Box, This is Thor:  | Toys & Games > Toys > Dolls, Playsets & Toy Figures | Product Add-Ons > Subscription Services |
| B087C2PK3Y | CUTEBEE Dollhouse Miniature with Furniture, DIY Wooden Dollh | Toys & Games > Toys > Dolls, Playsets & Toy Figures | Toys & Games > Toys |
| B007CJJRVA | Star Wars Science Force Glove | Toys & Games > Toys > Educational Toys | Toys & Games > Toys |
| B00HM3QANO | CirKa Controller for N64 (Turquoise) | Electronics > Video Game Console Accessories > Video Game Controllers | Electronics > Video Game Console Accessories > Home Game Console Accessories |
| B01LMXTS9W | Retro Fighters Next Gen NES Controller NES/PC/Mac Compatible | Electronics > Video Game Console Accessories > Video Game Controllers | Electronics > Video Game Console Accessories > Home Game Console Accessories |

### Label wrong on the 198 (3; Opus right)

| id | Title | Label | Opus |
|---|---|---|---|
| B005HAEJYG | Orisha Ifa Daily Wisdom | Software > Computer Software | Software > Computer Software > Handheld & PDA Software |
| B01N12DKCJ | Ambience - Nature sounds: relax and sleep | Software > Computer Software | Software > Computer Software > Handheld & PDA Software |
| B07M5YBZP8 | The Nickelodeon Box –The Officially Licensed Nickelodeon Mys | Bundles | Product Add-Ons > Subscription Services |

### Opus wrong on the 822 (26)

| id | Title | Label | Opus |
|---|---|---|---|
| B001EYBHW6 | Whirlpool Part Number 203725: ISOLATOR | Home & Garden > Household Appliance Accessories | Home & Garden > Kitchen & Dining > Kitchen Appliance Accessories |
| B077LDT3BL | INTERNATIONAL(R) 4300 UTILITY TRUCK W/DRILL - ASSEMBLED -- W | Arts & Entertainment > Hobbies & Creative Arts > Model Making | Arts & Entertainment > Hobbies & Creative Arts > Collectibles |
| B00413UZPW | the greatest of nat king cole vols.1 & 2 LP | Media > Music & Sound Recordings > Records & LPs | Media > Music & Sound Recordings |
| B009KLQN2Y | Daniel O'Donnell The Ultimate Concert Collection | Media > Videos > DVDs | Media > Videos |
| B09MYRGH5D | Lint XXV | Media > Music & Sound Recordings > Music CDs | Media > Music & Sound Recordings |
| B075V861WV | Benchmark Bouquets Yellow Roses and White Oriental Lilies, N | Home & Garden > Plants > Flowers | Arts & Entertainment > Party & Celebration > Gift Giving |
| B00Z715L6K | HGTV Magazine All Access    Print Magazine | Media > Magazines & Newspapers > Magazines | Media > Magazines & Newspapers |
| B000EOTRWY | Wine & Spirits | Media > Magazines & Newspapers > Magazines | Media > Magazines & Newspapers |
| B00KRPPEWW | Clean Eating (1-year automatic renewal)-Discontinued ASIN    | Media > Magazines & Newspapers > Magazines | Media > Magazines & Newspapers |
| B00007B1JU | Soap Opera Weekly | Media > Magazines & Newspapers > Magazines | Media > Magazines & Newspapers |
| B00HG1BOYM | Golf Digest All Access    Print Magazine | Media > Magazines & Newspapers > Magazines | Media > Magazines & Newspapers |
| B006SQ90KQ | Army PFT | Software > Computer Software > Handheld & PDA Software | Software > Computer Software |
| B07795R33P | KSHB 41 Kansas City News | Software > Computer Software > Handheld & PDA Software | Software > Computer Software |
| B00IACH2OC | OXYGEN TRACK Millions of Sound Track | Software > Computer Software > Multimedia & Design Software | Software > Computer Software |
| B07GPZYXV3 | Apogee Stream2 v4.7 for Amazon Fire TV | Software > Computer Software > Multimedia & Design Software | Software > Computer Software |
| B09448WMF1 | Unscrambled Words | Software > Video Game Software > Digital Video Games | Software > Computer Software |
| B08MY98R8F | CVPKG Presents Pelican 1400 4 Piece Upgraded Pluck Foam Set. | Luggage & Bags > Luggage Accessories > Dry Box Liners & Inserts | Luggage & Bags > Luggage Accessories |
| B09LVYB4N3 | Funko Doctor Strange Multivers Madness Subscription Box Size | Toys & Games > Toys > Dolls, Playsets & Toy Figures | Product Add-Ons > Subscription Services |
| B08L7LX6PG | The Englishbox- A Classic Literature Subscription Box | Media > Books > Print Books | Product Add-Ons > Subscription Services |
| B09R3MQLXY | Funko Marvel Collector Corp Subscription Box, This is Thor:  | Toys & Games > Toys > Dolls, Playsets & Toy Figures | Product Add-Ons > Subscription Services |
| B08RDQTW4Y | ubrand Gyros Bey Battle Burst Attack Evolution Battling Tops | Toys & Games > Games > Battle Tops | Toys & Games > Toys |
| B08JYTSGVN | SPILAY DIY Dollhouse Miniature with Wooden Furniture,DIY Dol | Toys & Games > Toys > Dolls, Playsets & Toy Figures | Toys & Games > Toys |
| B00EYHD42G | LEGO Minifigures Series 11 Scarecrow Mini Figure | Toys & Games > Toys > Dolls, Playsets & Toy Figures | Toys & Games > Toys |
| B08P2YPK9W | Cybcamo PS5 Controller Grip, Non Slip Comfort Silicone Skin  | Electronics > Video Game Console Accessories > Home Game Console Accessories | Electronics > Electronics Accessories > Computer Components |
| B0BF99KPZG | arVin Wireless Gaming Controller for iPhone Android Gamepad  | Electronics > Video Game Console Accessories > Video Game Controllers | Electronics > Electronics Accessories > Computer Components |
| B01N6EDRYS | Gold Bag PS4 Dualshock Full Controller with 4 Custom Thumbst | Electronics > Video Game Console Accessories > Video Game Controllers | Electronics > Video Game Console Accessories > Home Game Console Accessories |

### Label wrong on the 822 (5; Opus right)

| id | Title | Label | Opus |
|---|---|---|---|
| B00J4V6L1I | Valobra Fougere Hard Soap Pot - 100g | Home & Garden > Bathroom Accessories > Soap Dishes & Holders | Health & Beauty > Personal Care > Shaving & Grooming |
| B07NHYKW9F | Turkish Handmade Jewelry Sea ​​Shell Special Design 925 Ster | Apparel & Accessories > Jewelry > Rings | Apparel & Accessories > Jewelry > Necklaces |
| B00O8O3ZRY | Silhouette Christmas Advent Calendar | Home & Garden > Decor > Seasonal & Holiday Decorations | Software > Digital Goods & Currency |
| B08HQY9LG2 | Eco-Friendly Goodie Box / Choose Your Schedule / Natural Tre | Animals & Pet Supplies > Pet Supplies > Dog Supplies | Product Add-Ons > Subscription Services |
| B08CR7WXCL | IPS Ready Upgraded eXtremeRate Black Soft Touch Replacement  | Electronics > Video Game Console Accessories > Home Game Console Accessories | Electronics > Video Game Console Accessories > Portable Game Console Accessories |

### Bias guard on the 198: Jev counted right although it differs from the label (16)

| id | Title | Label (= Opus) | Jev | Why |
|---|---|---|---|---|
| B09TV9RTNW | Garage-Pro Camshaft Position Sensor Set of 2 Compatible With | Vehicles & Parts > Vehicle Parts & Accessories > Motor Vehicle Parts | Vehicles & Parts > Vehicle Parts & Accessories > Motor Vehicle Electronics | both fit |
| B07S5KJRCL | TUPARTS Mass Air Flow Sensor Meter MAF Compatible for 1998 f | Vehicles & Parts > Vehicle Parts & Accessories > Motor Vehicle Parts | Vehicles & Parts > Vehicle Parts & Accessories > Motor Vehicle Electronics | both fit |
| B07BQJWCG8 | Cute New York Pure Cotton Baby Hooded Towel and Washcloth Se | Home & Garden > Linens & Bedding > Towels | Baby & Toddler > Baby Bathing | both fit |
| B00BMV7OVE | Lunaura Baby Keepsake - Set of 12"Girl" Baby Baseball Key Ch | Arts & Entertainment > Party & Celebration > Party Supplies | Apparel & Accessories > Handbag & Wallet Accessories > Keychains | both fit |
| B01BUDWK7G | THE ESSENTIALS: BEST OF SEASON 1 | Media > Music & Sound Recordings > Music CDs | Media > Music & Sound Recordings > Vinyl | both fit |
| B0001G6V74 | Hodgson Mill All Natural Untoasted Wheat Germ 12 oz Box | Food, Beverages & Tobacco > Food Items > Grains, Rice & Cereal | Food, Beverages & Tobacco > Food Items > Cooking & Baking Ingredients | both fit |
| B019G77YW4 | Punch in the Nuts - Mixed Nut Variety 6 Pack - Trail Mix Swe | Food, Beverages & Tobacco > Food Items > Snack Foods | Food, Beverages & Tobacco > Food Items > Nuts & Seeds | both fit |
| B07CRJK2LP | 3M Steri-Strip Blend Tone Skin Closures 1/2"x4" Non-Reinforc | Health & Beauty > Health Care > First Aid | Business & Industrial > Medical > Medical Supplies | both fit |
| B01G2JPAHU | Green Tea Soap + Konjac Facial Sponge / Combo Pack | Health & Beauty > Personal Care > Cosmetics | Health & Beauty > Personal Care > Personal Care Gift Sets & Kits | both fit |
| B00YBMCAGA | LIWUYOU Merry-Go-Round Music Box Carousel Horse Toy with LED | Home & Garden > Decor > Music Boxes | Toys & Games > Toys > Musical Toys | both fit |
| B00O30XXTS | Robbie Deans: Black, red & gold | Media > Books > E-Books | Media > Books > Print Books | both fit |
| B08Q7X4C3Z | TUMUCUTE 9ft Patio Umbrella Outdoor Umbrella Market Table Um | Home & Garden > Lawn & Garden > Outdoor Living | Home & Garden > Parasols & Rain Umbrellas > Parasols | both fit |
| B004J2HOIG | MLB San Francisco Giants Buster Posey Black Crew Neck Women' | Arts & Entertainment > Hobbies & Creative Arts > Collectibles | Apparel & Accessories > Clothing > Clothing Tops | both fit |
| B00C31VUAS | Boelter Brands St. Louis Rams Mini Pilsner Glass | Home & Garden > Kitchen & Dining > Tableware | Home & Garden > Kitchen & Dining > Barware | both fit |
| B0047YQ8CO | Integy RC Model C23234 Assorted Thickness (1mm to 4mm) Shim  | Toys & Games > Toys > Remote Control Toy Accessories | Hardware > Hardware Accessories > Hardware Fasteners | both fit |
| B096K4MGL7 | Cat Spray for Scratching - Cat Scratch Deterrent for Kittens | Home & Garden > Household Supplies > Pest Control | Animals & Pet Supplies > Pet Supplies > Pet Training Aids | label wrong (6i found it); Opus wrong too |
