# Restaurant lead scoring (Danforth MVP)

A small pipeline that finds independent restaurants in an area, collects the signals that predict whether
online-ordering software (Owner.com) would help them, and ranks them as outbound leads.
Built on Toronto's Danforth (160 restaurants) as a proof of concept for a Canadian go-to-market.

**One file, no dependencies:** `cascade.py` uses only the Python 3 standard library.

## The idea in one paragraph
Lead score = **Online ordering gap** x **3rd party delivery marketplace pain** x **Takeout and delivery propensity** x
**Estimated customer traffic** (each 0-100%, multiplied). A restaurant with no good ordering of its own (gap), listed on many
delivery marketplaces paying commission (pain), with a business model that does a lot of takeout and delivery (propensity), and
enough customer traffic to benefit from and afford the software (traffic), is the best fit.
Estimated customer traffic is Google ratings per year: total ratings divided by years open (DineSafe first inspection), with tenure
capped (8 years by default) and an assumed tenure (4.5 years) when there is no DineSafe match. The weight is a floor (25%) plus a straight
line up to a reference level (300 ratings a year, about the 90th percentile on the Danforth). It is a rough proxy, not sales.
The scoring itself lives in the interactive lead table (all weights, settings and importance are adjustable there);
this repo produces the data behind it (`google_reviews` and `est_years_open` feed the traffic input).

## What it collects
| Stage | Source | Cost |
|---|---|---|
| 1 Discovery | Google Places Text Search (IDs only) | free |
| 2 Core details + filter (chains, closed, few reviews) | Google Places Details | paid |
| 3 Takeout / delivery / dine-in flags, payments | Google Places Details (Atmosphere) | paid |
| 4 Website fingerprint -> **onsite ordering tier** | the restaurant's own site (robots.txt respected) | free |
| Locations (franchise vs independent signal) | Google Places Text Search | paid |
| DineSafe (tenure; recall check) | Toronto open data | free |
| 5 Delivery marketplace listings (Uber Eats, DoorDash, Skip, Fantuan) | Brave Search | paid |

Output: `out/signals.csv`, one row per surviving restaurant (plus `excluded.csv` with the reason each place was dropped, and `usage.json` with the API calls actually made).

## Setup (about 10 minutes)
1. Install Python 3.9+ (`python3 --version`).
2. Create a Google Cloud project, enable **Places API (New)**, create an API key.
3. (Optional, for marketplace listings) get a Brave Search API key.
4. Save keys, **each alone on the first line** of a text file next to `cascade.py`:
   `google_places_key.txt` and `brave_search_key.txt`. (Or set `GOOGLE_PLACES_API_KEY` / `BRAVE_SEARCH_API_KEY`.)
   These files are in `.gitignore`: never commit keys.

## Run it
```bash
python3 cascade.py selftest                       # offline check, run after any edit
python3 cascade.py run --out out --estimate-only  # see the cost first; nothing is spent
python3 cascade.py run --out out --marketplaces --save-html all
```
The script prints a cost estimate and asks before every paid stage (`--yes` skips the question once you have reviewed it).
Useful options: `--min-reviews 30`, `--city Toronto`, `--fantuan all|asian|none`, `--no-locations`, `--dinesafe auto`.
Re-run one stage on its own: `python3 cascade.py marketplaces --out out`, `python3 cascade.py dinesafe --out out`.
`--save-html all` keeps each site's HTML so you can improve the vendor patterns without re-fetching.

## Cost
The Danforth run (160 restaurants) was estimated at about $12-$23 at list price (roughly $0.08-$0.15 per restaurant), and about $0 inside
Google's and Brave's free monthly allowances. The range comes from location counting, which makes 1 to 3 Google searches per restaurant
depending on how many pages of results come back. That run did not record actual call counts. From now on every run prints, and saves
to `<out>/usage.json`, the calls it actually made and their list-price cost, so the estimate can be checked against reality
(Google Cloud Console -> Billing -> Reports shows the same by SKU). Website fetching is free but slow (about 8 minutes for 160 sites).

## Taking it to all of Toronto
What works as-is: everything downstream of discovery (filters, website fingerprint, DineSafe join, marketplace search).
What to change first:
1. **Area.** `BBOX` (top of the file) is one rectangle around the Danforth. A text search returns at most 60 results per
   query per rectangle, so for a city you must **tile**: split the city into a grid of small rectangles (a few hundred
   metres each in dense areas), run stage 1 per tile and de-duplicate by place ID. Tiling is not built yet; it is a small change to `stage1()`.
2. **Recall.** On the Danforth, discovery found about 55% of DineSafe's active establishments (many are not typed as restaurants,
   or are not on Google as one). More queries and tiles raise it; check `dinesafe_coverage.csv` each run.
3. **Cost.** Scale the per-restaurant figure above and check the printed estimate before approving. Run in batches (`--max-stage3`).
4. **Chains.** `chains.txt` is the exclusion list (one name per line); extend it for new neighbourhoods.
5. **Vendors.** Ordering vendors are one regex each in `VENDORS` (group decides the tier). Check `order_cta_links` and
   `ordering_link_samples` on rows in tiers 4, 8 and 9 for vendors worth adding. Run `selftest` after edits.

## Ordering tier (the "online ordering gap" input)
First match wins: `_NO_WEBSITE`, `_SOCIAL_ONLY`, `_UNCHECKED`, 1 Owner customer, 2 POS online ordering, 3 other known ordering vendor,
4 order button to an unknown destination, 5 delivery-app storefront, 6 only links out to delivery apps, 7 call to order only,
8 order text but no link, 9 nothing detected, 99 JavaScript-only page (unreadable). Numbers are labels, not a ranking.

## Known limits
- Only the homepage is read; ordering buried deeper, or in a JavaScript-rendered page, can be missed (tier 99, or a false 8/9).
  The lead table lets a person verify and correct a tier.
- Marketplace listings come from search, so a miss can mean the search did not surface it.
- The takeout propensity is a rule-based prediction (service style x cuisine), not observed sales.
- Scores are untested against real conversion; the point of a pilot would be to calibrate the weights.

## Data and terms
Google limits storing Places content: keep `place_id` long-term and re-pull the rest. Review Google's and Brave's terms before
using this beyond a prototype. This repo deliberately contains no data and no keys.
