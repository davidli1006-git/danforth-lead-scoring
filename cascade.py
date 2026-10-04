#!/usr/bin/env python3
"""
Restaurant signal cascade (Danforth MVP)

Finds independent restaurants in an area and collects the signals used to rank them as outbound leads for
online-ordering software. Output is one row per surviving restaurant in <out>/signals.csv.

PIPELINE                                                    (paid = costs money; free = no charge)
  Stage 1    Text Search, IDs only (free)       discover place IDs with several queries inside BBOX.
  Stage 2    Place Details, Enterprise (paid)   name, address, rating, review count, website, phone, opening hours.
                                                Then FILTER (classify): not operational / chain (chains.txt) / too few reviews.
                                                Everything filtered out is listed, with the reason, in excluded.csv.
  Stage 3    Place Details, Enterprise+Atmosphere (paid, survivors only)
                                                takeout / delivery / dine-in flags, payment options, review summary.
  Stage 4    Website fingerprint (free)         fetch the restaurant's OWN site (robots.txt respected) and look for ordering
                                                vendors, "Order online" buttons and their links, marketplace links, reservation
                                                tools, site builder -> onsite_online_ordering_tier (see "Ordering tier" below).
  Locations  Text Search by name over the GTA (paid, survivors only)
                                                how many locations the brand has: 1 / 2-5 / 6+ (franchise vs independent signal).
  DineSafe   Toronto open data (free)           first inspection date = a lower bound on tenure; also a recall check on discovery.
  Stage 5    Delivery-marketplace listings (Brave Search, paid, optional)
                                                is the restaurant listed on Uber Eats / DoorDash / Skip / Fantuan?

COST CHECK: before any paid call the script prints an estimate (calls and dollars) and asks you to confirm
(see confirm_spend). Use --estimate-only to just look, --yes to skip the question after you have reviewed it.

USAGE
  Put your Google key alone on the first line of google_places_key.txt (same folder), or: export GOOGLE_PLACES_API_KEY=...
  python cascade.py run [--out out] [--min-reviews 30] [--max-stage3 N] [--no-locations]
                        [--dinesafe auto|PATH] [--marketplaces] [--fantuan all|asian|none] [--city Toronto]
                        [--save-html none|review|all] [--estimate-only] [--yes]
        --max-stage3 N     optional cap on restaurants sent to the paid stages (default 0 = no cap; a warning is printed if it drops any)
        --save-html        keep the fetched site HTML in out/html/ for offline review: review = only tiers where detection is least
                           certain (4, 8, 9), all = every site (recommended: lets you refine vendor patterns without re-fetching)
  python cascade.py marketplaces --out out [--only mezes,sinaloa] [--limit 10] [--fantuan all|asian|none] [--city Toronto]
                        # Stage 5 on its own, reading out/signals.csv -> out/marketplaces.csv (Brave key in brave_search_key.txt).
                        # Also reports recall against sites that link to a marketplace themselves.
  python cascade.py dinesafe --out out [--dinesafe auto|PATH]   # re-run only the DineSafe join on the cached raw files
  python cascade.py snapshot --out out      # re-pull rating counts; the 2nd+ run also writes velocity_<date>.csv
  python cascade.py selftest                # offline test with mocked responses (run this after editing the file)

OUTPUT FILES (in --out)
  usage.json (calls actually made and their list-price cost, one entry per command; also printed at the end of every run)
  signals.csv  excluded.csv  field_population.json  marketplaces.csv  dinesafe_join.csv  dinesafe_coverage.csv
  raw_stage2.json raw_stage3.json raw_stage4.json (cached API / fetch results)  html/<place_id>.html.gz  snapshot_*.json  velocity_*.csv

ORDERING TIER (signals.csv "onsite_online_ordering_tier", defined next to onsite_online_ordering_tier() below)
  The first matching check wins: _NO_WEBSITE, _SOCIAL_ONLY, _UNCHECKED (blocked/errored), 1 Owner customer, 2 POS ordering,
  3 other known ordering vendor, 5 delivery-app storefront (Uber Eats / DoorDash web shop), 6 only links out to delivery apps,
  4 order button to an unrecognised destination, 99_JS_RENDERED (page is an empty JavaScript shell we cannot read), 7 call to order only, 8 order text but no link, 9 nothing detected.
  Whether the restaurant is LISTED on marketplaces is not part of the tier: use the mkpc_search_* columns (Stage 5) alongside it.

NOTES
- Google's terms limit storing Places content: raw data is cached in out/raw_*.json for the run only. Keep place_id
  long-term. Have counsel read the terms before using this beyond an MVP. Likewise Brave: only derived fields are stored.
- Opening hours (hours_mon..hours_sun, hours_text) are Google's regular hours in the restaurant's LOCAL time: no holidays or
  special closures, and owners can leave them stale.
- Place reviews are returned "sorted by relevance" (max 5) per Google's reference, so review timestamps
  are NOT a velocity signal. Velocity = delta in userRatingCount between snapshots.
- `openingDate` is only populated for FUTURE_OPENING places, so it is not used for tenure.
- Sites that disallow us in robots.txt are not fetched (site_status ROBOTS_DISALLOWED; the reason is in the robots_* columns).
  If only the deep link is blocked we retry the homepage when robots.txt allows it (robots_fallback).
"""
import argparse, csv, difflib, gzip, io, json, math, os, re, sys, time, unicodedata, urllib.request, urllib.error
import urllib.robotparser, zipfile
import html as html_lib
from datetime import date, datetime, timedelta
from urllib.parse import urlencode, urlparse, unquote, urljoin

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = "https://places.googleapis.com/v1"
KEY_FILE = "google_places_key.txt"
BRAVE_KEY_FILE = "brave_search_key.txt"

def load_key(dirs=None, env="GOOGLE_PLACES_API_KEY", fname=KEY_FILE):
    """API key from env var (default GOOGLE_PLACES_API_KEY), else the first line of <fname>
    (looked for next to this script, then in the current folder). The key is never printed."""
    k = os.environ.get(env, "").strip()
    if k: return k
    for d in (dirs or [HERE, os.getcwd()]):
        fn = os.path.join(d, fname)
        if os.path.isfile(fn):
            if os.name == "posix" and os.stat(fn).st_mode & 0o077:
                print(f"note: {fn} is readable by other users; consider: chmod 600 {fname}", file=sys.stderr)
            lines = open(fn, encoding="utf-8").read().strip().splitlines()
            return lines[0].strip().strip("\"'") if lines else ""
    return ""

KEY = load_key()
BRAVE_KEY = load_key(env="BRAVE_SEARCH_API_KEY", fname=BRAVE_KEY_FILE)

# Danforth bbox (Broadview Ave to Coxwell/Woodbine-ish; adjust freely)
BBOX = {"low": {"latitude": 43.6735, "longitude": -79.3620},
        "high": {"latitude": 43.6900, "longitude": -79.3150}}
# Approximate GTA bbox for location counting (adjust freely)
GTA_BBOX = {"low": {"latitude": 43.40, "longitude": -79.95},
            "high": {"latitude": 44.10, "longitude": -78.85}}
QUERIES = ["restaurants", "takeout", "pizza", "greek restaurant", "indian restaurant", "chinese restaurant",
           "japanese sushi restaurant", "middle eastern restaurant", "cafe", "burger", "thai vietnamese restaurant",
           "ethiopian restaurant", "pub food"]

# Field masks decide which Google price tier (SKU) a request is billed at: the most expensive field requested sets the SKU.
# Stage 2 = Enterprise (rating, website, phone, hours); Stage 3 adds Atmosphere fields (takeout, delivery, payments, summaries).
F_STAGE2 = ("id,displayName,formattedAddress,location,businessStatus,primaryType,types,"
            "rating,userRatingCount,priceLevel,priceRange,websiteUri,nationalPhoneNumber,"
            "regularOpeningHours,regularSecondaryOpeningHours,containingPlaces")
F_STAGE3 = ("id,delivery,takeout,dineIn,curbsidePickup,reservable,paymentOptions,reviewSummary,"
            "generativeSummary,editorialSummary,servesLunch,servesDinner,servesBrunch,outdoorSeating,goodForGroups")


def post(url, body, fieldmask):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST", headers={
        "Content-Type": "application/json", "X-Goog-Api-Key": KEY, "X-Goog-FieldMask": fieldmask})
    d = _send(req)
    _bill("search_ids_only" if fieldmask == "places.id,nextPageToken" else "search_enterprise")   # ids-only discovery is free
    return d

def get(url, fieldmask):
    req = urllib.request.Request(url, headers={"X-Goog-Api-Key": KEY, "X-Goog-FieldMask": fieldmask})
    d = _send(req)
    _bill("details_enterprise" if fieldmask == F_STAGE2 else "details_atmosphere" if fieldmask == F_STAGE3 else "details_other")
    return d

def _send(req, tries=3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 503) and i < tries - 1:
                time.sleep(2 ** i); continue
            raise RuntimeError(f"{e.code}: {e.read()[:300]!r}")


# ---------- usage log: how many paid API calls this run actually made ----------
USAGE = {}
def _bill(kind, n=1):
    USAGE[kind] = USAGE.get(kind, 0) + n

def usage_report(out, cmd):
    """Print and save the calls actually made (successful responses only) with their list-price cost, so the pre-run estimate can be
    checked against reality. Saved to <out>/usage.json (one entry per command run). List price: free monthly allowances are not subtracted."""
    if not USAGE: return
    priced = {k: v for k, v in USAGE.items() if k in PRICING}
    cost = {k: round(v * PRICING[k]["per_1k"] / 1000, 2) for k, v in priced.items()}
    total = round(sum(cost.values()), 2)
    print(f"\n=== ACTUAL API USAGE ({cmd}) ===")
    for k, v in sorted(USAGE.items()):
        print(f"  {PRICING[k]['label'] if k in PRICING else k}: {v:,} calls" + (f"  list price ${cost[k]:,.2f}" if k in cost else "  (free or not priced)"))
    print(f"  TOTAL list price ${total:,.2f}  (before free monthly allowances)")
    try:
        os.makedirs(out, exist_ok=True)
        fn = os.path.join(out, "usage.json")
        log = json.load(open(fn)) if os.path.isfile(fn) else {"runs": []}
        log["runs"].append({"at": datetime.now().isoformat(timespec="seconds"), "cmd": cmd, "calls": dict(USAGE),
                            "list_price_usd": cost, "list_price_total_usd": total})
        json.dump(log, open(fn, "w"), indent=2)
        print(f"  saved to {fn}")
    except Exception as e:
        print(f"  (could not save usage.json: {e})")

# ---------- name normalization / chains ----------
def norm_name(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"['`]", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return re.sub(r"^the ", "", s)

def strip_branch(name):
    """'Pizza Pizza - Danforth' / 'Pizza Pizza (Danforth)' -> 'Pizza Pizza'"""
    n = re.sub(r"\s*[\(\[].*?[\)\]]\s*", " ", name or "")
    return re.split(r"\s+[-–—|@]\s+", n)[0].strip()

_CHAINS = None
def load_chains(path=None):
    path = path or os.path.join(HERE, "chains.txt")
    out = set()
    for line in open(path, encoding="utf-8"):
        line = line.split("#")[0].strip()
        if line: out.add(norm_name(line))
    return sorted(out, key=len, reverse=True)

def is_chain(name):
    global _CHAINS
    if _CHAINS is None: _CHAINS = load_chains()
    n = norm_name(strip_branch(name))
    return any(n == c or n.startswith(c + " ") for c in _CHAINS)


# ---------- Stage 1 ----------
def stage1():
    """Discover place IDs: run each query in QUERIES inside BBOX (up to 3 pages of 20 each) and de-duplicate. IDs-only = free."""
    ids = {}
    for q in QUERIES:
        token = None
        for _ in range(3):  # up to 60 results per query
            body = {"textQuery": q, "includedType": "restaurant" if q != "cafe" else "cafe",
                    "locationRestriction": {"rectangle": BBOX}, "pageSize": 20}
            if token: body["pageToken"] = token
            d = post(f"{BASE}/places:searchText", body, "places.id,nextPageToken")
            for p in d.get("places", []): ids[p["id"]] = True
            token = d.get("nextPageToken")
            if not token: break
    return list(ids)

# ---------- Stage 2 ----------
def stage2(ids):
    """Fetch core details (F_STAGE2) for every discovered ID. One paid Place Details call per place."""
    return [get(f"{BASE}/places/{i}", F_STAGE2) for i in ids]

def classify(p, min_reviews):
    """None = keep; otherwise the reason it was excluded: not_operational | chain | low_reviews (written to excluded.csv)."""
    name = (p.get("displayName") or {}).get("text", "")
    if p.get("businessStatus") != "OPERATIONAL": return "not_operational"
    if is_chain(name): return "chain"
    if (p.get("userRatingCount") or 0) < min_reviews: return "low_reviews"
    return None

def keep(p, min_reviews):   # True when classify() finds no reason to exclude
    return classify(p, min_reviews) is None

def hours_summary(p):
    """(hours open per week, days open per week) from regularOpeningHours.periods"""
    periods = (p.get("regularOpeningHours") or {}).get("periods") or []
    if not periods: return None, None
    if len(periods) == 1 and not periods[0].get("close"): return 168.0, 7
    mins, days = 0, set()
    for per in periods:
        o, c = per.get("open"), per.get("close")
        if not o or not c: continue
        days.add(o.get("day"))
        om = o.get("day", 0) * 1440 + o.get("hour", 0) * 60 + o.get("minute", 0)
        cm = c.get("day", 0) * 1440 + c.get("hour", 0) * 60 + c.get("minute", 0)
        if cm <= om: cm += 7 * 1440
        mins += cm - om
    return round(mins / 60, 1), len(days)

_DAYS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]      # Google numbers days 0 = Sunday

def hours_by_day(p):
    """Per-day opening hours from regularOpeningHours.periods, in the restaurant's LOCAL time (Toronto), 24h clock.
    Returns {"hours_mon": "12:00-14:30, 16:00-20:30", ..., "hours_text": "Monday: ... | Tuesday: ..."}.
    'Closed' = no period that day; 'Open 24h'; blank = Google has no hours at all. A shift that runs past midnight
    stays on the day it started (Friday '17:00-02:00'). Regular hours only: holidays/special closures are not included."""
    roh = p.get("regularOpeningHours") or {}
    periods = roh.get("periods") or []
    out = {f"hours_{d}": None for d in ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]}
    out["hours_text"] = None
    wd = roh.get("weekdayDescriptions") or []
    if wd:
        out["hours_text"] = " | ".join(re.sub(r"\s+", " ", w.replace("\u2009", " ").replace("\u202f", " ").replace("\u2013", "-")).strip() for w in wd)
    if not periods: return out
    if len(periods) == 1 and not periods[0].get("close"):
        return {**out, **{k: "Open 24h" for k in out if k.startswith("hours_") and k != "hours_text"}}
    by = {d: [] for d in range(7)}
    for per in periods:
        o, c = per.get("open"), per.get("close")
        if not o or not c: continue
        t = lambda x: f"{x.get('hour', 0):02d}:{x.get('minute', 0):02d}"
        om = o.get("day", 0) * 1440 + o.get("hour", 0) * 60 + o.get("minute", 0)
        cm = c.get("day", 0) * 1440 + c.get("hour", 0) * 60 + c.get("minute", 0)
        if cm <= om: cm += 7 * 1440
        by[o.get("day", 0)].append((om, "Open 24h" if cm - om >= 1440 else f"{t(o)}-{t(c)}"))
    for d in range(7):
        out[f"hours_{_DAYS[d]}"] = ", ".join(x for _, x in sorted(by[d])) or "Closed"
    return out

# ---------- Stage 3 ----------
def stage3(ids):
    """Fetch Atmosphere details (F_STAGE3) for survivors only. Returns {place_id: details}."""
    return {i: get(f"{BASE}/places/{i}", F_STAGE3) for i in ids}


# ---------- Location counting (franchise / multi-unit) ----------
def domain(u):
    try: h = urlparse(u or "").netloc.lower()
    except Exception: return ""
    return re.sub(r"^www\.", "", h)

def _search_gta(name):
    """Yield pages of Text Search results for an exact-ish name across the GTA."""
    token = None
    for _ in range(3):
        body = {"textQuery": name, "locationRestriction": {"rectangle": GTA_BBOX}, "pageSize": 20}
        if token: body["pageToken"] = token
        d = post(f"{BASE}/places:searchText", body, "places.id,places.displayName,places.websiteUri,nextPageToken")
        yield d.get("places", [])
        token = d.get("nextPageToken")
        if not token: break

def count_locations(p, search=None):
    """Distinct Places in the GTA with the same normalized name (and, separately, the same website domain)."""
    search = search or _search_gta
    name = (p.get("displayName") or {}).get("text", "")
    key, dom = norm_name(strip_branch(name)), domain(p.get("websiteUri"))
    by_name, by_dom = {p["id"]}, ({p["id"]} if dom else set())
    for page in search(name):
        hits = [q for q in page if norm_name(strip_branch((q.get("displayName") or {}).get("text", ""))) == key]
        for q in hits:
            by_name.add(q["id"])
            if dom and domain(q.get("websiteUri")) == dom: by_dom.add(q["id"])
        if not hits: break
    return {"locations_by_name": len(by_name), "locations_by_domain": len(by_dom) if dom else None}

def location_summary(loc):
    """Trust domain match when we have one; otherwise fall back to name match (lower confidence)."""
    if not loc: return None, None, None
    if loc.get("locations_by_domain") is not None:
        n, conf = loc["locations_by_domain"], "domain"
    else:
        n, conf = loc.get("locations_by_name"), "name_only"
    bucket = "1" if n <= 1 else "2-5" if n <= 5 else "6+"
    return n, bucket, conf


# ---------- Stage 4: website fingerprint ----------
# Vendor patterns, matched (case-insensitively) against the WHOLE page HTML. Shape: group -> {vendor: regex}.
# The GROUP decides the ordering tier (see TIER_BY_GROUP): owner -> 1, pos -> 2, platform -> 3, delivery_storefront -> 5.
# Add a new vendor by adding one regex to the right group. Evidence for each match is kept in ordering_link_samples / order_cta_links.
# KNOWN GAP (seen on the Danforth run, not added yet): Wix's built-in restaurant ordering ("restaurants-olo-client" in the page source).
VENDORS = {
    "owner": {"owner": r"(?<![a-z0-9-])owner\.com|order\.owner"},           # existing Owner.com customer
    "pos": {"toast": r"toasttab\.com", "square": r"square\.site|squareup\.com/store",
            "clover": r"clover\.com/online-ordering", "lightspeed": r"lightspeedhq|order\.lightspeed|mylightspeed\.app",
            "touchbistro": r"touchbistro|tbdine\.com", "ritual": r"ritual\.co"},
    # Non-POS ordering platforms. Shopify is treated as one (a restaurant selling through a Shopify store): the pattern is
    # broad and also matches merchandise/gift-card stores, so check order_cta_links on those rows.
    "platform": {"chownow": r"chownow", "menufy": r"menufy", "slice": r"slicelife", "calibre": r"bycalibre",
                 "orderonthego": r"orderonthego", "beyondmenu": r"beyondmenu", "olo": r"(?<![a-z0-9-])olo\.com",
                 "popmenu": r"popmenu\.com", "oftendining": r"oftendining\.com",
                 "shopify": r"cdn\.shopify\.com|myshopify",
                 "ambassador": r"(?<![a-z0-9-])(?!www\.)[a-z0-9-]+\.ambassador\.ai(?!/?[\"'][^>]*>\s*</a>)"},
                 # Ambassador: ordering on <name>.ambassador.ai. Needs a restaurant TENANT subdomain (link or widget script); a bare
                 # footer "powered by ambassador.ai" link or an EMPTY <a> (no text) does not count (Mezes, ONO Pizza were false positives).
    # Delivery-app web shops that live on the restaurant's own site (the restaurant's own branded ordering page that the app powers).
    # order.store -> Uber Eats: store paths look like /store/<slug>/<22-char id>, the SAME id as the restaurant's ubereats.com
    #   listing (checked on Sinaloa Factory).
    # order.online -> DoorDash Storefront: paths look like /store/<slug>-<numeric id>/?delivery=true&hideModal=true (Kababia, Papyrus,
    #   Shawarma Royale), a numeric id like DoorDash's own listings. This one is INFERRED (DoorDash's docs do not name the domain):
    #   spot-check order.online links against doordash.com listings on the first full run.
    "delivery_storefront": {"ubereats_webshop": r"(?<![a-z0-9-])order\.store",
                            "doordash_storefront": r"(?<![a-z0-9-])order\.online"},
    # Reserved groups, currently empty (kept so the group names in older notes still resolve).
    "ecommerce": {},
    "ambiguous": {},
}
VENDOR_GROUP = {v: g for g, vs in VENDORS.items() for v in vs}
# Display order for onsite_online_ordering_vendors / onsite_online_ordering_groups: POS first, other known vendors next, delivery-app storefronts last.
GROUP_ORDER = ("pos", "owner", "platform", "ecommerce", "ambiguous", "delivery_storefront")
_grank = lambda g: GROUP_ORDER.index(g) if g in GROUP_ORDER else len(GROUP_ORDER)

SIGNATURES = {
    "ordering": {v: rx for vs in VENDORS.values() for v, rx in vs.items()},
    "marketplace": {"ubereats": r"ubereats\.com", "doordash": r"doordash\.com", "skip": r"skipthedishes\.com",
                    "fantuan": r"fantuan\."},
    "reservation": {"opentable": r"opentable\.", "resy": r"resy\.com", "sevenrooms": r"sevenrooms", "tock": r"exploretock"},
    "giftcard_loyalty": {"toast_gift": r"toasttab\.com.*gift|toast.*gift", "square_gift": r"square.*gift",
                         "fivestars": r"fivestars", "yelp": r"yelp\.com/biz"},
    "builder": {"wix": r"wixstatic|wix\.com", "squarespace": r"squarespace", "wordpress": r"wp-content",
                "godaddy": r"godaddy|secureserver", "weebly": r"weebly"},
    "email_sms": {"mailchimp": r"mailchimp|list-manage", "klaviyo": r"klaviyo", "constantcontact": r"constantcontact"},
}
# Our identity when fetching sites / robots.txt. robots_decide() looks for rules addressed to UA_TOKEN before falling back to "*".
UA = "Mozilla/5.0 (compatible; SignalResearchBot/0.1)"
# every URL-bearing attribute in the page (used by link_samples)
_LINK_RX = re.compile(r"""(?:href|src|data-src|action)=["']([^"']+)["']""", re.I)

UA_TOKEN = "signalresearchbot"

def robots_decide(text, url):
    """Pure robots.txt evaluation (RFC 9309 style: longest matching rule wins, Allow beats Disallow on ties).
    Returns (allowed, rule, group) where rule is e.g. 'Disallow: /toronto/' and group is the User-agent
    group that applied ('signalresearchbot' if one names us, else '*')."""
    groups, cur, in_rules = [], None, False
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if ":" not in line: continue
        k, v = (x.strip() for x in line.split(":", 1)); k = k.lower()
        if k == "user-agent":
            if cur is None or in_rules: cur = {"agents": [], "rules": []}; groups.append(cur); in_rules = False
            cur["agents"].append(v.lower())
        elif k in ("allow", "disallow") and cur is not None:
            in_rules = True
            if v: cur["rules"].append((k, v))
    named = [g for g in groups if any(a and a != "*" and a in UA_TOKEN for a in g["agents"])]
    star = [g for g in groups if "*" in g["agents"]]
    use, label = (named, UA_TOKEN) if named else (star, "*")
    u = urlparse(url); path = (u.path or "/") + (("?" + u.query) if u.query else "")
    best = None  # (length, is_allow, rule_text)
    for g in use:
        for k, pat in g["rules"]:
            rx = re.escape(pat).replace(r"\*", ".*")
            if rx.endswith(r"\$"): rx = rx[:-2] + "$"
            if re.match(rx, path):
                cand = (len(pat), k == "allow", f"{k.capitalize()}: {pat}")
                if best is None or cand[:2] > best[:2]: best = cand
    if best is None or best[1]: return True, (best[2] if best else None), label
    return False, best[2], label

def robots_check(url):
    """Fetch + evaluate robots.txt with OUR user agent (the stdlib parser fetches with its own UA and treats a
    401/403 as 'disallow everything', which is how WAF bot-blocks were being mislabelled as owner rules).
    Returns dict: allowed (bool), reason, http (robots.txt status), rule, group."""
    u = urlparse(url); ru = f"{u.scheme}://{u.netloc}/robots.txt"
    try:
        with urllib.request.urlopen(urllib.request.Request(ru, headers={"User-Agent": UA}), timeout=10) as r:
            text = r.read(500_000).decode("utf-8", "ignore"); code = r.status
    except urllib.error.HTTPError as e:
        code = e.code
        if code in (401, 403):   # keeps previous behaviour (blocked) but labelled: likely a bot wall, not a site rule
            return {"allowed": False, "reason": "robots_fetch_denied_401_403", "http": code, "rule": None, "group": None}
        if code >= 500:
            return {"allowed": False, "reason": "robots_server_error_5xx", "http": code, "rule": None, "group": None}
        return {"allowed": True, "reason": "no_robots_txt", "http": code, "rule": None, "group": None}
    except Exception as e:
        return {"allowed": True, "reason": f"robots_unreadable:{type(e).__name__}", "http": None, "rule": None, "group": None}
    ok, rule, grp = robots_decide(text, url)
    if ok: return {"allowed": True, "reason": "allowed", "http": code, "rule": rule, "group": grp}
    sitewide = rule.split(":", 1)[1].strip() == "/"
    return {"allowed": False, "reason": "rule_disallow_sitewide" if sitewide else "rule_disallow_path",
            "http": code, "rule": rule, "group": grp}

def fetch_site(url):
    """Returns (html, status, robots_info)."""
    rb = None
    try:
        rb = robots_check(url)
        if not rb["allowed"] and rb["reason"] == "rule_disallow_path":
            # the owner blocks only this path (typical of a deep Google-listed link): try the homepage instead,
            # but only if robots.txt allows that too. Sitewide disallows and bot walls are never retried.
            u = urlparse(url); root = f"{u.scheme}://{u.netloc}/"
            if root != url:
                rb2 = robots_check(root)
                if rb2["allowed"]:
                    rb = {**rb2, "fallback": "root_after_path_disallow", "blocked_rule": rb["rule"]}; url = root
        if not rb["allowed"]: return None, "ROBOTS_DISALLOWED", rb
        rb["fetched_url"] = url
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.read(600_000).decode("utf-8", "ignore"), "OK", rb
    except Exception as e:
        return None, f"ERROR:{type(e).__name__}", rb

def link_samples(html, limit=30):
    """Raw outbound/ordering-ish URLs, kept as evidence so vendor patterns can be validated empirically.
    Vendor/marketplace matches come first (up to 20), then generic '...order...' links fill the rest, so
    repeated nav/footer 'order' links can't crowd out the vendor link. Detection itself does NOT use this
    list (it regexes the full HTML); this is evidence only."""
    pats = [rx for g in VENDORS.values() for rx in g.values()] + list(SIGNATURES["marketplace"].values())
    big = re.compile("|".join(pats), re.I)
    vend, other = [], []
    for u in _LINK_RX.findall(html):
        u = u[:300]
        if big.search(u):
            if u not in vend: vend.append(u)
        elif re.search(r"order", u, re.I) and u not in other:
            other.append(u)
    return (vend[:20] + other)[:limit]

# <a href="...">text</a> pairs (used by cta_links)
_ANCHOR_RX = re.compile(r"""<a\b[^>]*?\bhref=["']([^"']+)["'][^>]*>(.*?)</a>""", re.I | re.S)
# Which button/link texts we CAPTURE (broad, so evidence is not lost). The tier uses the stricter _ORDER_BUTTON_RX below,
# which leaves out a bare "Delivery" such as a "Delivery Information" page link.
_CTA_TEXT_RX = re.compile(r"\border\b|pick-?up|take-?(out|away)|\bdelivery\b", re.I)

# social / app-store links are never ordering links
_NOT_ORDERING_HOST = re.compile(r"(^|\.)(instagram|facebook|twitter|x|tiktok|youtube|linkedin|apple|google|goo)\.(com|gl)$|play\.google|apps\.apple", re.I)

def _reg_domain(host):   # "order.example.ca:443" -> "example.ca" (last two labels; good enough for .com/.ca)
    parts = (host or "").lower().split(":")[0].split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else (host or "").lower()

def cta_links(html, base=None, limit=10):
    """Order-ish BUTTON/LINK captured by what it SAYS (anchor text), not by what its URL looks like.
    link_samples() filters on the URL, so it misses links like https://gosnappy.io/owa/snappy/detail/G800 (or any
    vendor link) whose URL has no 'order' in it. `base` = the page URL (resolves relative links
    and tells us which links leave the restaurant's own domain). Returns up to `limit` dicts
    {"text": button text, "url": link, "external": leaves the restaurant's own domain, "kind": ...} where kind is
    "vendor:<name>" (a known ordering vendor) | "external" (unknown third party) | "same_site" (e.g. its own /order page)
    | "marketplace:<name>" (Uber Eats / DoorDash / Skip / Fantuan), sorted in that order."""
    own = _reg_domain(urlparse(base).netloc) if base else None
    seen, out = set(), []
    for href, inner in _ANCHOR_RX.findall(html):
        text = html_lib.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", inner))).strip()
        if not text or len(text) > 60 or not _CTA_TEXT_RX.search(text): continue
        href = html_lib.unescape(href.strip())
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")): continue
        url = urljoin(base, href) if base else href
        if url in seen or _NOT_ORDERING_HOST.search(urlparse(url).netloc): continue
        seen.add(url)
        host = urlparse(url).netloc
        kind = next((f"marketplace:{m}" for m, rx in SIGNATURES["marketplace"].items() if re.search(rx, url, re.I)), None) \
            or next((f"vendor:{v}" for v, rx in SIGNATURES["ordering"].items() if re.search(rx, url, re.I)), None)
        external = bool(host and own and _reg_domain(host) != own)
        out.append({"text": text[:60], "url": url[:300], "external": external,
                    "kind": kind or ("external" if external else "same_site")})
    # a restaurant's OWN ordering comes first: known vendor, then unknown third party, then same-site links;
    # marketplace links ("Order delivery from DoorDash") go last so they can't be mistaken for the main order link
    rank = {"vendor": 0, "external": 1, "same_site": 2, "marketplace": 3}
    out.sort(key=lambda x: rank[x["kind"].split(":")[0]])
    return out[:limit]

# ---- one URL per destination (onsite_online_ordering_url_* / offsite_marketplace_url_*) ----
_ABS_URL_RX = re.compile(r"https?://[^\s\"'<>\\)\]}]+", re.I)
_STATIC_URL_RX = re.compile(r"\.(?:js|css|png|jpe?g|gif|svg|webp|ico|woff2?|ttf|map|mp4)(?:[?#]|$)", re.I)
_ASSET_TAG_RX = re.compile(r"<(?:script|link|img|source)\b[^>]*?\b(?:src|href|srcset)=[\"']([^\"']+)", re.I)

def _norm_html(h):
    """Un-escape JSON-embedded URLs (https:\\/\\/x, \\u0026) so links inside script data are visible too."""
    return html_lib.unescape((h or "").replace("\\/", "/").replace("\\u002F", "/").replace("\\u0026", "&").replace("\\u003d", "="))

def ordering_urls(html, base=None):
    """Pick ONE url per destination from the whole page (anchors AND urls inside embedded JSON), ignoring script/css/image assets.
    A candidate must have a real path (not a bare vendor homepage / 'Powered by' footer) unless an order-button anchor points at it.
    Among candidates: order-button anchor first, then the deepest path, then first seen.
    -> {"onsite": {pos, pos_vendor, doordash, ubereats, other}, "offsite": {doordash, ubereats, skip, fantuan}}"""
    norm = _norm_html(html)
    assets = {_norm_html(u) for u in _ASSET_TAG_RX.findall(html or "")}
    anchors = {_norm_html(c["url"]) for c in cta_links(html, base, limit=200)}
    urls = []
    for u in _ABS_URL_RX.findall(norm):
        u = u.rstrip(".,;:'\"")
        if u not in urls and u not in assets and not _STATIC_URL_RX.search(u): urls.append(u)
    def best(rx):
        c = [u for u in urls if re.search(rx, u, re.I) and (urlparse(u).path.strip("/") or u in anchors)]
        return min(c, key=lambda u: (u not in anchors, -len(urlparse(u).path))) if c else None   # min() keeps the first among ties
    pos, pos_vendor = None, None
    for v, rx in VENDORS["pos"].items():
        u = best(rx)
        if u and (pos is None or (u in anchors, len(urlparse(u).path)) > (pos in anchors, len(urlparse(pos).path))): pos, pos_vendor = u, v
    if pos is None and base:          # the restaurant's own site is hosted on a POS (e.g. x.square.site): the site itself is the ordering url
        pos_vendor = next((v for v, rx in VENDORS["pos"].items() if re.search(rx, urlparse(base).netloc, re.I)), None)
        pos = base if pos_vendor else None
    if pos is None:                   # Square Online on a custom domain: the page data names its x.square.site subdomain
        m = re.search(r'"subdomain":"([a-z0-9-]+\.square\.site)"', norm, re.I)
        if m: pos, pos_vendor = f"https://{m.group(1)}/", "square"
    plat = "|".join(rx for g in ("owner", "platform") for rx in VENDORS[g].values())
    other = best(plat) or next((c["url"] for c in own_order_links({"cta_links": cta_links(html, base, limit=200)})), None)
    return {"onsite": {"pos": pos, "pos_vendor": pos_vendor, "doordash": best(VENDORS["delivery_storefront"]["doordash_storefront"]),
                       "ubereats": best(VENDORS["delivery_storefront"]["ubereats_webshop"]), "other": other},
            "offsite": {m: next((u for u in urls if re.search(MARKETPLACES[m]["store"], u)), None) for m in ("doordash", "ubereats", "skip", "fantuan")}}

def is_js_shell(html):
    """True when the raw page is an empty app shell: it loads a script bundle but has (almost) no visible body text, so the real
    content (including any order button) only exists after JavaScript runs and we cannot read it."""
    body = re.search(r"<body[^>]*>(.*?)(?:</body>|$)", html or "", re.I | re.S)     # no </body> when the fetch was cut at the size cap
    if not body: return False                                                      # no <body> seen at all: cannot tell
    text = re.sub(r"<(script|style|noscript)\b.*?(?:</\1>|$)|<[^>]+>", " ", body.group(1), flags=re.I | re.S)
    return bool(re.search(r"<script[^>]+src=", html or "", re.I)) and len(re.sub(r"\s+", " ", text).strip()) < 100

def fingerprint(html, base=None):
    """Everything we read from one page's HTML. Keys: one sorted list of matches per SIGNATURES category (ordering, marketplace,
    reservation, giftcard_loyalty, builder, email_sms); ordering_groups (the VENDORS groups hit); order_cta / call_to_order
    (the page says "Order online/now/pickup" / "call to order": text only, no link needed); menu_is_pdf_or_image;
    ordering_link_samples (URLs that look like ordering, see link_samples); cta_links (order buttons by their text, see cta_links)."""
    out = {}
    for cat, sigs in SIGNATURES.items():
        out[cat] = sorted(k for k, rx in sigs.items() if re.search(rx, html, re.I))
    out["ordering"] = sorted(out["ordering"], key=lambda v: (_grank(VENDOR_GROUP[v]), v))      # pos, other known, delivery storefronts
    out["ordering_groups"] = sorted({VENDOR_GROUP[v] for v in out["ordering"]}, key=lambda g: (_grank(g), g))
    out["order_cta"] = bool(re.search(r"order (online|now|pickup|takeout)|start (an )?order", html, re.I))
    out["call_to_order"] = bool(re.search(r"call (us )?(to|for) (order|takeout|pick)", html, re.I))
    out["menu_is_pdf_or_image"] = bool(re.search(r"href=[\"'][^\"']+\.pdf|menu[^\"'>]*\.(jpg|jpeg|png)", html, re.I))
    out["js_shell"] = is_js_shell(html)
    out["urls"] = ordering_urls(html, base)
    out["ordering_link_samples"] = link_samples(html)
    out["cta_links"] = cta_links(html, base)
    return out

# Tiers where the fingerprint is least trustworthy -> keep the page so patterns can be refined offline.
REVIEW_TIERS = {"4_Unknown_Online_Ordering_Vendor", "8_Only_Order_Text_No_Link", "9_No_Ordering_Detected", "99_JS_RENDERED"}

def maybe_save_html(pid, html, tier, out, mode):
    """mode: 'none' | 'review' (only REVIEW_TIERS) | 'all'. Gzipped to <out>/html/<place_id>.html.gz."""
    if not out or mode == "none" or (mode == "review" and tier not in REVIEW_TIERS): return None
    os.makedirs(f"{out}/html", exist_ok=True)
    path = f"{out}/html/{pid}.html.gz"
    with gzip.open(path, "wt", encoding="utf-8") as f: f.write(html)
    return path

def stage4(places, out=None, save_html="review"):
    """Fingerprint each survivor's own website. status: NO_WEBSITE_ON_GOOGLE | SOCIAL_ONLY (Facebook/Instagram/Linktree only) |
    ROBOTS_DISALLOWED | ERROR:<exception> | OK. Only OK pages get a fingerprint. Polite: 0.5 s pause between sites."""
    res = {}
    for p in places:
        url = p.get("websiteUri")
        if not url: res[p["id"]] = {"status": "NO_WEBSITE_ON_GOOGLE"}; continue
        if re.search(r"facebook\.com|instagram\.com|linktr\.ee", url):
            res[p["id"]] = {"status": "SOCIAL_ONLY", "url": url}; continue
        html, status, rb = fetch_site(url)
        r = {"status": status, "url": url, "robots": rb, **(fingerprint(html, (rb or {}).get("fetched_url") or url) if html else {})}
        if html: r["html_saved"] = maybe_save_html(p["id"], html, onsite_online_ordering_tier(r), out, save_html)
        res[p["id"]] = r
        time.sleep(0.5)
    return res


# ---------- Ordering tier ----------
# Plain check-order logic (first match wins). No P0-P3 scheme and no provisional '?' labels: marketplace
# LISTING evidence lives in the mkpc_search_* (Stage 5) columns and is combined with this tier later, in the sheet.
# The numbers are just labels (they match the team's check-order sheet) and do NOT rank leads; the leading underscore marks
# "no website data to classify".
# The checks run in this order (note the storefront and marketplace checks run before the unknown-vendor check):
#   _NO_WEBSITE, _SOCIAL_ONLY, _UNCHECKED, 1 Owner, 2 POS, 3 non-POS known (incl. Shopify), 5 onsite delivery-app
#   storefront, 6 marketplace link-out only, 4 unknown vendor (order button to an unrecognised destination),
#   99_JS_RENDERED (empty JS app shell, nothing else found), 7 call to order, 8 order text but no link, 9 nothing detected.
TIER_NO_WEBSITE, TIER_SOCIAL, TIER_UNCHECKED = "_NO_WEBSITE", "_SOCIAL_ONLY", "_UNCHECKED"
TIER_OWNER, TIER_POS, TIER_NONPOS = "1_Owner_Customer", "2_POS_Online_Ordering", "3_NonPOS_Known_Online_Ordering_Vendor"
TIER_UNKNOWN, TIER_STOREFRONT = "4_Unknown_Online_Ordering_Vendor", "5_Onsite_Delivery_App_Storefront"
TIER_LINKOUT, TIER_CALL = "6_Only_Offsite_Delivery_App_Linkout", "7_Only_Call_to_Order"
TIER_TEXT_ONLY, TIER_NONE = "8_Only_Order_Text_No_Link", "9_No_Ordering_Detected"
TIER_JS = "99_JS_RENDERED"      # raw page is an empty JavaScript shell: the order button (if any) is invisible to us
TIER_BY_GROUP = [("owner", TIER_OWNER), ("pos", TIER_POS), ("platform", TIER_NONPOS), ("delivery_storefront", TIER_STOREFRONT)]
_ORDER_BUTTON_RX = re.compile(r"\border\b|pick-?up|take-?(out|away)", re.I)   # 'Delivery Information' alone is not an order button

def own_order_links(site):
    """Order buttons that go somewhere that is neither a marketplace nor an already-recognised vendor."""
    return [c for c in (site or {}).get("cta_links", []) if c.get("kind") in ("external", "same_site") and _ORDER_BUTTON_RX.search(c.get("text", ""))]

def onsite_online_ordering_tier(site):
    site = site or {}
    st = site.get("status", "")
    if st == "NO_WEBSITE_ON_GOOGLE": return TIER_NO_WEBSITE
    if st == "SOCIAL_ONLY": return TIER_SOCIAL
    if st.startswith(("ROBOTS", "ERROR")) or not st: return TIER_UNCHECKED
    groups = set(site.get("ordering_groups", []))
    for g, tier in TIER_BY_GROUP:
        if g in groups: return tier
    own = own_order_links(site)
    if site.get("marketplace") and not own: return TIER_LINKOUT     # 'only': no own ordering button besides marketplace links
    if own: return TIER_UNKNOWN
    if site.get("js_shell"): return TIER_JS     # nothing positive found AND the page is an unrendered app shell: do not guess "no link"
    if site.get("call_to_order"): return TIER_CALL
    if site.get("order_cta"): return TIER_TEXT_ONLY      # says "Order online/now/pickup" but we found no usable link (JS-only, Wix, '#' anchor)
    return TIER_NONE


# ---------- DineSafe (Toronto open data) ----------
CKAN = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

def ds_download(out):
    """Download EVERY CSV/ZIP resource of the DineSafe package via CKAN into <out>/dinesafe/ and return that folder.
    The open.toronto.ca/dataset/dinesafe page is marked 'Retired' and third parties describe the data as yearly
    files plus a rolling file with different schemas, so this is unverified: if it fails or the resource list looks
    wrong, download the files manually into a folder and pass --dinesafe FOLDER."""
    req = urllib.request.Request(f"{CKAN}/api/3/action/package_show?id=dinesafe", headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r: pkg = json.load(r)
    res = pkg["result"]["resources"]
    print("DineSafe resources listed:", json.dumps([(r.get("name"), r.get("format"), r.get("url")) for r in res]))
    csvs = [r for r in res if (r.get("format") or "").upper() == "CSV"]
    # several CSVs can be snapshots of the same rolling feed: keep only the most recently modified one
    cand = [r for r in res if (r.get("format") or "").upper() == "ZIP"] + sorted(csvs, key=lambda r: r.get("last_modified") or "")[-1:]
    if not cand: sys.exit("No CSV/ZIP resources found (see list above). Download manually and pass --dinesafe FOLDER.")
    d = os.path.join(out, "dinesafe"); os.makedirs(d, exist_ok=True)
    for i, r in enumerate(cand):
        url = r["url"]; ext = ".zip" if url.lower().endswith(".zip") else ".csv"
        dest = os.path.join(d, f"{i:02d}_{re.sub(r'[^A-Za-z0-9]+', '_', r.get('name') or 'res')[:40]}{ext}")
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=300) as resp, open(dest, "wb") as f:
            f.write(resp.read())
        print("downloaded", url, "->", dest)
    return d

def parse_date(s):
    s = (s or "").strip()
    if not s: return None
    if s.isdigit() and len(s) >= 10:
        return datetime.utcfromtimestamp(int(s) / (1000 if len(s) >= 13 else 1)).date()
    for fmt, n in (("%Y-%m-%d", 10), ("%m/%d/%Y", 10), ("%d/%m/%Y", 10), ("%Y/%m/%d", 10)):
        try: return datetime.strptime(s[:n], fmt).date()
        except ValueError: pass
    return None

def _hkey(h): return re.sub(r"[^a-z0-9]", "", (h or "").lower())
def _pick(r, *names):
    for n in names:
        if r.get(n): return r[n]
    return ""
def _float(x):
    try:
        v = float(x); return v if v != 0 else None
    except (TypeError, ValueError): return None

def ds_aggregate(rows):
    """rows: dicts with normalized header keys. Collapse inspection rows to one record per establishment."""
    est = {}
    for r in rows:
        # current feed: estId is a new-style ID; oldEstId carries the historical Establishment ID (blank for newer places)
        eid = _pick(r, "oldestid", "establishmentid", "estid")
        name, addr = _pick(r, "establishmentname", "estname", "name"), _pick(r, "establishmentaddress", "address")
        key = eid or (norm_name(name) + "|" + addr.lower())
        e = est.setdefault(key, {"id": eid, "name": name, "address": addr, "type": "", "status": "",
                                 "lat": None, "lon": None, "first": None, "last": None, "n_insp": set()})
        e["type"] = e["type"] or _pick(r, "establishmenttype", "type")
        if e["lat"] is None:
            e["lat"], e["lon"] = _float(_pick(r, "latitude", "lat")), _float(_pick(r, "longitude", "lon", "lng", "long"))
        d = parse_date(_pick(r, "inspectiondate", "date"))
        if d:
            if e["first"] is None or d < e["first"]: e["first"] = d
            if e["last"] is None or d >= e["last"]:
                e["last"] = d; e["status"] = _pick(r, "establishmentstatus", "inspectionstatus", "status") or e["status"]
            e["n_insp"].add(_pick(r, "inspectionid") or d.isoformat())
    for e in est.values(): e["n_insp"] = len(e["n_insp"])
    # Different file versions may use different establishment IDs for the same place: merge on name + street address.
    merged = {}
    for e in est.values():
        ak = addr_key(e["address"])
        k = (norm_name(strip_branch(e["name"])), ak) if ak else ("raw", e["id"], e["name"], e["address"])
        m = merged.get(k)
        if m is None: merged[k] = e; continue
        firsts = [x for x in (m["first"], e["first"]) if x]; m["first"] = min(firsts) if firsts else None
        if e["last"] and (not m["last"] or e["last"] >= m["last"]): m["last"], m["status"] = e["last"], e["status"]
        m["n_insp"] += e["n_insp"]
        m["type"] = m["type"] or e["type"]
        if m["lat"] is None: m["lat"], m["lon"] = e["lat"], e["lon"]
    return list(merged.values())

def _ds_rows(path):
    raw = open(path, "rb").read()
    blobs = [raw]
    if path.lower().endswith(".zip"):
        z = zipfile.ZipFile(io.BytesIO(raw)); blobs = [z.read(n) for n in z.namelist() if n.lower().endswith(".csv")]
    for b in blobs:
        for r in csv.DictReader(io.StringIO(b.decode("utf-8-sig", errors="replace"))):
            yield {_hkey(k): (v or "").strip() for k, v in r.items() if k}

def ds_load(path):
    """path: one CSV/ZIP file, or a folder of them (all merged)."""
    files = ([os.path.join(path, f) for f in sorted(os.listdir(path)) if f.lower().endswith((".csv", ".zip"))]
             if os.path.isdir(path) else [path])
    if not files: sys.exit(f"No CSV/ZIP files found in {path}")
    return ds_aggregate([r for f in files for r in _ds_rows(f)])

def haversine_m(lat1, lon1, lat2, lon2):
    R, p1, p2 = 6371000, math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

class Grid:
    """Tiny spatial index (~0.002 deg cells) so city-scale joins are not O(n*m)."""
    def __init__(self, items, getll, cell=0.002):
        self.cell, self.g = cell, {}
        for it in items:
            ll = getll(it)
            if ll: self.g.setdefault((int(ll[0] // cell), int(ll[1] // cell)), []).append(it)
    def near(self, lat, lon):
        ci, cj = int(lat // self.cell), int(lon // self.cell)
        return [it for di in (-1, 0, 1) for dj in (-1, 0, 1) for it in self.g.get((ci + di, cj + dj), [])]

def addr_key(s):
    first = (s or "").split(",")[0].lower()
    m = re.search(r"(\d+)[a-z]?\s+(?:[nsew]\.?\s+)?([a-z]+)", first)
    return (m.group(1), m.group(2)) if m else None

def name_sim(a, b):
    a, b = norm_name(strip_branch(a)), norm_name(strip_branch(b))
    if not a or not b: return 0.0
    if a == b: return 1.0
    r = difflib.SequenceMatcher(None, a, b).ratio()
    ta, tb = set(a.split()), set(b.split())
    if ta <= tb or tb <= ta: r = max(r, 0.85)
    return r

def _place_ll(p):
    l = p.get("location") or {}
    return (l["latitude"], l["longitude"]) if "latitude" in l else None

def best_ds_match(p, grid, by_addr, min_score=0.6, radius_m=120):
    name = (p.get("displayName") or {}).get("text", "")
    cands, ll = [], _place_ll(p)
    if ll:
        for e in grid.near(*ll):
            if haversine_m(ll[0], ll[1], e["lat"], e["lon"]) <= radius_m: cands.append((e, "geo"))
    ak = addr_key(p.get("formattedAddress"))
    if ak:
        cands += [(e, "addr") for e in by_addr.get(ak, [])]
    best = None
    for e, how in cands:
        s = name_sim(name, e["name"])
        if best is None or s > best[0]: best = (s, e, how)
    return best if best and best[0] >= min_score else None

def _ds_index(ds):
    grid = Grid(ds, lambda e: (e["lat"], e["lon"]) if e["lat"] is not None else None)
    by_addr = {}
    for e in ds:
        k = addr_key(e["address"])
        if k: by_addr.setdefault(k, []).append(e)
    return grid, by_addr

def ds_enrich(places, ds):
    """Per place: first/last inspection, count, status, est_years_open. est_years_open = whole years (rounded) since the first
    DineSafe inspection: an estimate of how long the place has operated, and a LOWER bound. It is 'censored' when the
    first inspection sits at the start of the dataset window (the place may be much older)."""
    grid, by_addr = _ds_index(ds)
    dmin = min((e["first"] for e in ds if e["first"]), default=None)
    out = {}
    for p in places:
        m = best_ds_match(p, grid, by_addr)
        if not m: out[p["id"]] = {"ds_match_score": None}; continue
        s, e, how = m
        f = e["first"]
        out[p["id"]] = {"ds_match_score": round(s, 2), "ds_match_method": how, "ds_name": e["name"], "ds_type": e["type"],
                        "ds_status": e["status"], "ds_first_inspection": f.isoformat() if f else None,
                        "ds_last_inspection": e["last"].isoformat() if e["last"] else None,
                        "ds_inspection_count": e["n_insp"],
                        "est_years_open": int((date.today() - f).days / 365.25 + 0.5) if f else None,
                        "tenure_censored": bool(f and dmin and f <= dmin + timedelta(days=120))}
    return out

def ds_coverage(ds, p2, surv_ids, bbox=BBOX, types=("restaurant", "take out", "bakery"), max_age_days=550):
    """Discovery-recall check: which active DineSafe food establishments inside the bbox did Google discovery find?"""
    grid = Grid(p2, _place_ll)
    rows, cutoff = [], date.today() - timedelta(days=max_age_days)
    lo, hi = bbox["low"], bbox["high"]
    no_coords, untyped = sum(1 for e in ds if e["lat"] is None), 0
    if no_coords: print(f"note: {no_coords}/{len(ds)} DineSafe establishments have no coordinates and are skipped in the coverage check")
    for e in ds:
        if e["lat"] is None or not (lo["latitude"] <= e["lat"] <= hi["latitude"] and lo["longitude"] <= e["lon"] <= hi["longitude"]): continue
        if e["type"] and not any(t in e["type"].lower() for t in types): continue
        if "closed" in e["status"].lower() or not e["last"] or e["last"] < cutoff: continue
        if not e["type"]: untyped += 1    # current feed has no Establishment Type: kept, so may include non-restaurants
        best = None
        for p in grid.near(e["lat"], e["lon"]):
            ll = _place_ll(p)
            if haversine_m(e["lat"], e["lon"], *ll) > 120: continue
            s = name_sim(e["name"], (p.get("displayName") or {}).get("text", ""))
            if best is None or s > best[0]: best = (s, p)
        if best and best[0] >= 0.6:
            cat = "found_and_survived" if best[1]["id"] in surv_ids else "found_but_filtered"
            matched = best[1]["displayName"]["text"]
        else:
            cat, matched = "NOT_FOUND_BY_GOOGLE_DISCOVERY", ""
        rows.append({"ds_name": e["name"], "ds_address": e["address"], "ds_type": e["type"], "ds_last_inspection": e["last"].isoformat(),
                     "category": cat, "matched_place": matched})
    if untyped: print(f"note: {untyped}/{len(rows)} active establishments in bbox have no Establishment Type; included (may not all be restaurants)")
    return rows

def write_coverage(rows, out):
    fn = f"{out}/dinesafe_coverage.csv"
    if rows:
        with open(fn, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
    n = len(rows) or 1
    cnt = {c: sum(1 for r in rows if r["category"] == c) for c in ("found_and_survived", "found_but_filtered", "NOT_FOUND_BY_GOOGLE_DISCOVERY")}
    found = cnt["found_and_survived"] + cnt["found_but_filtered"]
    print(f"\nDineSafe coverage check ({len(rows)} active establishments in bbox): "
          f"discovery recall ~{round(100 * found / n)}%  {cnt}  -> {fn}")


# ---------- Stage 5: delivery-marketplace listings (Brave Search) ----------
# For each restaurant we search each marketplace's domain and score the candidate STORE pages that come back.
# status: high / medium / low = a candidate matched; not_found = no acceptable match in the search results
# (NOT proof the restaurant is unlisted: search coverage is imperfect); error = the search call failed.
# We keep only derived fields (status, URL, scores), not the raw search results: Brave's standard plan does not
# grant storage rights for results.
BRAVE_URL = "https://api.search.brave.com/res/v1/web/search"
MARKETPLACES = {
    "ubereats": {"domain": "ubereats.com",
                 "store": r"^https?://(?:www\.)?ubereats\.com/(?:[a-z]{2}(?:-[a-z]{2})?/)?store/([^/?#]+)"},
    "doordash": {"domain": "doordash.com",
                 "store": r"^https?://(?:www\.)?doordash\.com/(?:[a-z]{2}(?:-[A-Za-z]{2})?/)?store/([^/?#]+)"},
    "skip": {"domain": "skipthedishes.com",
             "store": r"^https?://(?:www\.)?skipthedishes\.com/(?!city|cities|restaurants|cuisines|grocery|alcohol)([a-z0-9][a-z0-9\-]*)/?$"},
    "fantuan": {"domain": "fantuan.ca",
                "store": r"^https?://[a-z]+\.fantuan\.ca/(?:[a-z]{2}-[A-Z]{2}/)?store/([^/?#]+)"},
}
FANTUAN_TYPES = {"chinese_restaurant", "asian_restaurant", "korean_restaurant", "japanese_restaurant", "sushi_restaurant",
                 "ramen_restaurant", "thai_restaurant", "vietnamese_restaurant"}
OTHER_CITIES = ["vaughan", "hamilton", "richmond hill", "markham", "mississauga", "pickering", "oakville", "brampton",
                "burlington", "ajax", "whitby", "oshawa", "ottawa", "kitchener", "waterloo", "barrie", "vancouver",
                "calgary", "edmonton", "montreal"]
_STRIP_BASE = {"toronto", "on", "ontario", "canada"}
_TITLE_CUT = re.compile(r"\s[-–|【]\s*(?:menu|toronto|delivery|order|vip|\d+%)|\s+(?:delivery|menu)\b|【", re.I)
_STREET_SUFFIX = re.compile(r"\b(ave|avenue|st|street|rd|road|dr|drive|blvd|boulevard|cres|crescent|way|lane|ln)\b")

def brave_search(q, key=None, count=20, country="CA", tries=4):
    key = key or BRAVE_KEY
    if not key: raise RuntimeError("no Brave API key")
    url = BRAVE_URL + "?" + urlencode({"q": q, "count": count, "country": country, "search_lang": "en"})
    req = urllib.request.Request(url, headers={"Accept": "application/json", "X-Subscription-Token": key, "User-Agent": UA})
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r: d = json.load(r)
            _bill("brave")
            time.sleep(0.2)
            return (d.get("web") or {}).get("results", [])
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and i < tries - 1:
                ra = e.headers.get("Retry-After") or ""
                time.sleep(float(ra) if ra.replace(".", "", 1).isdigit() else 2 ** i); continue
            raise RuntimeError(f"Brave HTTP {e.code}")

def clean_listing_title(t):
    t = re.sub(r"\s+", " ", t or "").strip()
    t = re.sub(r"^(?:order|delivery from)\s+", "", t, flags=re.I)
    return _TITLE_CUT.split(t, maxsplit=1)[0].strip(" -–|")

def _split_paren(t):
    m = re.search(r"\(([^)]*)\)", t)
    return re.sub(r"\([^)]*\)", " ", t).strip(), (m.group(1).strip() if m else "")

def slug_name(slug):
    s = re.sub(r"[-_]+", " ", unquote(slug))
    return " ".join(t for t in norm_name(s).split() if not t.isdigit() and not re.fullmatch(r"[0-9a-f]{6,}", t))

def core_name(s, strip):
    toks = norm_name(strip_branch(s)).split()
    return " ".join(t for t in toks if t not in strip) or " ".join(toks)

def location_evidence(addr, title, desc, slug):
    """-> 'address' | 'street' | 'toronto' | 'none' | 'conflict'. title is the RAW result title."""
    ak = addr_key(addr); num, street = ak if ak else (None, None)
    head = norm_name(f"{title} {desc}")                      # listing text only (not the URL slug)
    allt = head + " " + norm_name(re.sub(r"[-_]+", " ", unquote(slug)))
    if any(re.search(rf"\b{c}\b", head) for c in OTHER_CITIES) and "toronto" not in head: return "conflict"
    _, paren = _split_paren(title)
    if paren:
        pn = norm_name(paren)
        m = re.match(r"(\d+)[a-z]?\s", pn + " ")
        if m and num and m.group(1) != num: return "conflict"            # a different street number: another location
        if street and _STREET_SUFFIX.search(pn) and street not in pn: return "conflict"
        if m and num and m.group(1) == num: return "address"
    if num and street and re.search(rf"\b{num}\b\s+(?:[nsew]\s+)?{re.escape(street)}", head): return "address"
    if street and street in allt.split(): return "street"
    if "toronto" in allt.split(): return "toronto"
    return "none"

def score_candidate(name, addr, mk, r):
    """Score one search result against our restaurant. Returns None if it is not a store page or not a plausible match."""
    m = re.search(MARKETPLACES[mk]["store"], r.get("url") or "")
    if not m: return None
    slug = m.group(1)
    title = clean_listing_title(r.get("title"))
    base, _ = _split_paren(title)
    ak = addr_key(addr); strip = _STRIP_BASE | ({ak[1]} if ak else set())
    ours = core_name(name, strip)
    sim = max(name_sim(ours, core_name(base, strip)), name_sim(ours, core_name(slug_name(slug), strip)))
    if sim < 0.75: return None
    ev = location_evidence(addr, r.get("title") or "", r.get("description") or "", slug)   # raw title: "...- Toronto Delivery" carries the city
    if ev == "conflict": return None
    if (sim >= 0.85 and ev == "address") or (sim >= 0.95 and ev == "street"): conf = "high"
    elif sim >= 0.85 and ev in ("street", "toronto"): conf = "medium"
    else: conf = "low"
    return {"conf": conf, "url": r["url"], "sim": round(sim, 2), "evidence": ev, "slug": slug}

def marketplace_query(place, mk, city):
    ak = addr_key(place.get("address"))
    name = strip_branch(place.get("name", "")).rstrip(". ")
    street = ak[1] if ak and ak[1] not in norm_name(name).split() else ""      # don't repeat "Danforth" if the name has it
    return re.sub(r"\s+", " ", f'{name} {street} {city} site:{MARKETPLACES[mk]["domain"]}').strip()

def check_place(place, search, which, city="Toronto"):
    rank = {"high": 3, "medium": 2, "low": 1}
    out = {}
    for mk in which:
        try:
            results = search(marketplace_query(place, mk, city))
        except Exception as e:
            out[mk] = {"status": "error", "error": f"{type(e).__name__}: {str(e)[:80]}"}; continue
        cands = {}
        for r in results:
            c = score_candidate(place.get("name", ""), place.get("address", ""), mk, r)
            if c and (c["url"] not in cands or rank[c["conf"]] > rank[cands[c["url"]]["conf"]]): cands[c["url"]] = c
        if not cands:
            out[mk] = {"status": "not_found", "n": 0}; continue
        best = max(cands.values(), key=lambda c: (rank[c["conf"]], c["sim"]))
        tied = {c["slug"] for c in cands.values() if c["conf"] == best["conf"]}
        out[mk] = {"status": best["conf"], "url": best["url"], "sim": best["sim"], "evidence": best["evidence"],
                   "n": len(cands), "ambiguous": len(tied) > 1}
    return out

def check_marketplaces(rows, search=None, fantuan="all", city="Toronto", progress=True):
    """rows: dicts with place_id, name, address, primary_type. fantuan: 'all' | 'asian' | 'none'."""
    search = search or brave_search
    res = {}
    for i, row in enumerate(rows):
        which = [m for m in MARKETPLACES if m != "fantuan" or fantuan == "all" or (fantuan == "asian" and row.get("primary_type") in FANTUAN_TYPES)]
        r = check_place(row, search, which, city)
        for m in MARKETPLACES: r.setdefault(m, {"status": "not_checked"})
        res[row["place_id"]] = r
        if progress and (i + 1) % 10 == 0: print(f"  marketplaces: {i + 1}/{len(rows)}", flush=True)
    return res

def mk_flat(r):
    out = {}
    for m in MARKETPLACES:
        x = (r or {}).get(m) or {}
        out.update({f"mkpc_search_{m}": x.get("status"), f"mkpc_search_{m}_url": x.get("url"), f"mkpc_search_{m}_evidence": x.get("evidence"),
                    f"mkpc_search_{m}_n": x.get("n")})
    sts = [((r or {}).get(m) or {}).get("status") for m in MARKETPLACES]
    out["mkpc_search_listed_n"] = sum(1 for s in sts if s in ("high", "medium"))
    out["mkpc_search_listed_any_n"] = sum(1 for s in sts if s in ("high", "medium", "low"))
    return out

def mk_report(rows, res):
    import collections
    print("\nMarketplace check (status by marketplace):")
    for m in MARKETPLACES:
        c = collections.Counter((res[r["place_id"]].get(m) or {}).get("status") for r in rows)
        print(f"  {m:9s}", dict(c))
    errs = [(res[r["place_id"]][m].get("error")) for r in rows for m in MARKETPLACES if (res[r["place_id"]].get(m) or {}).get("status") == "error"]
    if errs: print(f"  {len(errs)} search errors; first: {errs[0]}")
    names = lambda r: r.get("offsite_marketplace_names") or r.get("marketplace_links_on_site") or ""     # old runs used the old name
    gt = [r for r in rows if names(r)]
    if gt:   # ground truth: the restaurant's own website links to the marketplace
        print(f"\nGround truth: {len(gt)} restaurants whose own site links to marketplaces (recall of this check):")
        tot = hit = lo = 0
        for m in ("ubereats", "doordash", "skip"):
            exp = [r for r in gt if m in names(r).split(",")]
            st = [(res[r["place_id"]].get(m) or {}).get("status") for r in exp]
            h, l = sum(s in ("high", "medium") for s in st), sum(s == "low" for s in st)
            tot += len(exp); hit += h; lo += l
            print(f"  {m:9s} expected {len(exp):2d} | found high/medium {h:2d} | found low {l:2d} | missed {len(exp) - h - l:2d}")
        if tot: print(f"  overall recall: {round(100 * hit / tot)}% (high/medium), {round(100 * (hit + lo) / tot)}% (incl. low)")

def marketplaces_cmd(args):
    if not BRAVE_KEY:
        sys.exit(f"No Brave key found. Put it alone on the first line of {BRAVE_KEY_FILE} in this folder (or set BRAVE_SEARCH_API_KEY).")
    rows = list(csv.DictReader(open(f"{args.out}/signals.csv")))
    if args.only:
        wanted = [w.strip().lower() for w in args.only.split(",") if w.strip()]
        # whole-word match ("allen" must not match "Challenge")
        rows = [r for r in rows if any(re.search(r"\b" + re.escape(w) + r"\b", r["name"].lower()) for w in wanted)]
    if args.limit: rows = rows[: args.limit]
    print(f"checking {len(rows)} restaurants")
    q = mk_query_count(rows, args.fantuan)
    confirm_spend(args, f"marketplace check, {len(rows)} restaurants", {"brave": (q, q)})
    res = check_marketplaces(rows, fantuan=args.fantuan, city=args.city)
    flat = [{"place_id": r["place_id"], "name": r["name"], "address": r["address"], **mk_flat(res[r["place_id"]])} for r in rows]
    fn = f"{args.out}/marketplaces.csv"
    with open(fn, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(flat[0])); w.writeheader(); w.writerows(flat)
    print("saved", fn)
    mk_report(rows, res)


# ---------- State / reports ----------
def population_report(rows, fields):
    n = len(rows) or 1
    return {f: round(100 * sum(1 for r in rows if r.get(f) not in (None, "", [], {})) / n) for f in fields}

def _ds_source(arg, out):
    return ds_download(out) if arg == "auto" else arg

# ---------- Cost estimate + confirmation ----------
# USD per 1,000 calls and the free calls per month for each paid SKU. Prices read from Google's public pricing page
# (Places API New) and Brave's plan on 2026-10-02; Text Search / Place Details "IDs only" calls are free. Which SKU a
# request lands in follows the most expensive field requested (rating/website/phone/hours = Enterprise; takeout,
# delivery, payment options, summaries = Enterprise + Atmosphere). Re-check these if Google changes prices.
PRICING = {
    "details_enterprise": {"label": "Google Place Details, Enterprise (rating, website, phone, hours)", "per_1k": 20.0, "free": 1000},
    "details_atmosphere": {"label": "Google Place Details, Enterprise+Atmosphere (takeout, delivery, payments)", "per_1k": 25.0, "free": 1000},
    "search_enterprise": {"label": "Google Text Search, Enterprise (location counting)", "per_1k": 35.0, "free": 1000},
    "brave": {"label": "Brave Search (marketplace listings)", "per_1k": 5.0, "free": 1000},      # $5/month credit = 1,000 queries
}
SURVIVOR_RATE = 0.82      # share of discovered places that pass the filters (Danforth: 149 of 182); used only for the up-front estimate

def mk_query_count(rows, fantuan="all"):
    """Number of Brave queries the marketplace check will make (3 per restaurant, +1 for Fantuan when it applies)."""
    return sum(3 + (1 if fantuan == "all" or (fantuan == "asian" and (r.get("primary_type") or r.get("primaryType")) in FANTUAN_TYPES) else 0)
               for r in rows)

def estimate_costs(calls):
    """calls: {sku_key: (low, high)} -> list of rows with gross and after-free-allowance dollars (low, high)."""
    out = []
    for k, (lo, hi) in calls.items():
        pr = PRICING[k]
        gross = lambda n: n * pr["per_1k"] / 1000
        net = lambda n: max(0, n - pr["free"]) * pr["per_1k"] / 1000
        out.append({"key": k, "label": pr["label"], "calls": (lo, hi), "gross": (gross(lo), gross(hi)), "net": (net(lo), net(hi))})
    return out

def _rng(a, b, money=False):
    f = (lambda x: f"${x:,.2f}") if money else (lambda x: f"{x:,.0f}")
    return f(a) if round(a, 2) == round(b, 2) else f"{f(a)} to {f(b)}"

def confirm_spend(args, title, calls, notes=()):
    """Print a cost estimate and ask before spending. Non-interactive runs (e.g. inside Claude Code) stop here unless --yes."""
    rows = estimate_costs(calls)
    print(f"\n=== COST ESTIMATE: {title} ===")
    for r in rows:
        print(f"  {r['label']}\n      calls: {_rng(*r['calls'])}   cost: {_rng(*r['gross'], True)}   "
              f"after free monthly allowance ({PRICING[r['key']]['free']:,} calls): {_rng(*r['net'], True)}")
    glo, ghi = sum(r["gross"][0] for r in rows), sum(r["gross"][1] for r in rows)
    nlo, nhi = sum(r["net"][0] for r in rows), sum(r["net"][1] for r in rows)
    print(f"  TOTAL: {_rng(glo, ghi, True)} before free allowances; {_rng(nlo, nhi, True)} if the free monthly allowances are still unused")
    for n in notes: print("  note:", n)
    print("  (free allowances are per month and shared with any other use of the same Google/Brave account)")
    if getattr(args, "estimate_only", False): print("  --estimate-only: stopping before any paid call."); sys.exit(0)
    if getattr(args, "yes", False): print("  --yes given: continuing without asking."); return
    if not sys.stdin.isatty():
        print("\n  This is not an interactive terminal, so I cannot ask. Show this estimate to the person who owns the budget, "
              "WAIT for their OK, then re-run the same command with --yes added.")
        sys.exit(3)
    if input("\n  Proceed? [y/N] ").strip().lower() not in ("y", "yes"): sys.exit("Stopped before spending anything further.")

def run(args):
    """Full pipeline: stage 1 -> [cost check] -> stage 2 + filter -> [cost check] -> stage 3 -> stage 4 -> locations ->
    DineSafe -> marketplaces (optional) -> signals.csv. The first cost check uses a projected survivor count; the second is exact."""
    if not KEY: sys.exit(f"No API key found. Put it alone on the first line of {KEY_FILE} in this folder "
                         "(or set the GOOGLE_PLACES_API_KEY environment variable).")
    os.makedirs(args.out, exist_ok=True)
    ids = stage1(); print(f"stage1: {len(ids)} places (discovery search is free)")
    proj = round(len(ids) * SURVIVOR_RATE); proj = min(proj, args.max_stage3) if args.max_stage3 else proj
    mkq = round(proj * (mk_query_count([{"primary_type": None}] * 10, args.fantuan) / 10)) if args.marketplaces else 0
    confirm_spend(args, f"whole run, {len(ids)} places found, about {proj} expected to survive the filters",
                  {"details_enterprise": (len(ids), len(ids)), "details_atmosphere": (proj, proj),
                   **({} if args.no_locations else {"search_enterprise": (proj, proj * 3)}),
                   **({"brave": (mkq, mkq)} if args.marketplaces else {})},
                  notes=[f"survivor count is a projection ({SURVIVOR_RATE:.0%} of places found); you are asked again with exact numbers after stage 2",
                         f"website fetches are free but slow (roughly {proj * 3 // 60 + 1} min)"])
    p2 = stage2(ids); json.dump(p2, open(f"{args.out}/raw_stage2.json", "w"))
    reasons = {p["id"]: classify(p, args.min_reviews) for p in p2}
    with open(f"{args.out}/excluded.csv", "w", newline="") as f:      # eyeball this for chain false positives
        w = csv.writer(f); w.writerow(["place_id", "name", "reason"])
        for p in p2:
            if reasons[p["id"]]: w.writerow([p["id"], p["displayName"]["text"], reasons[p["id"]]])
    allsurv = [p for p in p2 if not reasons[p["id"]]]
    surv = allsurv[: args.max_stage3] if args.max_stage3 else allsurv      # 0 = no cap (default)
    print(f"stage2 survivors: {len(surv)} (excluded.csv has the rest)")
    if len(surv) < len(allsurv):
        print(f"  WARNING: --max-stage3 {args.max_stage3} dropped {len(allsurv) - len(surv)} otherwise-qualifying restaurants "
              f"(first {args.max_stage3} in discovery order kept). Use --max-stage3 0 for no cap.")
    n = len(surv)
    mkq = mk_query_count([{"primary_type": p.get("primaryType")} for p in surv], args.fantuan) if args.marketplaces else 0
    confirm_spend(args, f"remaining stages for {n} restaurants (stage 2 is already paid for)",
                  {"details_atmosphere": (n, n),
                   **({} if args.no_locations else {"search_enterprise": (n, n * 3)}),
                   **({"brave": (mkq, mkq)} if args.marketplaces else {})})
    p3 = stage3([p["id"] for p in surv]); json.dump(p3, open(f"{args.out}/raw_stage3.json", "w"))
    sites = stage4(surv, args.out, args.save_html); json.dump(sites, open(f"{args.out}/raw_stage4.json", "w"))
    locs = None if args.no_locations else {p["id"]: count_locations(p) for p in surv}
    ds_out = None
    if args.dinesafe:
        ds = ds_load(_ds_source(args.dinesafe, args.out))
        ds_out = ds_enrich(surv, ds)
        write_coverage(ds_coverage(ds, p2, {p["id"] for p in surv}), args.out)
    mk = None
    if args.marketplaces:
        if not BRAVE_KEY: sys.exit(f"--marketplaces needs a Brave key in {BRAVE_KEY_FILE} (or BRAVE_SEARCH_API_KEY).")
        mk = check_marketplaces([{"place_id": p["id"], "name": p["displayName"]["text"], "address": p.get("formattedAddress"),
                                  "primary_type": p.get("primaryType")} for p in surv], fantuan=args.fantuan, city=args.city)
    build(surv, p3, sites, args.out, locs, ds_out, mk)

def dinesafe_cmd(args):
    p2 = json.load(open(f"{args.out}/raw_stage2.json"))
    surv = [p for p in p2 if keep(p, args.min_reviews)]
    if args.max_stage3: surv = surv[: args.max_stage3]
    ds = ds_load(_ds_source(args.dinesafe or "auto", args.out))
    res = ds_enrich(surv, ds)
    rows = [{"place_id": pid, **v} for pid, v in res.items()]
    keys = sorted({k for r in rows for k in r}, key=lambda k: (k != "place_id", k))
    with open(f"{args.out}/dinesafe_join.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys); w.writeheader(); w.writerows(rows)
    matched = sum(1 for r in rows if r.get("ds_match_score"))
    print(f"matched {matched}/{len(rows)} survivors -> {args.out}/dinesafe_join.csv")
    write_coverage(ds_coverage(ds, p2, {p["id"] for p in surv}), args.out)

# predicted_primary_type: Google often returns primaryType "restaurant" even when `types` says more. Types are sorted into tiers by
# rule (suffix), not by a per-cuisine list, so new Google cuisine types work automatically. Tune the four sets below.
VAGUE_PRIMARY = {"restaurant", ""}
GENERIC_TYPES = {"restaurant", "food", "point_of_interest", "establishment", "store", "food_store", "service", "health", "meal_takeaway",
                 "meal_delivery", "food_delivery", "catering_service", "event_venue", "shipping_service", "health_food_store",
                 "grocery_store", "supermarket", "convenience_store", "confectionery", "candy_store"}
FORMAT_TYPES = {"fast_food_restaurant", "breakfast_restaurant", "brunch_restaurant", "buffet_restaurant", "fine_dining_restaurant",
                "family_restaurant", "dessert_restaurant", "oyster_bar_restaurant", "diner", "cafe", "coffee_shop", "bakery",
                "pastry_shop", "pub", "gastropub", "bar", "bar_and_grill", "sports_bar", "wine_bar", "cocktail_bar", "hookah_bar",
                "juice_shop", "ice_cream_shop", "dessert_shop", "sandwich_shop", "food_court", "tea_house", "night_club"}
DIET_TYPES = {"vegetarian_restaurant", "vegan_restaurant", "halal_restaurant", "kosher_restaurant", "gluten_free_restaurant"}
UMBRELLA_CUISINES = {"european_restaurant", "mediterranean_restaurant", "asian_restaurant", "american_restaurant", "fusion_restaurant",
                     "asian_fusion_restaurant"}   # real cuisines but broad: used only if nothing narrower is listed

def predict_primary_type(primary, types):
    """-> (predicted_primary_type, basis). Keeps Google's primaryType unless it is vague ("restaurant"/blank); then picks, from
    `types` (Google's order = relevance), the first CUISINE type, else the first FORMAT type (pub, cafe, bakery...), else a DIET
    type, else "restaurant". Cuisine = any other *_restaurant (or steak_house) type not claimed by the sets above."""
    if (primary or "") not in VAGUE_PRIMARY: return primary, "google"
    ts = [t.strip() for t in (types.split(",") if isinstance(types, str) else (types or [])) if t and t.strip()]
    cuisine = [t for t in ts if t not in GENERIC_TYPES | FORMAT_TYPES | DIET_TYPES and (t.endswith("_restaurant") or t == "steak_house")]
    narrow = [t for t in cuisine if t not in UMBRELLA_CUISINES]
    for basis, pool in (("cuisine", narrow or cuisine), ("format", [t for t in ts if t in FORMAT_TYPES]),
                        ("diet", [t for t in ts if t in DIET_TYPES])):
        if pool: return pool[0], basis
    return "restaurant", "generic"

# predicted_takeout_delivery_pct: how takeout/delivery-heavy a restaurant likely is, from a format x cuisine lookup. Tune the
# constants below; the lookup table TAKEOUT_GRID holds the final labels.
FINE_PRICE_LEVELS = {"PRICE_LEVEL_EXPENSIVE", "PRICE_LEVEL_VERY_EXPENSIVE"}
FINE_LOWER_CAD, FINE_UPPER_CAD = 30, 50        # fine dining if price_range lower >= 30 AND upper > 50 (Google buckets are coarse)
QSR_LOWER_CAD, QSR_UPPER_CAD = 10, 20          # QSR if price_range lower <= 10 OR upper <= 20
QSR_PRIMARY_TYPES = {"fast_food_restaurant", "cafe", "coffee_shop", "bakery", "sandwich_shop", "meal_takeaway", "meal_delivery"}
# cuisine tier = regex searched in the predicted_primary_type (suffix-style, so new Google cuisines match by keyword)
CUISINE_TIERS = (("high", r"pizza|chicken|wing|burger"),
                 ("medium_high", r"chinese|cantonese|szechuan|dim_sum|middle_eastern|lebanese|shawarma|falafel|kebab|turkish|persian|"
                                 r"indian|pakistani|bangladeshi|sri_lankan|nepalese|mexican|taco|burrito|tex_mex"))
TAKEOUT_GRID = {("QSR", "high"): "1_VeryHigh", ("QSR", "medium_high"): "2_High", ("QSR", "neutral"): "3_MediumHigh",
                ("Casual", "high"): "3_MediumHigh", ("Casual", "medium_high"): "4_Medium", ("Casual", "neutral"): "5_MediumLow",
                ("Fine", "high"): "6_Low", ("Fine", "medium_high"): "6_Low", ("Fine", "neutral"): "6_Low"}

def _price_bounds(pr):
    """priceRange (dict or JSON string) -> (lower, upper) in CAD, either may be None; non-CAD ranges are ignored."""
    if isinstance(pr, str):
        try: pr = json.loads(pr)
        except ValueError: return None, None
    def units(k):
        x = (pr or {}).get(k) or {}
        return float(x["units"]) if x.get("currencyCode") == "CAD" and str(x.get("units", "")).replace(".", "", 1).isdigit() else None
    lo, hi = units("startPrice"), units("endPrice")
    return lo, (hi if hi is not None else lo)       # a lone "$50+" start price counts as the upper bound too

def classify_takeout(predicted_type, types, price_level, price_range, dine_in):
    """-> (predicted_format, predicted_format_rationale, cuisine_takeout_tier, predicted_takeout_delivery_pct).
    Format: Fine dining (price_level EXPENSIVE+, or price_range lower >= 30 and upper > 50 CAD) wins first; else QSR (QSR type,
    dine_in False, or price_range lower <= 10 / upper <= 20); else Casual. Cuisine tier: high | medium_high | neutral. Label = TAKEOUT_GRID[(format, tier)]."""
    ts = {t.strip() for t in (types.split(",") if isinstance(types, str) else (types or [])) if t and t.strip()}
    lo, hi = _price_bounds(price_range)
    why = []
    if price_level in FINE_PRICE_LEVELS: why.append(f"price_level={price_level.replace('PRICE_LEVEL_', '')}")
    if lo is not None and hi is not None and lo >= FINE_LOWER_CAD and hi > FINE_UPPER_CAD:
        why.append(f"price_range ${lo:g}-${hi:g} (lower>={FINE_LOWER_CAD}, upper>{FINE_UPPER_CAD})")
    if why: fmt = "Fine"
    else:
        if predicted_type in QSR_PRIMARY_TYPES: why.append(f"type={predicted_type}")
        if dine_in is False or str(dine_in).lower() == "false": why.append("dine_in=False")
        if lo is not None and lo <= QSR_LOWER_CAD: why.append(f"price_range lower ${lo:g}<={QSR_LOWER_CAD}")
        elif hi is not None and hi <= QSR_UPPER_CAD: why.append(f"price_range upper ${hi:g}<={QSR_UPPER_CAD}")
        fmt = "QSR" if why else "Casual"
        if not why:
            why.append("full-service default" + (f" (price_range ${lo:g}-${hi:g})" if lo is not None else " (no price_range)"))
    tier = next((name for name, rx in CUISINE_TIERS if re.search(rx, predicted_type or "")), "neutral")
    return fmt, "; ".join(why), tier, TAKEOUT_GRID[(fmt, tier)]

def build(surv, p3, sites, out, locs=None, ds=None, mk=None):
    """Write signals.csv (one row per survivor) and field_population.json. Column groups, in order: identity and Google basics ->
    hours -> Stage 3 flags -> locations -> DineSafe (ds_*, tenure) -> website (site_*, robots_*) -> ordering (onsite_online_ordering_tier,
    vendors, onsite urls, offsite_marketplace_*, then mkpc_search_* only when Stage 5 ran) -> other site signals -> evidence."""
    rows = []
    for p in surv:
        a, s = p3.get(p["id"], {}), sites.get(p["id"], {})
        hrs, days = hours_summary(p)
        n_loc, bucket, conf = location_summary((locs or {}).get(p["id"]))
        d = (ds or {}).get(p["id"], {})
        ll = _place_ll(p) or (None, None)
        pp = predict_primary_type(p.get("primaryType"), p.get("types"))
        tk = classify_takeout(pp[0], p.get("types"), p.get("priceLevel"), p.get("priceRange"), a.get("dineIn"))
        rows.append({
            "place_id": p["id"], "name": p["displayName"]["text"], "address": p.get("formattedAddress"),
            "lat": ll[0], "lng": ll[1], "phone": p.get("nationalPhoneNumber"),
            "primary_type": p.get("primaryType"), "types": ",".join(p.get("types", [])) or None,
            "predicted_primary_type": pp[0], "predicted_primary_type_basis": pp[1],
            **dict(zip(("predicted_format", "predicted_format_rationale", "cuisine_takeout_tier", "predicted_takeout_delivery_pct"), tk)),
            "rating": p.get("rating"), "google_reviews": p.get("userRatingCount"),
            "price_level": p.get("priceLevel"), "price_range": json.dumps(p.get("priceRange")) if p.get("priceRange") else None,
            "hours_per_week": hrs, "days_open": days, **hours_by_day(p),      # hours_mon..hours_sun + hours_text
            "secondary_hours": ",".join(sorted({h.get("secondaryHoursType", "") for h in p.get("regularSecondaryOpeningHours", [])})) or None,
            "flag_delivery": a.get("delivery"), "flag_takeout": a.get("takeout"), "flag_dine_in": a.get("dineIn"),
            "flag_reservable": a.get("reservable"),
            "payment_options": json.dumps(a.get("paymentOptions")) if a.get("paymentOptions") else None,
            "review_summary": (a.get("reviewSummary") or {}).get("text", {}).get("text"),
            "locations_n": n_loc, "location_bucket": bucket, "locations_confidence": conf,
            "ds_first_inspection": d.get("ds_first_inspection"), "ds_last_inspection": d.get("ds_last_inspection"),
            "ds_status": d.get("ds_status"), "ds_type": d.get("ds_type"), "ds_match_score": d.get("ds_match_score"),
            "est_years_open": d.get("est_years_open"), "tenure_censored": d.get("tenure_censored"),
            "site_status": s.get("status"), "site_url": s.get("url"),
            "robots_reason": (s.get("robots") or {}).get("reason"),
            "robots_rule": (s.get("robots") or {}).get("rule"),
            "robots_http": (s.get("robots") or {}).get("http"),
            "robots_group": (s.get("robots") or {}).get("group"),
            "robots_fallback": (s.get("robots") or {}).get("fallback"),
            "robots_blocked_rule": (s.get("robots") or {}).get("blocked_rule"),
            "site_fetched_url": (s.get("robots") or {}).get("fetched_url"),
            # ---- ordering block, in reading order: tier -> vendors -> onsite url per destination -> order button -> offsite links -> other site signals -> evidence
            "onsite_online_ordering_tier": onsite_online_ordering_tier(s),        # see "Ordering tier" above
            # own_ordering_vendor / ordering_groups: every known vendor pattern found anywhere on the page (can be several), POS first
            "onsite_online_ordering_vendors": ",".join(s.get("ordering", [])) or None,
            "onsite_online_ordering_groups": ",".join(s.get("ordering_groups", [])) or None,
            # one ordering url per destination that keeps the customer ON the restaurant's own ordering flow (see ordering_urls)
            "onsite_online_ordering_url_pos": ((s.get("urls") or {}).get("onsite") or {}).get("pos"),
            "onsite_online_ordering_vendor_pos": ((s.get("urls") or {}).get("onsite") or {}).get("pos_vendor"),
            "onsite_online_ordering_url_doordash": ((s.get("urls") or {}).get("onsite") or {}).get("doordash"),
            "onsite_online_ordering_url_ubereats": ((s.get("urls") or {}).get("onsite") or {}).get("ubereats"),
            "onsite_online_ordering_url_other": ((s.get("urls") or {}).get("onsite") or {}).get("other"),
            # offsite: store-page links to delivery marketplaces that the restaurant's own site points to (names, then one url each)
            "offsite_marketplace_names": ",".join(s.get("marketplace", [])) or None,
            **{f"offsite_marketplace_url_{m}": ((s.get("urls") or {}).get("offsite") or {}).get(m) for m in ("doordash", "ubereats", "skip", "fantuan")},
            # what a Brave SEARCH found (listing exists on the marketplace), next to the links above so the two can be compared
            **(mk_flat(mk.get(p["id"])) if mk else {}),
            "reservation_vendor": ",".join(s.get("reservation", [])) or None,
            "giftcard_loyalty": ",".join(s.get("giftcard_loyalty", [])) or None,
            "site_builder": ",".join(s.get("builder", [])) or None,
            # evidence (long text), kept last
            "order_cta_external_domains": ",".join(sorted({_reg_domain(urlparse(c["url"]).netloc) for c in s.get("cta_links", []) if c["external"]})) or None,
            "ordering_link_samples": " | ".join(s.get("ordering_link_samples", [])) or None,
            "order_cta_links": " | ".join(f'[{c["kind"]}] {c["text"]} -> {c["url"]}' for c in s.get("cta_links", [])) or None,
        })
    with open(f"{out}/signals.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
    rep = population_report(rows, [k for k in rows[0] if k not in ("place_id", "name", "address")])
    json.dump(rep, open(f"{out}/field_population.json", "w"), indent=2)
    print("\nField population (% of independents with non-empty value):")
    for k, v in sorted(rep.items(), key=lambda x: -x[1]): print(f"  {v:3d}%  {k}")

def snapshot(args):
    rows = list(csv.DictReader(open(f"{args.out}/signals.csv")))
    snap = {r["place_id"]: get(f"{BASE}/places/{r['place_id']}", "id,userRatingCount,rating") for r in rows}
    today = time.strftime("%Y%m%d"); fn = f"{args.out}/snapshot_{today}.json"
    prior = sorted(f for f in os.listdir(args.out) if re.fullmatch(r"snapshot_\d{8}\.json", f) and f != os.path.basename(fn))
    json.dump(snap, open(fn, "w")); print("saved", fn)
    if prior:   # velocity = change in userRatingCount per 30 days since the previous snapshot
        old = json.load(open(f"{args.out}/{prior[-1]}"))
        days = max((datetime.strptime(today, "%Y%m%d") - datetime.strptime(prior[-1][9:17], "%Y%m%d")).days, 1)
        vf = f"{args.out}/velocity_{today}.csv"
        with open(vf, "w", newline="") as f:
            w = csv.writer(f); w.writerow(["place_id", "name", "count_old", "count_new", "days", "reviews_per_30d"])
            for r in rows:
                o, n = (old.get(r["place_id"]) or {}).get("userRatingCount"), (snap.get(r["place_id"]) or {}).get("userRatingCount")
                if o is not None and n is not None: w.writerow([r["place_id"], r["name"], o, n, days, round((n - o) / days * 30, 2)])
        print("saved", vf)


# ---------- selftest ----------
def selftest():
    import tempfile
    # chains
    for n in ("Pizza Pizza - Danforth", "McDonald's", "The Keg Steakhouse + Bar", "Mr. Greek Mediterranean Grill", "Tim Hortons #123", "A&W"):
        assert is_chain(n), n
    for n in ("Pizzeria Libretto", "Coral Cafe", "Greektown Souvlaki", "Allen's"):
        assert not is_chain(n), n
    # predicted_primary_type
    for pt, ty, want in (("restaurant", "restaurant,food,point_of_interest,establishment", "restaurant"),
                         ("restaurant", "restaurant,indian_restaurant,food", "indian_restaurant"),
                         ("restaurant", "latin_american_restaurant,brunch_restaurant,cafe,restaurant", "latin_american_restaurant"),
                         ("restaurant", "restaurant,pub,bar,food", "pub"),
                         ("restaurant", "health_food_store,cafe,bakery,restaurant", "cafe"),
                         ("restaurant", "restaurant,vegetarian_restaurant,juice_shop,food", "juice_shop"),
                         ("restaurant", "restaurant,vegetarian_restaurant,food", "vegetarian_restaurant"),
                         ("restaurant", "restaurant,european_restaurant,french_restaurant", "french_restaurant"),
                         ("restaurant", "restaurant,european_restaurant,bar", "european_restaurant"),
                         ("greek_restaurant", "restaurant,pub", "greek_restaurant"), (None, "restaurant,bar", "bar"), ("restaurant", None, "restaurant")):
        assert predict_primary_type(pt, ty)[0] == want, (ty, predict_primary_type(pt, ty))
    # classify_takeout
    pr = lambda lo, hi: {"startPrice": {"currencyCode": "CAD", "units": str(lo)}, "endPrice": {"currencyCode": "CAD", "units": str(hi)}}
    for args_, want in (
            (("pizza_restaurant", "pizza_restaurant,restaurant", None, pr(10, 20), True), ("QSR", "high", "1_VeryHigh")),
            (("indian_restaurant", "indian_restaurant", None, pr(1, 10), True), ("QSR", "medium_high", "2_High")),
            (("cafe", "cafe,bakery", None, pr(10, 20), True), ("QSR", "neutral", "3_MediumHigh")),
            (("pizza_restaurant", "pizza_restaurant", None, pr(20, 30), True), ("Casual", "high", "3_MediumHigh")),
            (("mexican_restaurant", "mexican_restaurant", None, pr(30, 40), True), ("Casual", "medium_high", "4_Medium")),
            (("greek_restaurant", "greek_restaurant", "PRICE_LEVEL_MODERATE", pr(30, 40), True), ("Casual", "neutral", "5_MediumLow")),
            (("pizza_restaurant", "pizza_restaurant", "PRICE_LEVEL_EXPENSIVE", None, True), ("Fine", "high", "6_Low")),
            (("greek_restaurant", "greek_restaurant", None, pr(30, 60), True), ("Fine", "neutral", "6_Low")),
            (("greek_restaurant", "greek_restaurant", None, pr(20, 60), True), ("Casual", "neutral", "5_MediumLow")),
            (("restaurant", "restaurant,meal_takeaway", None, None, True), ("Casual", "neutral", "5_MediumLow")),
            (("meal_takeaway", "meal_takeaway,restaurant", None, None, True), ("QSR", "neutral", "3_MediumHigh")),
            (("restaurant", "restaurant", None, None, False), ("QSR", "neutral", "3_MediumHigh")),
            (("restaurant", "restaurant", None, None, None), ("Casual", "neutral", "5_MediumLow"))):
        got = classify_takeout(*args_)
        assert (got[0], got[2], got[3]) == want, (args_, got)
    # keep / classify
    p = {"id": "x1", "displayName": {"text": "Test Grill"}, "businessStatus": "OPERATIONAL", "userRatingCount": 200,
         "rating": 4.4, "websiteUri": "https://www.example.com", "formattedAddress": "1234 Danforth Ave, Toronto, ON",
         "location": {"latitude": 43.6800, "longitude": -79.3400}, "primaryType": "greek_restaurant",
         "regularOpeningHours": {"periods": [{"open": {"day": d, "hour": 11}, "close": {"day": d, "hour": 22}} for d in range(1, 7)]}}
    assert keep(p, 30) and classify({**p, "displayName": {"text": "Subway"}}, 30) == "chain"
    assert classify({**p, "userRatingCount": 3}, 30) == "low_reviews"
    assert hours_summary(p) == (66.0, 6), hours_summary(p)
    # cost estimate
    ce = {r["key"]: r for r in estimate_costs({"details_enterprise": (2000, 2000), "search_enterprise": (100, 300)})}
    assert ce["details_enterprise"]["gross"] == (40.0, 40.0) and ce["details_enterprise"]["net"] == (20.0, 20.0), ce
    assert ce["search_enterprise"]["net"] == (0.0, 0.0) and ce["search_enterprise"]["gross"][1] == 10.5, ce
    assert mk_query_count([{"primary_type": "chinese_restaurant"}, {"primary_type": "pizza_restaurant"}], "asian") == 7
    assert mk_query_count([{"primary_type": "x"}] * 10, "all") == 40 and mk_query_count([{"primary_type": "x"}] * 10, "none") == 30
    hb = hours_by_day(p)
    assert hb["hours_mon"] == "11:00-22:00" and hb["hours_sun"] == "Closed" and hb["hours_text"] is None, hb
    hb2 = hours_by_day({"regularOpeningHours": {"periods": [
        {"open": {"day": 2, "hour": 16}, "close": {"day": 2, "hour": 20, "minute": 30}},
        {"open": {"day": 2, "hour": 12}, "close": {"day": 2, "hour": 14, "minute": 30}},
        {"open": {"day": 5, "hour": 17}, "close": {"day": 6, "hour": 2}}],
        "weekdayDescriptions": ["Monday: Closed", "Tuesday: 12:00\u2009\u2013\u20092:30\u202fPM"]}})
    assert hb2["hours_tue"] == "12:00-14:30, 16:00-20:30" and hb2["hours_fri"] == "17:00-02:00" and hb2["hours_sat"] == "Closed", hb2
    assert hb2["hours_text"] == "Monday: Closed | Tuesday: 12:00 - 2:30 PM", hb2["hours_text"]
    assert hours_by_day({"regularOpeningHours": {"periods": [{"open": {"day": 0, "hour": 0}}]}})["hours_wed"] == "Open 24h"
    assert hours_by_day({})["hours_mon"] is None
    # robots.txt evaluation
    rt = "User-agent: *\nDisallow: /toronto/\nAllow: /toronto/menu\n\nUser-agent: badbot\nDisallow: /\n"
    assert robots_decide(rt, "https://x.com/toronto/home")[:2] == (False, "Disallow: /toronto/")
    assert robots_decide(rt, "https://x.com/toronto/menu")[0] is True
    assert robots_decide(rt, "https://x.com/")[0] is True
    assert robots_decide("User-agent: *\nDisallow: /\n", "https://x.com/a")[:2] == (False, "Disallow: /")
    assert robots_decide("User-agent: SignalResearchBot\nDisallow: /\nUser-agent: *\nAllow: /\n", "https://x.com/")[2] == UA_TOKEN
    # ordering_urls: one url per destination
    u = ordering_urls('<a href="https://order.toasttab.com/online/foo">Order Online</a><a href="https://www.toasttab.com/">Powered by Toast</a>'
                      '<script src="https://cdn.shopify.com/x.js"></script><a href="https://www.ubereats.com/ca/store/foo/AbC123">Uber</a>'
                      '{"link":"https:\\/\\/order.online\\/store\\/foo-123\\/?delivery=true\\u0026hideModal=true"}', "https://foo.ca/")
    assert u["onsite"]["pos"] == "https://order.toasttab.com/online/foo" and u["onsite"]["pos_vendor"] == "toast", u
    assert u["onsite"]["doordash"] == "https://order.online/store/foo-123/?delivery=true&hideModal=true" and u["onsite"]["other"] is None, u
    assert u["offsite"]["ubereats"] == "https://www.ubereats.com/ca/store/foo/AbC123" and u["offsite"]["doordash"] is None, u
    assert ordering_urls("<html>x</html>", "https://zenku.square.site/")["onsite"]["pos"] == "https://zenku.square.site/"
    # JS app shell -> 99_JS_RENDERED (not tier 8/9); real pages and vendor hits are unaffected
    shell = '<html><head><meta name="description" content="Order Online Directly"><script defer src="/main.js"></script></head><body><noscript>Enable JS</noscript><div id="root"></div></body></html>'
    assert onsite_online_ordering_tier({"status": "OK", **fingerprint(shell)}) == "99_JS_RENDERED"
    assert onsite_online_ordering_tier({"status": "OK", **fingerprint(shell.replace("</body>", '<a href="https://order.toasttab.com/x">o</a></body>'))}) == "2_POS_Online_Ordering"
    real = "<html><body><script src='/a.js'></script><p>" + "Welcome to our family restaurant, serving pasta daily. " * 5 + "Order online now</p></body></html>"
    assert onsite_online_ordering_tier({"status": "OK", **fingerprint(real)}) == "8_Only_Order_Text_No_Link"
    # fingerprint + tiers
    fp = fingerprint('<a href="https://order.toasttab.com/x">Order Online</a> wp-content opentable.com')
    assert fp["ordering"] == ["toast"] and fp["order_cta"] and fp["builder"] == ["wordpress"], fp
    assert onsite_online_ordering_tier({"status": "OK", **fp}) == "2_POS_Online_Ordering"
    fp2 = fingerprint('<a href="https://abc.order.store/x">x</a> <a href="https://order.online/store/y">y</a>')
    fp3 = fingerprint('<a href="https://x.order.online/store/a-1/">o</a><a href="https://x.order.store/">o</a><a href="https://toasttab.com/x">o</a><a href="https://chownow.com/x">o</a>')
    assert fp3["ordering"] == ["toast", "chownow", "doordash_storefront", "ubereats_webshop"] and fp3["ordering_groups"] == ["pos", "platform", "delivery_storefront"], fp3
    assert fp2["ordering"] == ["doordash_storefront", "ubereats_webshop"] and "order.store" in fp2["ordering_link_samples"][0], fp2
    assert onsite_online_ordering_tier({"status": "OK", **fp2}) == "5_Onsite_Delivery_App_Storefront"
    assert onsite_online_ordering_tier({"status": "OK", **fingerprint('<script src="https://cdn.shopify.com/s/x.js"></script>')}) == "3_NonPOS_Known_Online_Ordering_Vendor"
    assert onsite_online_ordering_tier({"status": "OK", **fingerprint('<a href="https://order.online/z">x</a>')}) == "5_Onsite_Delivery_App_Storefront"
    assert onsite_online_ordering_tier({"status": "OK", **fingerprint('<a href="https://www.ubereats.com/store/z">x</a>')}) == "6_Only_Offsite_Delivery_App_Linkout"
    assert onsite_online_ordering_tier({"status": "OK", **fingerprint("Call us to order")}) == "7_Only_Call_to_Order"
    assert onsite_online_ordering_tier({"status": "NO_WEBSITE_ON_GOOGLE"}) == "_NO_WEBSITE"
    assert onsite_online_ordering_tier({"status": "ROBOTS_DISALLOWED"}) == "_UNCHECKED" and onsite_online_ordering_tier({"status": "SOCIAL_ONLY"}) == "_SOCIAL_ONLY"
    T = lambda h: onsite_online_ordering_tier({"status": "OK", **fingerprint(h, "https://x.ca/")})
    assert T('<a href="https://allens.ambassador.ai">Order Takeout or Delivery</a>') == "3_NonPOS_Known_Online_Ordering_Vendor"   # Ambassador is a known non-POS vendor
    assert T('<a href="/order">Order Online</a>') == "4_Unknown_Online_Ordering_Vendor"                     # own /order page
    assert T('<a href="/delivery-info">Delivery Information</a>') == "9_No_Ordering_Detected"              # not an order button
    assert T('<a href="https://www.doordash.com/store/x">Order delivery from DoorDash</a>') == "6_Only_Offsite_Delivery_App_Linkout"
    assert T('<a href="https://www.doordash.com/store/x">DoorDash</a><a href="https://gosnappy.io/x">Order Now</a>') == "4_Unknown_Online_Ordering_Vendor"
    assert T('<a href="https://www.doordash.com/store/x">Order from DoorDash</a><a href="https://order.toasttab.com/x">Order Now</a>') == "2_POS_Online_Ordering"
    assert T('<a href="https://order.store/store/x/abc">Order</a><a href="https://gosnappy.io/x">Order Now</a>') == "5_Onsite_Delivery_App_Storefront"
    assert T('Call us to order. <a href="https://allens.ambassador.ai">Order Online</a>') == "3_NonPOS_Known_Online_Ordering_Vendor"
    assert T("<p>Order online coming soon</p>") == "8_Only_Order_Text_No_Link"
    assert T('<button id="order-button">Order Online</button>') == "8_Only_Order_Text_No_Link"             # button, not a link
    assert T("<p>Our menu</p>") == "9_No_Ordering_Detected"
    assert onsite_online_ordering_tier({"status": "OK", **fingerprint('<a href="https://owner.com/x">o</a> toasttab.com')}) == "1_Owner_Customer"
    # CTA capture by anchor text (URL has no 'order' in it)
    fp3 = fingerprint('<a class="b" href="https://allens.ambassador.ai"\n class="x">\n  Order Takeout or Delivery\n </a>'
                      '<a href="/menu">Menu</a><a href="/order-online/">Order Now</a>', "https://allensdanforth.com/")
    assert fingerprint('<a href="https://mylightspeed.app/WRVRWGMW/C-ordering">ORDER ONLINE</a>')["ordering"] == ["lightspeed"]
    assert fp3["cta_links"][0] == {"text": "Order Takeout or Delivery", "url": "https://allens.ambassador.ai", "external": True, "kind": "vendor:ambassador"}, fp3["cta_links"]
    assert fp3["cta_links"][1]["url"] == "https://allensdanforth.com/order-online/" and fp3["cta_links"][1]["kind"] == "same_site"
    fp4 = fingerprint('<a href="https://www.doordash.com/store/x">Order delivery from DoorDash</a><a href="https://order.toasttab.com/online/x">Order Now</a>', "https://x.ca/")
    assert [c["kind"] for c in fp4["cta_links"]] == ["vendor:toast", "marketplace:doordash"], fp4["cta_links"]
    assert all(c["text"] != "Menu" for c in fp3["cta_links"])
    # key loading (env var wins, then file; quotes/whitespace stripped)
    kd = tempfile.mkdtemp(); saved = os.environ.pop("GOOGLE_PLACES_API_KEY", None)
    try:
        assert load_key([kd]) == ""
        open(f"{kd}/{KEY_FILE}", "w").write('  "AIza-test-key"  \nsecond line ignored\n')
        assert load_key([kd]) == "AIza-test-key"
        os.environ["GOOGLE_PLACES_API_KEY"] = "from-env"; assert load_key([kd]) == "from-env"
    finally:
        os.environ.pop("GOOGLE_PLACES_API_KEY", None)
        if saved is not None: os.environ["GOOGLE_PLACES_API_KEY"] = saved
    # link-sample priority + html saving
    many = "".join(f'<a href="/order-{i}">x</a>' for i in range(40)) + '<a href="https://www.chownow.com/order/123?utm=' + "z" * 400 + '">c</a>'
    ls = link_samples(many)
    assert ls[0].startswith("https://www.chownow.com") and len(ls[0]) == 300 and len(ls) == 30, (ls[0][:40], len(ls))
    td = tempfile.mkdtemp()
    assert maybe_save_html("a", "<html>x</html>", "2_POS_Online_Ordering", td, "review") is None
    pth = maybe_save_html("a", "<html>x</html>", "4_Unknown_Online_Ordering_Vendor", td, "review")
    assert gzip.open(pth, "rt").read() == "<html>x</html>"
    assert maybe_save_html("b", "<html/>", "2_POS_Online_Ordering", td, "all") and maybe_save_html("c", "x", "1_Owner_Customer", td, "none") is None
    # location counting with a fake search
    fake = lambda name: iter([[{"id": "x1", "displayName": {"text": "Test Grill"}, "websiteUri": "https://example.com"},
                               {"id": "x2", "displayName": {"text": "Test Grill - Leslieville"}, "websiteUri": "https://www.example.com/leslieville"},
                               {"id": "x3", "displayName": {"text": "Test Grill"}, "websiteUri": "https://other.com"},
                               {"id": "x4", "displayName": {"text": "Unrelated"}}]])
    loc = count_locations(p, fake)
    assert loc == {"locations_by_name": 3, "locations_by_domain": 2}, loc
    assert location_summary(loc) == (2, "2-5", "domain")
    assert location_summary({"locations_by_name": 7, "locations_by_domain": None}) == (7, "6+", "name_only")
    # DineSafe join + coverage with a mock file
    mock = ("Establishment ID,Establishment Name,Establishment Type,Establishment Address,Establishment Status,Inspection ID,Inspection Date,Latitude,Longitude\n"
            "1,TEST GRILL,Restaurant,1234 DANFORTH AVE,Pass,a,2009-02-01,43.6801,-79.3401\n"
            "1,TEST GRILL,Restaurant,1234 DANFORTH AVE,Pass,b,2026-03-01,43.6801,-79.3401\n"
            "2,NEW PLACE,Restaurant,1300 DANFORTH AVE,Pass,c,2025-06-01,43.6805,-79.3300\n"
            "3,GHOST KITCHEN,Food Take Out,1400 DANFORTH AVE,Pass,d,2026-01-01,43.6810,-79.3200\n"
            "4,CLOSED CAFE,Restaurant,1500 DANFORTH AVE,Closed,e,2020-01-01,43.6820,-79.3250\n")
    d = tempfile.mkdtemp(); mp = f"{d}/ds.csv"; open(mp, "w").write(mock)
    ds = ds_load(mp); assert len(ds) == 4 and next(e for e in ds if e["id"] == "1")["n_insp"] == 2
    res = ds_enrich([p], ds)["x1"]
    assert res["ds_first_inspection"] == "2009-02-01" and res["ds_match_method"] == "geo" and res["tenure_censored"] is True, res
    assert res["est_years_open"] == int((date.today() - date(2009, 2, 1)).days / 365.25 + 0.5) and isinstance(res["est_years_open"], int), res
    p_new = {"id": "x9", "displayName": {"text": "New Place"}, "formattedAddress": "1300 Danforth Ave, Toronto", "location": {"latitude": 43.6805, "longitude": -79.3300}}
    r2 = ds_enrich([p_new], ds)["x9"]; assert r2["tenure_censored"] is False and r2["ds_first_inspection"] == "2025-06-01", r2
    cov = ds_coverage(ds, [p, p_new], {"x1"})
    cats = {r["ds_name"]: r["category"] for r in cov}
    assert cats == {"TEST GRILL": "found_and_survived", "NEW PLACE": "found_but_filtered", "GHOST KITCHEN": "NOT_FOUND_BY_GOOGLE_DISCOVERY"}, cats
    # multi-file DineSafe folder: same place under different IDs/schemas gets merged, earliest date wins
    dd = tempfile.mkdtemp()
    open(f"{dd}/2010.csv", "w").write("Establishment ID,Establishment Name,Establishment Address,Inspection Date,Latitude,Longitude\n"
                                      "55,Old Spot,10 Danforth Ave,2010-05-05,43.68,-79.34\n")
    open(f"{dd}/2025.csv", "w").write("ESTABLISHMENT_ID,ESTABLISHMENT_NAME,ESTABLISHMENT_ADDRESS,INSPECTION_DATE,LATITUDE,LONGITUDE\n"
                                      "9001,OLD SPOT,10 DANFORTH AVE,2025-02-02,43.68,-79.34\n")
    mds = ds_load(dd); assert len(mds) == 1 and mds[0]["first"].isoformat() == "2010-05-05" and mds[0]["last"].isoformat() == "2025-02-02" and mds[0]["n_insp"] == 2, mds
    # --- Stage 5: marketplace matching, using real search results captured on 2026-10-01 as fixtures ---
    assert clean_listing_title("Order Mezes (danforth) - Menu & Prices - Toronto Delivery") == "Mezes (danforth)"
    assert clean_listing_title("Order Mezes - Toronto, ON Menu Delivery [Menu & Prices]") == "Mezes"
    assert clean_listing_title("Il Fornello (Danforth Ave) Delivery【Menu & Prices & Promotion】| Toronto ON") == "Il Fornello (Danforth Ave)"
    assert clean_listing_title("SINALOA FACTORY Delivery Menu") == "SINALOA FACTORY"
    assert clean_listing_title("Allen’s") == "Allen’s"
    sm = lambda mk, u: re.search(MARKETPLACES[mk]["store"], u)
    assert sm("ubereats", "https://www.ubereats.com/ca/store/mezes-danforth/uF33fVn5QKum8GcIl716ng").group(1) == "mezes-danforth"
    assert not sm("ubereats", "https://www.ubereats.com/ca/category/toronto-on/greek") and not sm("ubereats", "https://www.ubereats.com/ca/city/toronto-on")
    assert sm("doordash", "https://www.doordash.com/en-CA/store/mezes-toronto-44746").group(1) == "mezes-toronto-44746"
    assert sm("doordash", "https://www.doordash.com/store/27733726") and not sm("doordash", "https://www.doordash.com/en-CA/business/sinaloa-factory-11106806/")
    assert sm("skip", "https://www.skipthedishes.com/yanagi-sushi-danforth-avenue").group(1) == "yanagi-sushi-danforth-avenue"
    assert not sm("skip", "https://www.skipthedishes.com/city-area/toronto/east-end-danforth") and not sm("skip", "https://www.skipthedishes.com/cities/toronto")
    assert sm("fantuan", "https://order.fantuan.ca/en-US/store/il-fornello/ca-20207").group(1) == "il-fornello"
    R = lambda t, u, d="": {"title": t, "url": u, "description": d}
    UE, DD, SK = "https://www.ubereats.com/ca/", "https://www.doordash.com/", "https://www.skipthedishes.com/"
    def fake_brave(q):
        ql = q.lower()
        if "boom" in ql: raise RuntimeError("Brave HTTP 429")
        if "mezes" in ql and "ubereats.com" in ql:
            return [R("Order Mezes (danforth) - Menu & Prices - Toronto Delivery", UE + "store/mezes-danforth/uF33fVn5QKum8GcIl716ng"),
                    R("THE 10 BEST Greek Food Delivery in Toronto 2026| Order Greek Food Takeout", UE + "category/toronto-on/greek"),
                    R("Danforth Eats", UE + "store/danforth-eats/anfZ2326TbePGSum3JeARw")]
        if "mezes" in ql and "doordash.com" in ql:
            return [R("Order Mezes - Toronto, ON Menu Delivery [Menu & Prices]", DD + "store/mezes-toronto-44746"),
                    R("Order Mehman Restaurant (Danforth Avenue) - Toronto, ON Menu Delivery", DD + "store/mehman-restaurant-(danforth-avenue)-toronto-31133056/"),
                    R("Order Megas Restaurant - Toronto, ON Menu Delivery [Menu & Prices]", DD + "en-US/store/megas-restaurant-toronto-60823/")]
        if "mezes" in ql and "skipthedishes.com" in ql:
            return [R("Best Local Food Delivery in Toronto Near You", SK + "cities/toronto"), R("Messini (445 Danforth Ave)", SK + "messini-danforth-avenue")]
        if "sinaloa" in ql and "ubereats.com" in ql:
            return [R("Order Sinaloa Factory - Menu & Prices - Toronto Delivery", UE + "store/sinaloa-factory/n9WgGW6TV42Hdm5JfIQFcA"),
                    R("Order Sinaloa Factory Menu Delivery in Toronto", UE + "store/sinaloa-factory/l6Ts0ySpWt6jfMxMvpSV_g/efdaa993")]
        if "sinaloa" in ql and "doordash.com" in ql:
            return [R("Order Sinaloa Factory - Toronto, ON Menu Delivery [Menu & Prices]", DD + "en-CA/store/sinaloa-factory-toronto-23059309/"),
                    R("Order SINALOA FACTORY - Vaughan, ON Menu Delivery [Menu & Prices]", DD + "store/sinaloa-factory-toronto-1223937/en-ca"),
                    R("Order Sinaloa Factory - Hamilton, ON Menu Delivery ...", DD + "en/store/sinaloa-factory-hamilton-28212401/")]
        if "sinaloa" in ql and "skipthedishes.com" in ql:
            return [R("Sinaloa Factory (551 Danforth Ave)", SK + "sinaloa-factory-danforth"), R("Sinaloa Factory (511 Maple Grove Dr)", SK + "sinaloa-factory-511-maple-grove")]
        if "yanagi" in ql and "ubereats.com" in ql:
            return [R("Order Yanagi Sushi - Menu & Prices - Toronto Delivery", UE + "store/yanagi-sushi/L-2TMObWT6mXrOxT9qHfEg"),
                    R("Order Aji Sai Japanese Restaurant (Danforth Ave & Jones Ave) - Menu & Prices", UE + "store/aji-sai-japanese-restaurant/JXEPdfNoTDOsDtUebHdJng"),
                    R("Order Sushi Bar - Menu & Prices - Toronto Delivery", UE + "store/sushi-bar/0CdOIqgPS4y_9rfJxpr0wQ")]
        if "yanagi" in ql and "doordash.com" in ql:
            return [R("Order Yanagi Sushi - Toronto, ON Menu Delivery [Menu & Prices]", DD + "en/store/yanagi-sushi-toronto-178941/"),
                    R("Yanagi Sushi & Grill's Delivery & Takeout Near You - DoorDash", DD + "business/yanagi-sushi-&-grill-25458/")]
        if "yanagi" in ql and "skipthedishes.com" in ql:
            return [R("Yanagi Sushi (1524 Danforth Ave)", SK + "yanagi-sushi-danforth-avenue")]
        if "allen" in ql and "ubereats.com" in ql:
            return [R("Allen’s", UE + "store/allens/DZBjF08aXceG_8npjpIE2w")]
        return []
    prow = lambda i, n, a, t="": {"place_id": i, "name": n, "address": a, "primary_type": t}
    rows5 = [prow("m", "Mezes", "440 Danforth Ave, Toronto, ON M4K 1P1, Canada", "greek_restaurant"),
             prow("s", "Sinaloa Factory Danforth.", "551 Danforth Ave, Toronto, ON M4K 1R1, Canada"),
             prow("y", "Yanagi Sushi", "1524 Danforth Ave, Toronto, ON M4J 1N4, Canada", "japanese_restaurant"),
             prow("a", "Allen's", "143 Danforth Ave, Toronto, ON M4K 1N2, Canada"), prow("b", "Boom Cafe", "1 Danforth Ave, Toronto")]
    r5 = check_marketplaces(rows5, search=fake_brave, progress=False)
    st = lambda pid, m: r5[pid][m]["status"]
    assert [st("m", m) for m in ("ubereats", "doordash", "skip", "fantuan")] == ["high", "medium", "not_found", "not_found"], r5["m"]
    assert [st("s", m) for m in ("ubereats", "doordash", "skip")] == ["medium", "medium", "high"], r5["s"]
    assert r5["s"]["doordash"]["n"] == 1, r5["s"]["doordash"]            # Vaughan / Hamilton / other-address pages rejected
    assert r5["s"]["ubereats"]["n"] == 2 and r5["s"]["ubereats"]["ambiguous"] is False   # same slug twice -> not ambiguous
    assert [st("y", m) for m in ("ubereats", "doordash", "skip")] == ["medium", "medium", "high"], r5["y"]
    assert [st("a", m) for m in ("ubereats", "doordash", "skip")] == ["low", "not_found", "not_found"], r5["a"]
    assert r5["b"]["skip"]["status"] == "error" and mk_flat(r5["b"])["mkpc_search_listed_n"] == 0
    assert mk_flat(r5["s"])["mkpc_search_listed_n"] == 3 and mk_flat(r5["a"])["mkpc_search_listed_any_n"] == 1
    assert check_marketplaces(rows5[:3], search=fake_brave, fantuan="asian", progress=False)["m"]["fantuan"]["status"] == "not_checked"
    # build with marketplace columns
    build([p], {"x1": {"takeout": True, "delivery": True}}, {"x1": {"status": "OK", **fp}}, d, {"x1": loc}, {"x1": res}, {"x1": r5["m"]})
    assert "mkpc_search_ubereats_url" in open(f"{d}/signals.csv").read().split("\n")[0]
    # build
    build([p], {"x1": {"takeout": True, "delivery": True}}, {"x1": {"status": "OK", **fp}}, d, {"x1": loc}, {"x1": res})
    print(open(f"{d}/signals.csv").read()[:300].split("\n")[0][:200], "...")
    print("selftest OK")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("cmd", choices=["run", "snapshot", "dinesafe", "marketplaces", "selftest"])
    ap.add_argument("--marketplaces", action="store_true", help="(run) also check delivery-marketplace listings via Brave")
    ap.add_argument("--fantuan", choices=["all", "asian", "none"], default="all", help="which restaurants to check on Fantuan")
    ap.add_argument("--city", default="Toronto"); ap.add_argument("--only", default=None, help="(marketplaces) comma list of whole words to match in the name")
    ap.add_argument("--limit", type=int, default=0, help="(marketplaces) only the first N rows")
    ap.add_argument("--out", default="out"); ap.add_argument("--min-reviews", type=int, default=30)
    ap.add_argument("--max-stage3", type=int, default=0, help="cap on restaurants sent to the paid detail/website stages (0 = no cap)")
    ap.add_argument("--no-locations", action="store_true")
    ap.add_argument("--yes", action="store_true", help="skip the cost confirmation (only after reviewing the estimate)")
    ap.add_argument("--estimate-only", action="store_true", help="print the cost estimate and stop before any paid call")
    ap.add_argument("--save-html", choices=["none", "review", "all"], default="review",
                    help="save gzipped site HTML to <out>/html/: 'review' = only low-confidence ordering tiers")
    ap.add_argument("--dinesafe", default=None, help="'auto' to download via CKAN, or a path to a DineSafe CSV/ZIP")
    a = ap.parse_args()
    try:
        {"run": run, "snapshot": snapshot, "dinesafe": dinesafe_cmd, "marketplaces": marketplaces_cmd, "selftest": lambda x: selftest()}[a.cmd](a)
    finally:
        if a.cmd != "selftest": usage_report(a.out, a.cmd)
