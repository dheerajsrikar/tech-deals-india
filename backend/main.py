"""Indian Tech & Mobile Deals — autonomous affiliate agent (backend).

Runs on Google Cloud Run (Always Free Tier) + Firestore (Native Mode).
Every 2 hours Cloud Scheduler calls GET /run?key=CRON_SECRET.

Flow per run:
  1. Fetch the Smartprix RSS feed (latest 25) and scrape the public
     web previews of 2 deal Telegram channels (latest 10 messages
     each) — max 25 items scanned per run.
  2. For each item, resolve the real merchant link (Amazon/Flipkart/...)
     — short links (amzn.to / fkrt.it / link.amazon ...) are followed
     to the final store URL first.
  3. Dedup permanently: Amazon deals are keyed by ASIN (doc id
     `asin_<ASIN>`); other stores by MD5 of the query-stripped URL.
     A product whose first-5-words key was posted in the last 7 days
     is also skipped. A deal is never processed twice.
  4. Ask Gemini (gemini-3.5-flash-lite, free tier) to approve ONLY
     Smartphones, Laptops, Earbuds, Headphones, Smartwatches, Tablets,
     Power Banks, Chargers or Branded Tech Accessories — and to write the
     headline / Telegram alert / Pinterest copy / price tag as strict JSON.
  5. Wrap the store link with the Amazon Associates tag (amazon.in) or
     Cuelinks (every other store), after stripping any existing
     affiliate/tracking params (tag, ascsubtag, linkCode, ref, affid).
  6. Post to Telegram (affiliate link inline in the message text, with
     product preview card) and, when configured, pin to Pinterest.
     Save the deal to Firestore `deals` for the website. Max 5 new
     posts per run.

Env vars (set on Cloud Run, never committed to git):
  CRON_SECRET, GEMINI_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
  AMAZON_TAG, CUELINKS_ID, PINTEREST_TOKEN, PINTEREST_BOARD_ID
Optional: FIRESTORE_PROJECT (defaults to the GCP project),
  RSS_FEED_URLS (comma-separated; default: Smartprix only),
  RSS_URL (legacy single-feed override),
  TG_CHANNELS (comma-separated; default: techglaredeals,OMGDeals).
"""

import hashlib
import hmac
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, urlparse, parse_qsl, urlencode, urlunparse

import feedparser
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tech-deals-agent")

app = Flask(__name__)
CORS(app)  # /deals is a public endpoint consumed by GitHub Pages

# DesiDime (the original single source) returns 403 to Cloud Run egress
# and to this VM — its WAF blocks cloud IPs outright (verified 2026-11-22).
# The other Indian deal feeds were tested from Cloud Run the same day and
# all failed there too: IndiaFreeStuff (200 but 0 entries — serves HTML,
# not RSS), DealNLoot (522, origin down), FreeKaaMaal (410 Gone), GyFTR,
# CouponRani, MySmartPrice (all 0 entries). Smartprix is the only feed
# verified returning 200 OK with real entries from Cloud Run, so it is
# the only feed configured. Re-add a feed only after verifying it returns
# 200 OK with entries from Cloud Run.
_DEFAULT_FEEDS = "https://www.smartprix.com/feed/"
_RSS_URLS_RAW = os.environ.get("RSS_FEED_URLS", "").strip()
if not _RSS_URLS_RAW:
    _RSS_URLS_RAW = os.environ.get("RSS_URL", "").strip() or _DEFAULT_FEEDS
RSS_FEED_URLS = [u.strip() for u in _RSS_URLS_RAW.split(",") if u.strip()]
MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash-lite")
# Total items scanned per run across all sources, and the per-run cap on
# NEW Telegram posts (steady stream, no flooding). A run stays well
# inside the Scheduler's 180s deadline: ~25 x (page fetch + Gemini).
MAX_ITEMS_PER_RUN = int(os.environ.get("MAX_ITEMS_PER_RUN", "25"))
MAX_ITEMS_PER_FEED = int(os.environ.get("MAX_ITEMS_PER_FEED", "25"))
MAX_POSTS_PER_RUN = int(os.environ.get("MAX_POSTS_PER_RUN", "5"))
# Public Telegram deal channels scraped via their t.me/s/<channel> web
# preview (no login). Verified 2026-11-22: both return 200 with fresh
# merchant links (amzn.to / flipkart.com) posted the same day.
# TechPuran was checked the same day and rejected: tech NEWS, 0 deal
# links. These are general loot channels, so Gemini still filters to
# tech only.
_TG_RAW = os.environ.get("TG_CHANNELS", "").strip()
TELEGRAM_CHANNELS = tuple(
    c.strip() for c in _TG_RAW.split(",") if c.strip()) or (
        "techglaredeals", "OMGDeals")
MAX_ITEMS_PER_TG_CHANNEL = int(
    os.environ.get("MAX_ITEMS_PER_TG_CHANNEL", "10"))
# A product (first 5 words of its name) posted within this window is
# skipped even under a different ASIN.
PRODUCT_KEY_WINDOW_DAYS = int(
    os.environ.get("PRODUCT_KEY_WINDOW_DAYS", "7"))

# Merchant domains a deal link may point at. Blog posts link to these;
# anything else in an entry is treated as a blog/internal link.
MERCHANT_HOSTS = (
    "amazon.in", "amazon.com", "flipkart.com", "croma.com",
    "reliancedigital.in", "vijaysales.com", "tatacliq.com",
    "myntra.com", "ajio.com", "mi.com", "samsung.com", "oneplus.in",
    "realme.com", "oppo.com", "vivo.com", "store.google.com",
    "apple.com", "snapdeal.com", "boat-lifestyle.com",
)
SHORTENER_HOSTS = (
    "amzn.to", "fkrt.it", "bit.ly", "tinyurl.com", "goo.gl",
    "cutt.ly", "shorturl.at", "t.co",
    # Store shorteners seen in Telegram deal posts (all resolve to the
    # merchant product page): Amazon's own link.amazon, Flipkart's
    # fkrt.cc, Myntra's myntr.it, Ajio's ajiio.in, Associates amzaff.in.
    "link.amazon", "fkrt.cc", "myntr.it", "ajiio.in", "amzaff.in",
    # Deal-site redirectors: the href is not the merchant itself, but
    # following it lands on the merchant (e.g. Smartprix "Buy" buttons
    # go through l.smartprix.com -> amazon.in / flipkart.com).
    "l.smartprix.com",
)

ALLOWED_CATEGORIES = (
    "Smartphones, Laptops, Earbuds, Headphones, Smartwatches, Tablets, "
    "Power Banks, Chargers, Branded Tech Accessories"
)

# Browser-like headers: DesiDime returns 403 to bare curl / default
# python user-agents, but serves the feed to a normal browser UA.
RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-IN,en;q=0.9",
}

# ---------------------------------------------------------------- Firestore
_db = None
_db_tried = False


def get_db():
    """Lazy Firestore client. Returns None (and logs) when unavailable so
    /deals stays up and /run reports the failure instead of crashing."""
    global _db, _db_tried
    if _db_tried:
        return _db
    _db_tried = True
    try:
        from google.cloud import firestore

        project = os.environ.get("FIRESTORE_PROJECT") or os.environ.get(
            "GOOGLE_CLOUD_PROJECT")
        _db = firestore.Client(project=project) if project \
            else firestore.Client()
        log.info("Firestore client ready (project=%s)", _db.project)
    except Exception as exc:  # noqa: BLE001 - startup must not crash
        log.warning("Firestore unavailable: %s", exc)
        _db = None
    return _db


ASIN_RE = re.compile(r"/(?:dp|gp/product)/([A-Z0-9]{10})(?:[/?#]|$)", re.I)


def extract_asin(url):
    """Amazon ASIN: the 10-char code after /dp/ or /gp/product/."""
    m = ASIN_RE.search(url or "")
    return m.group(1).upper() if m else ""


def dedup_doc_id(merchant_url):
    """Permanent dedup key for a merchant URL: `asin_<ASIN>` for Amazon
    (same product, any blog/source, any tracking params = one deal);
    for other stores, `url_<md5>` of the URL with ALL query params
    stripped (?ref=, ?tag=, ...), so the same product link with
    different tracking still dedups."""
    asin = extract_asin(merchant_url)
    if asin:
        return "asin_" + asin
    clean = urlunparse(urlparse(merchant_url)._replace(query=""))
    return "url_" + hashlib.md5(clean.encode("utf-8")).hexdigest()


def product_key(title):
    """Normalized product identity: first 5 words of the name,
    lowercased, punctuation stripped — 'Sony WH-CH530 Wireless
    Headphones' and 'Sony WH-CH530 Wireless Headphone (Black)' share
    the key sony_wh_ch530_wireless_headphones."""
    words = re.sub(r"[^a-z0-9 ]", " ", (title or "").lower()).split()
    return "_".join(words[:5]) or "unknown"


def product_posted_recently(db, key, days=PRODUCT_KEY_WINDOW_DAYS):
    """True when this product key was posted within the last `days`."""
    ref = db.collection("product_keys").document(key)
    snap = ref.get()
    if not snap.exists:
        return False
    ts = snap.to_dict().get("ts", 0)
    return (time.time() - ts) < days * 86400


# ------------------------------------------------------------ Affiliate wrap
def affiliate_url(url):
    """Amazon.in -> append ?tag=AMAZON_TAG ; any other store -> Cuelinks."""
    url = url.strip()
    host = urlparse(url).netloc.lower()
    if "amazon.in" in host or host.endswith(".amazon.in"):
        tag = os.environ.get("AMAZON_TAG", "").strip()
        if not tag:
            return url
        parts = urlparse(url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        # Strip any existing affiliate params (e.g. the feed source's own
        # tag / ascsubtag / linkCode) so ONLY our AMAZON_TAG remains —
        # the `tag` is what decides which Associates account gets paid.
        for param in ("tag", "ascsubtag", "linkCode", "ref", "affid"):
            query.pop(param, None)
        query["tag"] = tag
        return urlunparse(parts._replace(query=urlencode(query)))
    pub_id = os.environ.get("CUELINKS_ID", "").strip()
    if not pub_id:
        return url
    return ("https://linksredirect.com/?pub_id={}&url={}".format(
        quote(pub_id, safe=""), quote(url, safe="")))


# ------------------------------------------------------------------- Gemini
def ai_review(title, summary, link, image):
    """Ask Gemini to approve/reject + write copy. Returns dict or None.

    Strict JSON out: approved, headline (<=70 chars), telegram_html
    (3-line HTML alert, no URLs), pin_description (SEO copy + 4 hashtags),
    price_tag.
    """
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        log.warning("GEMINI_API_KEY not set — skipping AI review")
        return None
    try:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)
        prompt = (
            "You are a strict deals editor for an Indian tech-deals channel. "
            "Approve ONLY deals for these categories: {cats}. "
            "REJECT everything else: coupons, clothing, footwear, jewellery, "
            "groceries, food, furniture, beauty, home appliances (TVs, ACs, "
            "washing machines, fans, kitchen items), bags, watches that are "
            "not smartwatches, finance/credit-card offers, gift cards, "
            "recharge, spam, or any deal with a weak/missing discount.\n\n"
            "Deal title: {title}\nDeal text: {summary}\nStore link: {link}\n\n"
            "Reply with STRICT JSON only, no markdown, no extra keys:\n"
            '{{"approved": true/false, "headline": "<max 70 chars, deal + '
            'store, no clickbait>", "telegram_html": "<exactly 3 lines of '
            'HTML using <b> and emojis, no URLs, no link text>", '
            '"pin_description": "<SEO copy, 1-2 sentences, then exactly 4 '
            'hashtags>", "price_tag": "<e.g. Rs.1,299 | MRP Rs.2,999 | 57% '
            'OFF — use only prices stated in the deal text; if none is '
            'stated use \\"Deal Price\\"">}}'
        ).format(cats=ALLOWED_CATEGORIES, title=title,
                 summary=(summary or "")[:600], link=link)
        resp = client.models.generate_content(
            model=MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.2,
                max_output_tokens=600,
            ),
        )
        data = json.loads(resp.text)
        if not isinstance(data, dict) or "approved" not in data:
            log.warning("Gemini returned unexpected JSON: %s", resp.text[:300])
            return None
        data["headline"] = str(data.get("headline") or title)[:70]
        return data
    except Exception as exc:  # noqa: BLE001 - one bad call must not kill run
        log.warning("Gemini review failed for %r: %s", title, exc)
        return None


# ------------------------------------------------------------------ Telegram
def post_telegram(aff_url, review):
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        log.warning("Telegram not configured — skipping post")
        return False
    # Link inline in the message text (no bottom button): Telegram shows
    # the product preview card for the plain URL line.
    link_html = aff_url.replace("&", "&amp;")
    text = (review.get("telegram_html", "") +
            '\n\n👉 <a href="{}">Grab Deal Now</a>\n🔗 {}'.format(
                link_html, aff_url))
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        r = requests.post(
            "https://api.telegram.org/bot{}/sendMessage".format(token),
            json=payload, timeout=20)
        if r.status_code != 200:
            log.warning("Telegram API %s: %s", r.status_code, r.text[:300])
            return False
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("Telegram post failed: %s", exc)
        return False


# ----------------------------------------------------------------- Pinterest
def post_pinterest(aff_url, review, image):
    token = os.environ.get("PINTEREST_TOKEN", "").strip()
    board_id = os.environ.get("PINTEREST_BOARD_ID", "").strip()
    if not token or not board_id:
        return False  # optional channel — silently skipped when unset
    payload = {
        "board_id": board_id,
        "link": aff_url,
        "title": review.get("headline", "")[:100],
        "description": review.get("pin_description", ""),
    }
    if image:
        payload["media_source"] = {
            "source_type": "image_url", "url": image}
    try:
        r = requests.post(
            "https://api.pinterest.com/v5/pins", json=payload,
            headers={"Authorization": "Bearer {}".format(token),
                     "Content-Type": "application/json"},
            timeout=20)
        if r.status_code not in (200, 201):
            log.warning("Pinterest API %s: %s", r.status_code, r.text[:300])
            return False
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("Pinterest pin failed: %s", exc)
        return False


# ------------------------------------------------------- Merchant links
def _host(url):
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:  # noqa: BLE001
        return ""


def _is_merchant(url):
    h = _host(url)
    return any(h == m or h.endswith("." + m) for m in MERCHANT_HOSTS)


def _resolve_shortener(url):
    """Follow a short link (amzn.to / fkrt.it / bit.ly ...) to its final
    URL; returns the original URL when resolution fails or is not a
    shortener."""
    if _host(url) not in SHORTENER_HOSTS:
        return url
    try:
        r = requests.get(url, headers=RSS_HEADERS, timeout=8,
                         allow_redirects=True)
        if r.url and r.url != url:
            return r.url
    except Exception as exc:  # noqa: BLE001
        log.warning("Short-link resolve failed (%s): %s", url, exc)
    return url


def _merchant_from_html(html):
    """First merchant href in an HTML page, in document order. A
    shortener/redirector href is followed and counts if it lands on a
    merchant."""
    seen = set()
    for href in re.findall(r'href=["\']([^"\']+)["\']', html or "", re.I):
        href = href.strip()
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            continue
        if not href.startswith("http"):
            continue
        if href in seen:
            continue
        seen.add(href)
        if _is_merchant(href):
            return _resolve_shortener(href)
        if _host(href) in SHORTENER_HOSTS:
            resolved = _resolve_shortener(href)
            if _is_merchant(resolved):
                return resolved
    return ""


def extract_merchant_link(entry):
    """Real store link for an RSS item.

    1. Any merchant URL already in the entry (link, feedparser links,
       or hrefs inside the summary/description) wins — no extra fetch.
    2. Otherwise fetch the item page (a deal blog post) and take its
       first merchant link.
    3. Fall back to the item link itself.
    """
    blog_link = (entry.get("link") or "").strip()
    candidates = []
    if blog_link:
        candidates.append(blog_link)
    html = (entry.get("html") or entry.get("summary")
            or entry.get("description") or "")
    for lnk in entry.get("links") or []:
        href = (lnk.get("href") or "").strip()
        if href:
            candidates.append(href)
    candidates += re.findall(r'href=["\']([^"\']+)["\']', html or "", re.I)
    for url in candidates:
        url = url.strip()
        if _is_merchant(url):
            return _resolve_shortener(url)
        if _host(url) in SHORTENER_HOSTS:
            resolved = _resolve_shortener(url)
            if _is_merchant(resolved):
                return resolved
    if blog_link and not _is_merchant(blog_link):
        try:
            r = requests.get(blog_link, headers=RSS_HEADERS, timeout=8)
            r.raise_for_status()
            merchant = _merchant_from_html(r.text)
            if merchant:
                return merchant
        except Exception as exc:  # noqa: BLE001
            log.warning("Blog-page fetch failed (%s): %s", blog_link, exc)
    return blog_link


# ---------------------------------------------------------------------- Feed
def fetch_feed(feed_url):
    """Fetch + parse one RSS feed. Retries 3x with backoff; returns []
    only after all attempts fail (a run then reports that feed as
    failed instead of crashing)."""
    for attempt in range(1, 4):
        try:
            r = requests.get(feed_url, headers=RSS_HEADERS, timeout=15)
            r.raise_for_status()
            content = r.content  # read inside try: truncated chunked
            # responses raise ChunkedEncodingError here, not on .get()
            if len(content) < 500:
                raise ValueError("feed too small ({} bytes)".format(
                    len(content)))
        except Exception as exc:  # noqa: BLE001
            log.warning("RSS fetch attempt %d/3 failed (%s): %s",
                        attempt, feed_url, exc)
            time.sleep(2 * attempt)
            continue
        feed = feedparser.parse(content)
        items = []
        for e in feed.entries[:MAX_ITEMS_PER_FEED]:
            link = (e.get("link") or "").strip()
            if not link:
                continue
            title = re.sub(r"\s+", " ", (e.get("title") or "")).strip()
            summary = re.sub(r"<[^>]+>", " ",
                             (e.get("summary") or e.get("description") or ""))
            summary = re.sub(r"\s+", " ", summary).strip()
            # Full post HTML (WordPress content:encoded) often carries the
            # merchant "Buy" links that the short summary does not.
            html = (e.get("summary") or e.get("description") or "")
            content = e.get("content") or []
            if content:
                html += " " + (content[0].get("value") or "")
            image = ""
            if e.get("media_content"):
                image = e.media_content[0].get("url", "")
            elif e.get("enclosures"):
                image = e.enclosures[0].get("href", "")
            items.append({"title": title, "summary": summary,
                          "link": link, "image": image,
                          "html": html, "source_feed": feed_url})
        log.info("Feed OK (%s): %d entries", feed_url, len(items))
        return items
    log.warning("Feed FAILED after 3 attempts: %s", feed_url)
    return []


# ------------------------------------------------- Telegram channel scrape
def scrape_telegram_channel(channel, max_items=MAX_ITEMS_PER_TG_CHANNEL):
    """Public web preview https://t.me/s/<channel> (no login): take the
    latest messages that carry a merchant link, newest first. The page
    lists oldest first, so the list is reversed after collecting."""
    url = "https://t.me/s/" + channel
    try:
        r = requests.get(url, headers=RSS_HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        log.warning("Telegram channel fetch failed (%s): %s", channel, exc)
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    items = []
    for msg in soup.select("div.tgme_widget_message"):
        text_el = msg.select_one("div.tgme_widget_message_text")
        text = text_el.get_text("\n", strip=True) if text_el else ""
        if not text:
            continue
        # First merchant/shortener href in the message (document order).
        merchant = ""
        for a in msg.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href.startswith("http"):
                continue
            if _is_merchant(href) or _host(href) in SHORTENER_HOSTS:
                merchant = _resolve_shortener(href)
                break
        if not merchant or not _is_merchant(merchant):
            continue
        title = re.sub(r"\s+", " ", text.split("\n")[0]).strip()[:120]
        items.append({"title": title,
                      "summary": re.sub(r"\s+", " ", text).strip()[:600],
                      "link": merchant, "image": "",
                      "source_url": url,
                      "source_feed": "telegram:" + channel})
    items.reverse()
    log.info("Telegram channel OK (%s): %d items", channel,
             len(items[:max_items]))
    return items[:max_items]


# ------------------------------------------------------------------- Routes
@app.route("/", methods=["GET"])
def health():
    return jsonify({"service": "tech-deals-agent", "status": "ok",
                    "model": MODEL, "feeds": RSS_FEED_URLS})


@app.route("/run", methods=["GET"])
def run():
    secret = os.environ.get("CRON_SECRET", "")
    key = request.args.get("key", "")
    if not secret:
        return jsonify({"error": "CRON_SECRET not configured"}), 500
    if not key or not hmac.compare_digest(key, secret):
        return jsonify({"error": "unauthorized"}), 403

    db = get_db()
    if db is None:
        return jsonify({"error": "firestore_unavailable"}), 500

    # Fetch RSS feeds + Telegram channel previews concurrently: a dead
    # source costs ~45s of retries, and serial fetching would push the
    # run past the Scheduler's 180s deadline. Concurrently the run
    # costs only the slowest source.
    with ThreadPoolExecutor(
            max_workers=len(RSS_FEED_URLS) + len(TELEGRAM_CHANNELS)) as pool:
        fetched = list(pool.map(fetch_feed, RSS_FEED_URLS))
        tg_fetched = list(pool.map(
            lambda ch: scrape_telegram_channel(ch), TELEGRAM_CHANNELS))
    feed_stats = {}
    rss_items = []
    for feed_url, feed_items in zip(RSS_FEED_URLS, fetched):
        feed_stats[feed_url] = {"status": "ok" if feed_items else "failed",
                                "entries": len(feed_items)}
        rss_items.extend(feed_items)
    tg_items = []
    for channel, ch_items in zip(TELEGRAM_CHANNELS, tg_fetched):
        feed_stats["telegram:" + channel] = {
            "status": "ok" if ch_items else "failed",
            "entries": len(ch_items)}
        tg_items.extend(ch_items)
    # RSS first (cleanest titles, highest tech density), then the fresh
    # Telegram finds; cap the whole run at MAX_ITEMS_PER_RUN.
    items = (rss_items[:MAX_ITEMS_PER_FEED] + tg_items)[:MAX_ITEMS_PER_RUN]
    checked = len(items)
    posted = approved = skipped = 0
    for item in items:
        if posted >= MAX_POSTS_PER_RUN:
            # Post cap reached: leave the rest unmarked so the next run
            # can still post them.
            break
        # Resolve the real merchant link first (short links followed),
        # then dedup on THAT url — the same product from two sources
        # is one deal.
        merchant_url = extract_merchant_link(item)
        if not merchant_url or not _is_merchant(merchant_url):
            skipped += 1
            continue
        did = dedup_doc_id(merchant_url)
        key = product_key(item["title"])
        ref = db.collection("posted_deals").document(did)
        if ref.get().exists:
            skipped += 1
            continue
        if product_posted_recently(db, key):
            ref.set({"url": merchant_url,
                     "source_url": item.get("source_url", item["link"]),
                     "source_feed": item.get("source_feed", ""),
                     "title": item["title"], "product_key": key,
                     "status": "skipped_product_key", "ts": time.time()})
            skipped += 1
            continue
        review = ai_review(item["title"], item["summary"],
                           merchant_url, item["image"])
        status = "rejected"
        if review and review.get("approved"):
            approved += 1
            aff = affiliate_url(merchant_url)
            tg_ok = post_telegram(aff, review)
            pin_ok = post_pinterest(aff, review, item["image"])
            now = time.time()
            db.collection("deals").add({
                "headline": review["headline"],
                "price_tag": review.get("price_tag", "Deal Price"),
                "telegram_html": review.get("telegram_html", ""),
                "pin_description": review.get("pin_description", ""),
                "original_url": merchant_url,
                "source_url": item.get("source_url", item["link"]),
                "source_feed": item.get("source_feed", ""),
                "affiliate_url": aff,
                "image": item["image"],
                "source_title": item["title"],
                "telegram_posted": bool(tg_ok),
                "pinterest_posted": bool(pin_ok),
                "ts": now,
            })
            if tg_ok:
                posted += 1
                db.collection("product_keys").document(key).set(
                    {"ts": now, "title": item["title"],
                     "dedup_id": did})
            status = "posted" if tg_ok else "approved_not_posted"
        ref.set({"url": merchant_url,
                 "source_url": item.get("source_url", item["link"]),
                 "source_feed": item.get("source_feed", ""),
                 "title": item["title"], "product_key": key,
                 "status": status, "ts": time.time()})
    return jsonify({"checked": checked, "approved": approved,
                    "posted": posted, "skipped": skipped,
                    "feeds": feed_stats})


@app.route("/deals", methods=["GET"])
def deals():
    db = get_db()
    if db is None:
        return jsonify({"deals": [], "count": 0,
                        "error": "firestore_unavailable"})
    docs = (db.collection("deals").order_by("ts", direction="DESCENDING")
            .limit(24).stream())
    out = []
    for d in docs:
        data = d.to_dict()
        out.append({
            "id": d.id,
            "headline": data.get("headline", ""),
            "price_tag": data.get("price_tag", ""),
            "affiliate_url": data.get("affiliate_url", ""),
            "image": data.get("image", ""),
            "ts": data.get("ts", 0),
        })
    return jsonify({"deals": out, "count": len(out)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
