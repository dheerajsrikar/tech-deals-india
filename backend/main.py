"""Indian Tech & Mobile Deals — autonomous affiliate agent (backend).

Runs on Google Cloud Run (Always Free Tier) + Firestore (Native Mode).
Every 2 hours Cloud Scheduler calls GET /run?key=CRON_SECRET.

Flow per run:
  1. Fetch each RSS feed in RSS_FEED_URLS (latest 8 items per feed,
     max 10 items processed per run).
  2. For each item, extract the real merchant link (Amazon/Flipkart/...)
     — from the entry itself, or by fetching the deal blog post and
     taking its first merchant link; falls back to the item link.
  3. Skip any merchant URL whose MD5 is already in Firestore
     `posted_deals` (deduplication — a deal is never processed twice).
  4. Ask Gemini (gemini-3.5-flash-lite, free tier) to approve ONLY
     Smartphones, Laptops, Earbuds, Headphones, Smartwatches, Tablets,
     Power Banks, Chargers or Branded Tech Accessories — and to write the
     headline / Telegram alert / Pinterest copy / price tag as strict JSON.
  5. Wrap the store link with the Amazon Associates tag (amazon.in) or
     Cuelinks (every other store).
  6. Post to Telegram (inline "Grab Deal Now" button) and, when configured,
     pin to Pinterest. Save the deal to Firestore `deals` for the website.

Env vars (set on Cloud Run, never committed to git):
  CRON_SECRET, GEMINI_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
  AMAZON_TAG, CUELINKS_ID, PINTEREST_TOKEN, PINTEREST_BOARD_ID
Optional: FIRESTORE_PROJECT (defaults to the GCP project),
  RSS_FEED_URLS (comma-separated; default: the three open Indian deal
  feeds below), RSS_URL (legacy single-feed override).
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
from flask import Flask, jsonify, request
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tech-deals-agent")

app = Flask(__name__)
CORS(app)  # /deals is a public endpoint consumed by GitHub Pages

# DesiDime (the original single source) returns 403 to Cloud Run egress
# and to this VM — its WAF blocks cloud IPs outright (verified 2026-11-22).
# These open Indian deal feeds are the replacements; the Cloud Run live
# test (see DEPLOY-NOTES.md) settles which of them answer 200 from Google.
_DEFAULT_FEEDS = (
    "https://www.indiafreestuff.in/feed,"
    "https://www.smartprix.com/feed/,"
    "https://www.gyftr.com/blog/feed/,"
    "https://www.couponrani.com/feed,"
    "https://www.mysmartprice.com/feed"
)
_RSS_URLS_RAW = os.environ.get("RSS_FEED_URLS", "").strip()
if not _RSS_URLS_RAW:
    _RSS_URLS_RAW = os.environ.get("RSS_URL", "").strip() or _DEFAULT_FEEDS
RSS_FEED_URLS = [u.strip() for u in _RSS_URLS_RAW.split(",") if u.strip()]
MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash-lite")
# Total items processed per run across all feeds (keeps a run well inside
# the Scheduler's 180s attempt deadline: ~10 x (page fetch + Gemini)).
MAX_ITEMS_PER_RUN = int(os.environ.get("MAX_ITEMS_PER_RUN", "10"))
MAX_ITEMS_PER_FEED = int(os.environ.get("MAX_ITEMS_PER_FEED", "8"))

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


def url_hash(url):
    return hashlib.md5(url.strip().encode("utf-8")).hexdigest()


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
        query["tag"] = tag
        # Drop the feed source's own tracking label (e.g. Smartprix's
        # ascsubtag): the `tag` is what decides which Associates account
        # gets paid; the subtag only lets the source track the click.
        query.pop("ascsubtag", None)
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
    payload = {
        "chat_id": chat_id,
        "text": review.get("telegram_html", ""),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "reply_markup": {"inline_keyboard": [[{
            "text": "🔥 Grab Deal Now", "url": aff_url}]]},
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

    items = []
    feed_stats = {}
    # Fetch feeds concurrently: a dead feed costs ~45s of retries, and
    # serial fetching would push the run past the Scheduler's 180s
    # deadline. Concurrently the run costs only the slowest feed.
    with ThreadPoolExecutor(max_workers=len(RSS_FEED_URLS)) as pool:
        fetched = list(pool.map(fetch_feed, RSS_FEED_URLS))
    for feed_url, feed_items in zip(RSS_FEED_URLS, fetched):
        feed_stats[feed_url] = {"status": "ok" if feed_items else "failed",
                                "entries": len(feed_items)}
        items.extend(feed_items)
    items = items[:MAX_ITEMS_PER_RUN]
    checked = len(items)
    posted = approved = skipped = 0
    for item in items:
        # Resolve the real merchant link first, then dedup on THAT url —
        # the same product posted by two blogs is one deal.
        merchant_url = extract_merchant_link(item)
        if not merchant_url:
            skipped += 1
            continue
        digest = url_hash(merchant_url)
        ref = db.collection("posted_deals").document(digest)
        if ref.get().exists:
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
                "source_url": item["link"],
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
            status = "posted" if tg_ok else "approved_not_posted"
        ref.set({"url": merchant_url, "source_url": item["link"],
                 "source_feed": item.get("source_feed", ""),
                 "title": item["title"],
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
