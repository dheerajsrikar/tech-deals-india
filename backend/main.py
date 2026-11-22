"""Indian Tech & Mobile Deals — autonomous affiliate agent (backend).

Runs on Google Cloud Run (Always Free Tier) + Firestore (Native Mode).
Every 2 hours Cloud Scheduler calls GET /run?key=CRON_SECRET.

Flow per run:
  1. Fetch the DesiDime deals RSS (latest 8 items).
  2. Skip any URL whose MD5 is already in Firestore `posted_deals`
     (deduplication — a deal is never processed twice).
  3. Ask Gemini (gemini-3.5-flash-lite, free tier) to approve ONLY
     Smartphones, Laptops, Earbuds, Headphones, Smartwatches, Tablets,
     Power Banks, Chargers or Branded Tech Accessories — and to write the
     headline / Telegram alert / Pinterest copy / price tag as strict JSON.
  4. Wrap the store link with the Amazon Associates tag (amazon.in) or
     Cuelinks (every other store).
  5. Post to Telegram (inline "Grab Deal Now" button) and, when configured,
     pin to Pinterest. Save the deal to Firestore `deals` for the website.

Env vars (set on Cloud Run, never committed to git):
  CRON_SECRET, GEMINI_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
  AMAZON_TAG, CUELINKS_ID, PINTEREST_TOKEN, PINTEREST_BOARD_ID
Optional: FIRESTORE_PROJECT (defaults to the GCP project), RSS_URL.
"""

import hashlib
import hmac
import json
import logging
import os
import re
import time
from urllib.parse import quote, urlparse, parse_qsl, urlencode, urlunparse

import feedparser
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tech-deals-agent")

app = Flask(__name__)
CORS(app)  # /deals is a public endpoint consumed by GitHub Pages

RSS_URL = os.environ.get(
    "RSS_URL", "https://www.desidime.com/deals.rss")
MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash-lite")
MAX_ITEMS_PER_RUN = int(os.environ.get("MAX_ITEMS_PER_RUN", "8"))

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


# ---------------------------------------------------------------------- Feed
def fetch_feed():
    """Fetch + parse the RSS feed. Retries 3x: DesiDime's WAF kills some
    connections mid-transfer, so a single failed attempt proves nothing.
    Returns [] only after all attempts fail (a run then reports
    checked=0 instead of crashing)."""
    for attempt in range(1, 4):
        try:
            r = requests.get(RSS_URL, headers=RSS_HEADERS, timeout=25)
            r.raise_for_status()
            content = r.content  # read inside try: truncated chunked
            # responses raise ChunkedEncodingError here, not on .get()
            if len(content) < 500:
                raise ValueError("feed too small ({} bytes)".format(
                    len(content)))
        except Exception as exc:  # noqa: BLE001
            log.warning("RSS fetch attempt %d/3 failed (%s): %s",
                        attempt, RSS_URL, exc)
            time.sleep(2 * attempt)
            continue
        feed = feedparser.parse(content)
        items = []
        for e in feed.entries[:MAX_ITEMS_PER_RUN]:
            link = (e.get("link") or "").strip()
            if not link:
                continue
            title = re.sub(r"\s+", " ", (e.get("title") or "")).strip()
            summary = re.sub(r"<[^>]+>", " ",
                             (e.get("summary") or e.get("description") or ""))
            summary = re.sub(r"\s+", " ", summary).strip()
            image = ""
            if e.get("media_content"):
                image = e.media_content[0].get("url", "")
            elif e.get("enclosures"):
                image = e.enclosures[0].get("href", "")
            items.append({"title": title, "summary": summary,
                          "link": link, "image": image})
        return items
    return []


# ------------------------------------------------------------------- Routes
@app.route("/", methods=["GET"])
def health():
    return jsonify({"service": "tech-deals-agent", "status": "ok",
                    "model": MODEL, "rss": RSS_URL})


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

    items = fetch_feed()
    checked = len(items)
    posted = approved = skipped = 0
    for item in items:
        digest = url_hash(item["link"])
        ref = db.collection("posted_deals").document(digest)
        if ref.get().exists:
            skipped += 1
            continue
        review = ai_review(item["title"], item["summary"],
                           item["link"], item["image"])
        status = "rejected"
        if review and review.get("approved"):
            approved += 1
            aff = affiliate_url(item["link"])
            tg_ok = post_telegram(aff, review)
            pin_ok = post_pinterest(aff, review, item["image"])
            now = time.time()
            db.collection("deals").add({
                "headline": review["headline"],
                "price_tag": review.get("price_tag", "Deal Price"),
                "telegram_html": review.get("telegram_html", ""),
                "pin_description": review.get("pin_description", ""),
                "original_url": item["link"],
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
        ref.set({"url": item["link"], "title": item["title"],
                 "status": status, "ts": time.time()})
    return jsonify({"checked": checked, "approved": approved,
                    "posted": posted, "skipped": skipped})


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
