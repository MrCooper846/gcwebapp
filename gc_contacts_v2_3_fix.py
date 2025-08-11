
#!/usr/bin/env python3
import argparse
import asyncio
import csv
import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import aiofiles
import httpx
import pandas as pd
from bs4 import BeautifulSoup, FeatureNotFound
from dotenv import load_dotenv
from openai import AsyncOpenAI
from tqdm.asyncio import tqdm
import re
from collections import defaultdict

# ───────── ENV & CONFIG ─────────
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("❌ OPENAI_API_KEY not found in .env")

OPENALEX_API = "https://api.openalex.org/institutions"
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

DEFAULT_UA = "UniContactsAsync/3.5 (+debug+csv)"
BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"

HEADERS = {
    "User-Agent": DEFAULT_UA,
    "Accept-Language": "en;q=0.9,*;q=0.6",
}
TIMEOUT = httpx.Timeout(30.0)

PROBE_LIMIT = 24
CONCURRENCY = 12
GPT_CONCURRENCY = 4
PAGINATION_CAP = 5  # pages per directory
CMS_SEARCH_TERMS = ["international", "admissions", "recruitment", "contact", "directory", "people", "global", "engagement", "partnerships"]

TOK_BUCKET = 200_000
TOK_REFRESH = 60.0
COST_IN, COST_OUT = 0.60, 2.40

CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)

bucket_lock = asyncio.Lock()
bucket_used = 0
bucket_reset = time.monotonic()

OAI = AsyncOpenAI(api_key=OPENAI_API_KEY)
GPT_SEM = asyncio.Semaphore(GPT_CONCURRENCY)

HTTP: Optional[httpx.AsyncClient] = None
HOST_SEMS = defaultdict(lambda: asyncio.Semaphore(3))  # per-host throttle

DEBUG_ENABLED = False
DEBUG_DIR = Path("debug_logs")
TRAIN_CSV_PATH: Optional[Path] = None
IGNORE_ROBOTS = False

# Logging default
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger("gc")

# ───────── heuristic slugs (fallback) ─────────
TOKENS = [
    "staff", "directory", "people", "leadership", "administration",
    "contacts", "recruitment", "admissions", "rector", "chancellor",
    "governance", "executive", "faculty", "personnel",
    "international", "global", "partnerships", "relations", "engagement",
]
PREFIXES = ["", "about", "about-us", "administration", "contact", "hr", "about/leadership"]
SLUGS = [""] + [
    v
    for p in PREFIXES
    for t in TOKENS
    for v in {
        f"/{p}/{t}" if p else f"/{t}",
        f"/{p}/{t}/" if p else f"/{t}/",
        f"/{p}/{t}/index.html" if p else f"/{t}/index.html",
        f"/{p}/{t}.html" if p else f"/{t}.html",
    }
]
SUBDOMS = ["international", "global", "admissions", "apply", "about", "contact"]

ALLOWED_ROLE_WORDS = r"(international|global|admissions?|recruit(ment|er)?|partnerships?|relations?|engagement|enrol?ment|outreach|external|mobility|exchange|rector|chancellor|president|provost|vice[- ]?chancellor|prorector|vice[- ]?president)"
SENIORITY = r"(head|director|chief|deputy|associate|assistant|manager|lead|officer|coordinator|vice|pro)"
INTL_HINTS = r"(国际|招生|留学生|国際|入試|国际|国际处|relaciones internacionales|admisiones|rekrutacja|échanges|relations internationales|internacional|mobilidade|auslands|internationale)"

GENERIC_EMAIL = re.compile(r"^(info|enquiries|enquiry|contact|office|support|hello|admissions|international|ug|pg|postgrad|undergrad|apply|students|noreply)[\+\.-]?", re.I)
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
OBFUSCATED = re.compile(r'([A-Za-z0-9._%+-]+)\s*(?:\[?at\]?|@)\s*([A-Za-z0-9.-]+)\s*(?:\[?dot\]?|\.)\s*([A-Za-z]{2,})', re.I)

from urllib.robotparser import RobotFileParser
_ROBOTS: dict[str, RobotFileParser] = {}

# ───────── UTIL ─────────
def tokens_of(text: str) -> int:
    return max(1, len(text) // 4)

def cache_path(url: str) -> Path:
    return CACHE_DIR / (hashlib.sha1(url.encode()).hexdigest() + ".html")

async def read_cache(url: str) -> Optional[str]:
    p = cache_path(url)
    if p.exists():
        async with aiofiles.open(p, "r", encoding="utf-8") as f:
            return await f.read()
    return None

async def write_cache(url: str, html: str) -> None:
    async with aiofiles.open(cache_path(url), "w", encoding="utf-8") as f:
        await f.write(html)

def bs_text(html: str) -> str:
    try:
        soup = BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text("\n", strip=True)

async def reserve(need: int):
    global bucket_used, bucket_reset
    while True:
        async with bucket_lock:
            now = time.monotonic()
            if now - bucket_reset >= TOK_REFRESH:
                bucket_used, bucket_reset = 0, now
            if bucket_used + need <= TOK_BUCKET:
                bucket_used += need
                return
            sleep = max(0.1, TOK_REFRESH - (now - bucket_reset))
        await asyncio.sleep(sleep)

def normalize_url(u: str) -> str:
    if not u:
        return u
    p = urlparse(u)
    p = p._replace(fragment="")
    return urlunparse(p)

async def allowed(url: str) -> bool:
    if IGNORE_ROBOTS:
        return True
    try:
        parsed = urlparse(url)
        host = parsed.scheme + "://" + parsed.netloc
        rp = _ROBOTS.get(host)
        if not rp:
            robots_url = host.rstrip("/") + "/robots.txt"
            host_sem = HOST_SEMS[parsed.netloc]
            async with host_sem:
                try:
                    r = await HTTP.get(robots_url, timeout=TIMEOUT, follow_redirects=True)
                    txt = r.text if r.status_code == 200 else ""
                except Exception:
                    txt = ""
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.parse((txt or "").splitlines())
            _ROBOTS[host] = rp
        return rp.can_fetch(HEADERS.get("User-Agent","*"), url)
    except Exception:
        return True

async def get_with_retry(url: str, tries=3) -> Optional[httpx.Response]:
    host = urlparse(url).netloc
    async with HOST_SEMS[host]:
        for i in range(tries):
            try:
                r = await HTTP.get(url, timeout=TIMEOUT, follow_redirects=True)
                r.raise_for_status()
                return r
            except Exception as e:
                LOG.debug("GET fail %s try %d: %s", url, i+1, e)
                await asyncio.sleep(0.5 * (2 ** i))
        LOG.debug("GET exhausted %s", url)
        return None

async def fetch_page(url: str, expect_html: bool=True) -> Optional[str]:
    url = normalize_url(url)
    if not await allowed(url):
        LOG.debug("robots disallow: %s", url)
        return None
    cached = await read_cache(url)
    if cached is not None:
        return cached
    r = await get_with_retry(url)
    if not r:
        return None
    if expect_html:
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "text/html" not in ctype and "application/xhtml" not in ctype and "xml" not in ctype:
            LOG.debug("skip non-HTML %s (ctype=%s)", url, ctype)
            return None
    text = r.text
    await write_cache(url, text)
    return text

# ───────── HREFLANG → ENGLISH ─────────
def pick_hreflang_en(home_html: str, home_url: str) -> Tuple[str,bool]:
    hop = False
    try:
        soup = BeautifulSoup(home_html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(home_html, "html.parser")
    links = soup.find_all("link", rel=lambda v: v and "alternate" in v)
    best = None
    for l in links:
        lang = (l.get("hreflang") or "").lower()
        href = l.get("href")
        if not href:
            continue
        if lang == "en":
            best = href
            break
        if lang.startswith("en-") and not best:
            best = href
    if best:
        hop = True
        return urljoin(home_url, best), hop
    return home_url, hop

# ───────── LLM-ASSISTED SLUG SUGGESTION ─────────
async def gpt_suggest_slugs(homepage_text: str, base_url: str) -> List[str]:
    prompt = (
        f"You are a web crawler helping find contact or staff directory pages for university leadership and international recruitment/admissions.\n"
        f"Homepage sample:\n{homepage_text[:1200]}\n\n"
        "Return up to 5 likely internal URL paths (relative) that contain people directories or contact info "
        "for leaders (chancellor/rector/president) or international/global admissions/recruitment/engagement offices. "
        "Examples: '/about/leadership', '/contact', '/international/contact', '/people', '/directory'. "
        "Only return an array of strings like '/path'."
    )

    schema_slugs = {
        "name": "slug_suggestion",
        "schema": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 5},
    }

    need = tokens_of(prompt) + 200
    await reserve(need)

    try:
        async with GPT_SEM:
            resp = await OAI.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "You output a JSON array of slug strings per the provided schema."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_schema", "json_schema": schema_slugs},
            )
        slugs = json.loads(resp.choices[0].message.content)
        if isinstance(slugs, list) and slugs:
            return [s if s.startswith("/") else f"/{s}" for s in slugs]
    except Exception:
        pass

    # fallback
    try:
        async with GPT_SEM:
            resp2 = await OAI.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "Return ONLY a JSON array of strings."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
            )
        slugs2 = json.loads(resp2.choices[0].message.content)
        if isinstance(slugs2, list):
            return [s if s.startswith("/") else f"/{s}" for s in slugs2]
    except Exception:
        pass
    return []

# ───────── CANDIDATE DISCOVERY ─────────
KEYWORDS = [
    "contact","contacts","directory","people","staff","leadership","governance","president","chancellor","rector",
    "admissions","international","global","recruitment","relations","partnerships","engagement","mobility","exchange",
]

def score_candidate(url: str, anchor_text: str="") -> float:
    url_l = url.lower()
    score = 0.0
    for k in KEYWORDS:
        if f"/{k}" in url_l or url_l.endswith(k) or k in anchor_text.lower():
            score += 2.5
    if "/international" in url_l or "/admissions" in url_l: score += 2.0
    if "/about" in url_l or "/leadership" in url_l: score += 1.5
    depth = url_l.count("/")
    score -= max(0, depth - 6) * 0.5
    if any(url_l.endswith(ext) for ext in (".pdf",".jpg",".png",".zip",".doc",".ppt",".xls",".xlsx",".js",".css")):
        score -= 5.0
    if "/events" in url_l or "/news" in url_l: score -= 1.5
    if "/student" in url_l and "/international" not in url_l: score -= 0.5
    if "/en/" in url_l or url_l.endswith("/en"): score += 0.8
    return score

async def extract_nav_candidates(html: str, base_url: str) -> List[Tuple[str,str]]:
    try:
        soup = BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(html, "html.parser")
    anchors = []
    for section in soup.find_all(["nav","footer","header"]):
        for a in section.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(" ", strip=True)[:120]
            if any(k in (href.lower() + " " + text.lower()) for k in KEYWORDS):
                anchors.append((urljoin(base_url, href), text))
    for a in soup.find_all("a", href=True, limit=300):
        href = a["href"]; text = a.get_text(" ", strip=True)[:120]
        if any(k in (href.lower() + " " + text.lower()) for k in KEYWORDS):
            anchors.append((urljoin(base_url, href), text))
    seen = set()
    out = []
    for u,t in anchors:
        u = normalize_url(u)
        if u not in seen:
            seen.add(u); out.append((u,t))
    return out

async def discover_sitemap_urls(base_url: str) -> List[str]:
    root = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}"
    candidates = [root + p for p in ["/sitemap.xml","/sitemap_index.xml","/sitemap-index.xml"]]
    urls = []
    for sm in candidates:
        xml = await fetch_page(sm, expect_html=False)
        if not xml:
            continue
        for m in re.finditer(r"<loc>\s*([^<\s]+)\s*</loc>", xml, re.I):
            u = m.group(1).strip()
            if urlparse(u).netloc == urlparse(root).netloc:
                urls.append(u)
        if len(urls) > 5000:
            break
    urls = list({normalize_url(u) for u in urls})
    urls = [u for u in urls if any(kw in u.lower() for kw in KEYWORDS)]
    return urls[:300]

def subdomain_candidates(base_url: str) -> List[str]:
    p = urlparse(base_url)
    dom = p.netloc
    base_root = ".".join(dom.split(".")[-2:])
    outs = []
    for sd in SUBDOMS:
        outs.append(f"{p.scheme}://{sd}.{base_root}")
        outs.append(f"{p.scheme}://{sd}.{base_root}/")
    return outs

def is_wordpress(html: str) -> bool:
    return "wp-content" in html or "WordPress" in html or "wp-json" in html
def is_drupal(html: str) -> bool:
    return "drupal-settings-json" in html or "drupal" in html.lower()

async def wp_api_root(base_url: str) -> Optional[str]:
    root = urljoin(base_url, "/wp-json/")
    r = await get_with_retry(root)
    if r and (r.headers.get("Content-Type") or "").lower().startswith("application/json"):
        return root
    return None

async def wp_search_urls(base_url: str) -> List[str]:
    root = await wp_api_root(base_url)
    if not root:
        return []
    urls = []
    for term in CMS_SEARCH_TERMS:
        q = f"{root}wp/v2/search?search={term}&per_page=20&subtype=page"
        r = await get_with_retry(q)
        if r and r.status_code == 200:
            try:
                data = r.json()
                for item in data:
                    url = item.get("url")
                    if url:
                        urls.append(url)
            except Exception:
                pass
        q2 = f"{root}wp/v2/pages?search={term}&per_page=20&_fields=link"
        r2 = await get_with_retry(q2)
        if r2 and r2.status_code == 200:
            try:
                data = r2.json()
                for p in data:
                    url = p.get("link")
                    if url:
                        urls.append(url)
            except Exception:
                pass
    return list({normalize_url(u) for u in urls})

async def drupal_jsonapi_root(base_url: str) -> Optional[str]:
    root = urljoin(base_url, "/jsonapi/")
    r = await get_with_retry(root)
    if r and r.status_code == 200 and (r.headers.get("Content-Type") or "").lower().startswith("application/vnd.api+json"):
        return root
    return None

async def drupal_search_urls(base_url: str) -> List[str]:
    root = await drupal_jsonapi_root(base_url)
    if not root:
        return []
    urls = []
    node_types = ["page", "news", "person", "people", "staff", "team", "directory"]
    for t in node_types:
        for term in CMS_SEARCH_TERMS:
            q = f"{root}node/{t}?filter[fulltext]={term}&page[limit]=25"
            r = await get_with_retry(q)
            if r and r.status_code == 200:
                try:
                    data = r.json()
                    for item in data.get("data", []):
                        attrs = item.get("attributes", {})
                        alias = attrs.get("path", {}).get("alias")
                        if alias:
                            urls.append(urljoin(base_url, alias))
                        else:
                            nid = attrs.get("drupal_internal__nid")
                            if nid:
                                urls.append(urljoin(base_url, f"/node/{nid}"))
                except Exception:
                    pass
    return list({normalize_url(u) for u in urls})

# ---- helper to parse features from URL/html
def url_features(u: str) -> Dict[str, object]:
    p = urlparse(u)
    path_tokens = [t for t in p.path.split("/") if t]
    ext = ""
    if "." in (path_tokens[-1] if path_tokens else ""):
        ext = (path_tokens[-1].split(".")[-1] or "").lower()
    return {
        "depth": p.path.count("/"),
        "path_tokens": len(path_tokens),
        "subdomain": ".".join(p.netloc.split(".")[:-2]) if p.netloc.count(".") >= 2 else "",
        "ext": ext if ext else "",
    }

# ───────── DEBUG CSV HELPERS ─────────
TRAIN_HEADERS = [
    "university","country","homepage","candidate_url","source_type","anchor_text","heuristic_score",
    "raw_contacts","kept_contacts","page_length","mailto_count",
    "depth","path_tokens","subdomain","ext",
    "cms_wordpress","cms_drupal","hreflang_en_hop",
    "Label","ReasonCode","Notes"
]

async def append_training_row(row: Dict[str, object]):
    if not DEBUG_ENABLED or not TRAIN_CSV_PATH:
        return
    header = not TRAIN_CSV_PATH.exists()
    async with aiofiles.open(TRAIN_CSV_PATH, "a", encoding="utf-8", newline="") as f:
        if header:
            await f.write(",".join(TRAIN_HEADERS) + "\n")
        vals = []
        for h in TRAIN_HEADERS:
            v = row.get(h, "")
            if v is None: v = ""
            s = str(v).replace("\n"," ").replace("\r"," ").replace(","," ")
            vals.append(s)
        await f.write(",".join(vals) + "\n")

# ───────── GATHER CANDIDATES (with source metadata) ─────────
async def gather_candidates(home_url: str) -> Tuple[List[Dict[str,object]], bool, bool, bool]:
    home_html = await fetch_page(home_url)
    if not home_html:
        return [], False, False, False

    # hreflang hop
    en_url, hopped = pick_hreflang_en(home_html, home_url)
    if en_url != home_url:
        home_url = en_url
        home_html = await fetch_page(home_url) or home_html

    cms_wp = is_wordpress(home_html)
    cms_drupal = is_drupal(home_html)

    cands: List[Dict[str,object]] = []

    # LLM slugs
    try:
        slugs = await gpt_suggest_slugs(home_html, home_url)
    except Exception:
        slugs = []
    for s in slugs:
        u = urljoin(home_url, s)
        cands.append({"url": normalize_url(u), "source_type": "llm", "anchor_text": ""})

    # nav/footer
    for u,t in await extract_nav_candidates(home_html, home_url):
        cands.append({"url": normalize_url(u), "source_type": "nav", "anchor_text": t})

    # sitemap
    for u in await discover_sitemap_urls(home_url):
        cands.append({"url": normalize_url(u), "source_type": "sitemap", "anchor_text": ""})

    # heuristic slugs
    for s in SLUGS:
        u = urljoin(home_url, s)
        cands.append({"url": normalize_url(u), "source_type": "heuristic", "anchor_text": ""})

    # subdomains
    for u in subdomain_candidates(home_url):
        cands.append({"url": normalize_url(u), "source_type": "subdomain", "anchor_text": ""})

    # CMS APIs
    if cms_wp:
        for u in await wp_search_urls(home_url):
            cands.append({"url": normalize_url(u), "source_type": "wp", "anchor_text": ""})
    if cms_drupal:
        for u in await drupal_search_urls(home_url):
            cands.append({"url": normalize_url(u), "source_type": "drupal", "anchor_text": ""})

    # de-dupe & score
    seen = set()
    scored: List[Dict[str,object]] = []
    for c in cands:
        u = c["url"]
        if u in seen: 
            continue
        seen.add(u)
        sc = score_candidate(u, c.get("anchor_text","") or "")
        c["heuristic_score"] = sc
        scored.append(c)

    scored.sort(key=lambda x: x["heuristic_score"], reverse=True)
    return scored[:PROBE_LIMIT], cms_wp, cms_drupal, hopped

# ───────── CONTACT EXTRACTION ─────────
def decode_js_emails(html: str) -> List[str]:
    emails = []
    for m in re.finditer(r'["\']([A-Za-z0-9._%+-]+)["\']\s*\+\s*["\']@["\']\s*\+\s*["\']([A-Za-z0-9.-]+\.[A-Za-z]{2,})["\']', html):
        emails.append((m.group(1) + "@" + m.group(2)).lower())
    for m in re.finditer(r'["\']([A-Za-z0-9._%+-]+)["\']\s*\+\s*["\']@["\']\s*\+\s*["\']([A-Za-z0-9.-]+)["\']\s*\+\s*["\']\.([A-Za-z]{2,})["\']', html):
        emails.append((m.group(1) + "@" + m.group(2) + "." + m.group(3)).lower())
    for m in re.finditer(r'data-user=["\']([A-Za-z0-9._%+-]+)["\']\s+data-domain=["\']([A-Za-z0-9.-]+\.[A-Za-z]{2,})["\']', html, re.I):
        emails.append((m.group(1) + "@" + m.group(2)).lower())
    return list({e for e in emails})

def deobfuscate(text: str) -> List[str]:
    emails = []
    for m in OBFUSCATED.finditer(text):
        emails.append(f"{m.group(1)}@{m.group(2)}.{m.group(3)}".lower())
    return emails

def simple_regex_contacts(text: str, page_url: str, extra_emails: Optional[List[str]] = None) -> List[Dict[str, str]]:
    contacts = []
    for match in EMAIL_RE.finditer(text):
        email = match.group(0).lower()
        start = max(0, match.start() - 120)
        snippet = text[start: match.end() + 120]
        name_search = re.search(r"([A-Z][a-z]+(?:[\-\s][A-Z][a-z]+)+)", snippet)
        role_search = re.search(
            r"(director|dean|head|officer|coordinator|international|admissions|recruitment|manager|lead|regional|global|partnerships?)",
            snippet, re.IGNORECASE,
        )
        contacts.append({
            "role": role_search.group(0) if role_search else "",
            "name": name_search.group(0) if name_search else "",
            "email": email,
            "page_url": page_url
        })
    for email in (extra_emails or []) + deobfuscate(text):
        contacts.append({"role":"", "name":"", "email":email, "page_url":page_url})
    return contacts

def role_score(role: str) -> int:
    r = role.lower()
    base = 0
    if re.search(ALLOWED_ROLE_WORDS, r): base += 5
    if re.search(SENIORITY, r): base += 3
    if any(k in r for k in ["director","head","chancellor","president","rector","provost","vice-chancellor","vice president","vice-president"]): base += 4
    return base

def email_is_generic(addr: str) -> bool:
    local = addr.split("@",1)[0]
    return bool(GENERIC_EMAIL.match(local))

def keep_contact(c: dict, home_domain: str) -> Tuple[bool,int,str]:
    name = (c.get("name") or "").strip()
    email = (c.get("email") or "").lower()
    role  = (c.get("role") or "")
    if not email or "@" not in email: 
        return (False,0,"no email")
    if email_is_generic(email):
        return (False,0,"generic inbox")
    if len([t for t in re.split(r"[^\w]+", name) if t]) < 2:
        return (False,0,"no personal name")
    dom = email.split("@",1)[1]
    ok_domain = home_domain.endswith(dom) or dom.endswith(home_domain)
    score = role_score(role)
    if not ok_domain: score -= 2
    if re.search(INTL_HINTS, role, re.I): score += 2
    return (score >= 6, score, "ok" if score>=6 else "low score")

async def gpt_extract(text: str, page: str) -> List[Dict[str, str]]:
    if len(text) < 100:
        return []
    snippet = text[:18000]
    prompt = (
        "Extract named individuals with university-affiliated email addresses who are involved in: "
        "International Recruitment/Admissions/Office, Global Engagement/Partnerships/Relations, Mobility/Exchange, or top leadership "
        "(Chancellor/President/Rector/Vice-Chancellor/Provost). "
        "For each person, return role/title, full name, email, and the page_url. "
        "Exclude generic inboxes (info@ admissions@ international@). "
        f"Source: {page}\n---\n{snippet}"
    )
    need = tokens_of(prompt) + 600
    await reserve(need)

    schema_contacts = {
        "name": "contacts_extraction",
        "schema": {
            "type": "object",
            "properties": {
                "contacts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "role": {"type": "string"},
                            "name": {"type": "string"},
                            "email": {"type": "string"},
                            "page_url": {"type": "string"},
                        },
                        "required": ["email"],
                    },
                }
            },
            "required": ["contacts"],
        },
    }

    resp = None
    try:
        async with GPT_SEM:
            resp = await OAI.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "Output JSON strictly matching the provided schema."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_schema", "json_schema": schema_contacts},
            )
        parsed = json.loads(resp.choices[0].message.content)
        contacts = parsed.get("contacts", [])
        if isinstance(contacts, list):
            return contacts
    except Exception:
        pass

    try:
        async with GPT_SEM:
            resp2 = await OAI.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "Return a JSON object with a 'contacts' array."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
            )
        parsed2 = json.loads(resp2.choices[0].message.content)
        contacts2 = parsed2.get("contacts", [])
        if isinstance(contacts2, list):
            return contacts2
    except Exception:
        pass

    return []

# ───────── OPENALEX ─────────
async def fetch_openalex_unis(country: str, limit: Optional[int] = None):
    per_page = 200
    cursor = "*"
    seen = 0
    root = f"{OPENALEX_API}?filter=country_code:{country}&per_page={per_page}"
    while True:
        url = f"{root}&cursor={cursor}"
        try:
            r = await HTTP.get(url)
            r.raise_for_status()
            rsp = r.json()
        except Exception as e:
            LOG.warning("OpenAlex request failed: %s", e)
            await asyncio.sleep(2)
            continue

        for r in rsp.get("results", []):
            home = r.get("homepage_url")
            if home:
                yield {"name": r["display_name"], "url": home}
                seen += 1
                if limit is not None and seen >= limit:
                    return

        cursor = rsp.get("meta", {}).get("next_cursor")
        if not cursor:
            break

# ───────── MAIN PER-UNI ─────────
def home_domain_of(url: str) -> str:
    return urlparse(url).netloc

async def fetch_and_extract_single(url: str) -> Tuple[str, List[Dict[str,str]], str, int, int]:
    html = await fetch_page(url)
    if not html:
        return url, [], "", 0, 0
    mailtos = html.lower().count("mailto:")
    text = bs_text(html)
    extra_emails = decode_js_emails(html)
    try:
        gpt_contacts = await gpt_extract(text, url)
    except Exception as e:
        LOG.debug("gpt_extract failed on %s: %s", url, e)
        gpt_contacts = []
    regex_contacts = simple_regex_contacts(text, url, extra_emails=extra_emails)
    contacts = gpt_contacts + regex_contacts
    return url, contacts, text, len(html), mailtos

def find_pagination_links(html: str, base_url: str) -> List[str]:
    try:
        soup = BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", rel=lambda v: v and "next" in v):
        if a.get("href"):
            links.add(urljoin(base_url, a["href"]))
    for a in soup.find_all("a", href=True):
        cls = " ".join(a.get("class", []))
        if any(k in cls.lower() for k in ["pager", "pagination", "nav", "next"]):
            links.add(urljoin(base_url, a["href"]))
        href = a["href"]
        if re.search(r"[?&]page=\d+", href) or re.search(r"/page/\d+/?", href):
            links.add(urljoin(base_url, href))
    return list({normalize_url(u) for u in links})

async def fetch_and_extract_with_pagination(candidate_url: str) -> Tuple[str, List[Dict[str,str]], str, int, int]:
    url, contacts, text, page_len, mailtos = await fetch_and_extract_single(candidate_url)
    if not text:
        return url, contacts, text, page_len, mailtos
    html = await read_cache(candidate_url)
    if not html:
        return url, contacts, text, page_len, mailtos
    next_links = find_pagination_links(html, candidate_url)[:PAGINATION_CAP]
    for nxt in next_links:
        _, c_more, t_more, pl_more, mt_more = await fetch_and_extract_single(nxt)
        contacts.extend(c_more)
        text += "\n" + (t_more or "")
        page_len += pl_more
        mailtos += mt_more
    return url, contacts, text, page_len, mailtos

def safe_slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)[:80]

async def write_debug_json(uni_name: str, data: dict):
    if not DEBUG_ENABLED:
        return
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"{safe_slug(uni_name)}.json"
    path = DEBUG_DIR / fname
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(data, ensure_ascii=False, indent=2))

async def process_uni(
    uni: Dict[str, str],
    country: str,
    sem: asyncio.Semaphore,
    results: List[Dict[str, str]],
    seen: Set[Tuple[str, str]],
    stats: Dict[str, int],
    emit_all: bool,
):
    async with sem:
        home = uni["url"].rstrip("/")
        LOG.info("Processing university: %s (%s)", uni.get("name"), home)

        # gather candidates with metadata
        cands, cms_wp, cms_drupal, hopped = await gather_candidates(home)
        if not cands:
            LOG.info("No candidate pages for %s", uni.get("name"))
            if DEBUG_ENABLED:
                await write_debug_json(uni.get("name","unknown"), {
                    "university": uni.get("name"),
                    "home_url": home,
                    "candidates_ranked": [],
                    "probed_pages": [],
                    "best_page": None,
                    "note": "no candidates"
                })
            return

        limit = min(len(cands), PROBE_LIMIT)
        tasks = [fetch_and_extract_with_pagination(cands[i]["url"]) for i in range(limit)]
        best_hits: List[Tuple[str,int]] = []  # (url, num_kept)
        probed_dbg: List[dict] = []
        home_dom = home_domain_of(home)

        for i, fut in enumerate(tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="page", desc=f"{uni.get('name','uni')} pages", leave=False)):
            try:
                page_url, contacts, text, page_len, mailtos = await fut
            except Exception as e:
                LOG.debug("candidate failed: %s", e)
                continue

            stats["tok_in"] += tokens_of(text[:8000]) if text else 0
            stats["tok_out"] += 600

            raw_count = len(contacts)
            kept_local = 0
            for c in contacts:
                email = (c.get("email") or "").lower()
                if not email:
                    continue
                key = (uni["name"], email)
                if key in seen:
                    continue

                ok,score,reason = keep_contact(c, home_dom)
                row = {
                    "University": uni["name"],
                    "Country": country,
                    "Role": c.get("role",""),
                    "Name": c.get("name",""),
                    "Email": email,
                    "PageURL": c.get("page_url", page_url),
                    "Score": score,
                    "Reason": reason,
                }
                if emit_all or ok:
                    results.append(row)
                if ok:
                    kept_local += 1
                    seen.add(key)

            best_hits.append((page_url, kept_local))

            # write training row
            cand = cands[i] if i < len(cands) else {"source_type":"unknown","anchor_text":"","heuristic_score":0.0}
            feats = url_features(page_url)
            await append_training_row({
                "university": uni["name"],
                "country": country,
                "homepage": home,
                "candidate_url": page_url,
                "source_type": cand.get("source_type",""),
                "anchor_text": cand.get("anchor_text",""),
                "heuristic_score": cand.get("heuristic_score",0.0),
                "raw_contacts": raw_count,
                "kept_contacts": kept_local,
                "page_length": page_len,
                "mailto_count": mailtos,
                "depth": feats["depth"],
                "path_tokens": feats["path_tokens"],
                "subdomain": feats["subdomain"],
                "ext": feats["ext"],
                "cms_wordpress": int(cms_wp),
                "cms_drupal": int(cms_drupal),
                "hreflang_en_hop": int(hopped),
                "Label": "",
                "ReasonCode": "",
                "Notes": "",
            })

            if DEBUG_ENABLED:
                probed_dbg.append({
                    "url": page_url,
                    "raw_contacts": raw_count,
                    "kept_contacts": kept_local
                })

        best_hits.sort(key=lambda x: x[1], reverse=True)
        best_page = {"url": best_hits[0][0], "kept_contacts": best_hits[0][1]} if best_hits else None

        if DEBUG_ENABLED:
            await write_debug_json(uni.get("name","unknown"), {
                "university": uni.get("name"),
                "home_url": home,
                "candidates_ranked": [{"url": c["url"], "score": c["heuristic_score"], "source": c["source_type"]} for c in cands[:PROBE_LIMIT]],
                "probed_pages": probed_dbg,
                "best_page": best_page
            })

# ───────── MAIN & ENTRYPOINT ─────────
async def main(country: str, limit: Optional[int], outfile: str, emit_all: bool, debug: bool, debug_dir: Optional[str], ignore_robots: bool, verbose: bool, browser_ua: bool, on_progress=None):
    global HTTP, DEBUG_ENABLED, DEBUG_DIR, TRAIN_CSV_PATH, IGNORE_ROBOTS, HEADERS
    DEBUG_ENABLED = debug
    IGNORE_ROBOTS = ignore_robots
    if browser_ua:
        HEADERS["User-Agent"] = BROWSER_UA
    if debug_dir:
        DEBUG_DIR = Path(debug_dir)
    if DEBUG_ENABLED:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        TRAIN_CSV_PATH = DEBUG_DIR / "debug_training_data.csv"
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        LOG.setLevel(logging.DEBUG)

    HTTP = httpx.AsyncClient(headers=HEADERS, timeout=TIMEOUT, follow_redirects=True, http2=True)
    try:
        sem = asyncio.Semaphore(CONCURRENCY)
        results: List[Dict[str, str]] = []
        seen: Set[Tuple[str, str]] = set()
        stats = {"tok_in": 0, "tok_out": 0}

        unis = [u async for u in fetch_openalex_unis(country, limit)]
        if not unis:
            print("No universities found from OpenAlex; check country code.")
            return
        total = len(unis)
        if on_progress:
            on_progress({"event": "start", "total": total})

        done = 0
        for t in tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="uni", desc="universities"):
            try:
                await t
            except Exception as e:
                LOG.warning("[warn] task error: %s", repr(e))
            finally:
                done += 1
                if on_progress:
                    on_progress({"event": "tick", "done": done, "total": total})

        if on_progress:
            on_progress({"event": "finish", "outfile": outfile})

        tasks = [process_uni(u, country, sem, results, seen, stats, emit_all) for u in unis]

        for t in tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="uni", desc="universities"):
            try:
                await t
            except Exception as e:
                LOG.warning("[warn] task error: %s", repr(e))

        if not results:
            print("No contacts found")
            return

        df = pd.DataFrame(results).sort_values(["University", "Score"], ascending=[True, False])
        if Path(outfile).suffix.lower() in {".xlsx", ".xls"}:
            try:
                df.to_excel(outfile, index=False)
            except ModuleNotFoundError:
                outfile = str(Path(outfile).with_suffix(".csv"))
                df.to_csv(outfile, index=False, quoting=csv.QUOTE_MINIMAL)
        else:
            df.to_csv(outfile, index=False, quoting=csv.QUOTE_MINIMAL)

        dollars = (stats["tok_in"] / 1000) * COST_IN + (stats["tok_out"] / 1000) * COST_OUT
        print(f"✅ {len(df)} rows  →  {outfile}\n≈{stats['tok_in']}/{stats['tok_out']} tokens  ≈  ${dollars:.2f}")
        if DEBUG_ENABLED:
            print(f"📝 Debug JSON files + training CSV at: {DEBUG_DIR.resolve()}")
    finally:
        await OAI.aclose()
        if HTTP:
            await HTTP.aclose()

async def run_all(country: str, limit: Optional[int], outfile: str, emit_all: bool, debug: bool, debug_dir: Optional[str], ignore_robots: bool, verbose: bool, browser_ua: bool, on_progress=None):
    await main(country, limit, outfile, emit_all, debug, debug_dir, ignore_robots, verbose, browser_ua)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("country", help="ISO country code, e.g., GB, US, CN")
    p.add_argument("--limit", type=int, default=None, help="limit number of universities from OpenAlex")
    p.add_argument("--outfile", default="contacts.csv", help="output CSV/XLSX path")
    p.add_argument("--emit-all", action="store_true", help="emit all contacts with Score/Reason (not only high-confidence)")
    p.add_argument("--debug", action="store_true", help="write per-university debug JSON and a training CSV")
    p.add_argument("--debug-dir", default="debug_logs", help="directory for debug files")
    p.add_argument("--ignore-robots", action="store_true", help="ignore robots.txt (be respectful; use for testing only)")
    p.add_argument("--verbose", action="store_true", help="enable verbose DEBUG logging")
    p.add_argument("--browser-ua", action="store_true", help="use a standard browser User-Agent")
    args = p.parse_args()
    asyncio.run(run_all(args.country.upper(), args.limit, args.outfile, args.emit_all, args.debug, args.debug_dir, args.ignore_robots, args.verbose, args.browser_ua))
