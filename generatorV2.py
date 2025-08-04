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
from urllib.parse import urljoin

import aiofiles
import httpx
import pandas as pd
from bs4 import BeautifulSoup, FeatureNotFound
from openai import AsyncOpenAI
from dotenv import load_dotenv
from tqdm.asyncio import tqdm
import re

# ───────── ENV & CONFIG ─────────
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("❌ OPENAI_API_KEY not found in .env")

OPENALEX_API = "https://api.openalex.org/institutions"
MODEL = "gpt-4o-mini"
HEADERS = {"User-Agent": "UniContactsAsync/2.3"}
TIMEOUT = httpx.Timeout(30.0)

PROBE_LIMIT = 15
CONCURRENCY = 12
GPT_CONCURRENCY = 4

TOK_BUCKET = 200_000
TOK_REFRESH = 60.0
COST_IN, COST_OUT = 0.60, 2.40

CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)

# Token bucket state
bucket_lock = asyncio.Lock()
bucket_used = 0
bucket_reset = time.monotonic()

# LLM client & semaphore
OAI = AsyncOpenAI(api_key=OPENAI_API_KEY)
GPT_SEM = asyncio.Semaphore(GPT_CONCURRENCY)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ───────── heuristic slugs (fallback) ─────────
TOKENS = [
    "staff",
    "directory",
    "people",
    "leadership",
    "administration",
    "contacts",
    "recruitment",
    "admissions",
    "rector",
    "chancellor",
    "governance",
    "executive",
    "faculty",
    "personnel",
]
PREFIXES = [
    "",
    "about",
    "about-us",
    "administration",
    "contact",
    "hr",
    "about/leadership",
]
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

# ───────── UTIL ─────────
def tokens_of(text: str) -> int:
    return len(text) // 4  # rough heuristic

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
            sleep = TOK_REFRESH - (now - bucket_reset)
        await asyncio.sleep(sleep)

async def fetch_page(url: str) -> Optional[str]:
    async with httpx.AsyncClient(headers=HEADERS, timeout=TIMEOUT, follow_redirects=True, http2=True) as client:
        try:
            r = await client.get(url)
            r.raise_for_status()
            logging.info("HTTP Request: GET %s %r", url, r.status_code)
            return r.text
        except Exception as e:
            logging.debug("Failed to fetch %s: %s", url, e)
            return None

# ───────── LLM-ASSISTED SLUG SUGGESTION ─────────
async def gpt_suggest_slugs(homepage_text: str, base_url: str) -> List[str]:
    prompt = (
        f"You are a web crawler helping to find contact details for university leadership.\n"
        f"The homepage content is:\n{homepage_text[:1000]}\n\n"
        "Suggest 3 likely internal URL paths (relative to the base URL) that are likely to contain contact info "
        "for university leadership (e.g. chancellor, rector, admissions head). "
        "Only return paths like '/about/leadership' or '/contact'."
    )

    schema_slugs = {
        "name": "slug_suggestion",
        "schema": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
            "maxItems": 5,
        },
    }

    resp = None
    try:
        async with GPT_SEM:
            resp = await OAI.chat.completions.create(
                model=MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful assistant that outputs a JSON array of slug strings according to the provided schema."
                    },
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_schema", "json_schema": schema_slugs},
            )
        slugs = json.loads(resp.choices[0].message.content)
        if isinstance(slugs, list) and slugs:
            return slugs
        logging.warning("Slug suggestion schema returned unexpected shape: %r", slugs)
    except Exception as e:
        raw = ""
        if resp:
            raw = getattr(resp.choices[0].message, "content", "")
        logging.warning("Slug schema suggestion failed (%s). Raw: %s", e, raw)

    # Fallback to loose json_object
    try:
        async with GPT_SEM:
            resp2 = await OAI.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that responds with a JSON array of strings."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
            )
        slugs2 = json.loads(resp2.choices[0].message.content)
        if isinstance(slugs2, list):
            return slugs2
        logging.warning("Fallback slug suggestion returned non-list: %r", slugs2)
    except Exception as e2:
        logging.warning("Fallback slug extraction also failed: %s", e2)
    return []

# ───────── CONTACT PAGE DISCOVERY ─────────
async def find_contact_page_via_crawler(base_url: str, max_depth: int = 5) -> Optional[str]:
    visited = set()
    queue = [(base_url, 0)]

    async with httpx.AsyncClient(headers=HEADERS, timeout=TIMEOUT, follow_redirects=True, http2=True) as client:
        while queue:
            url, depth = queue.pop(0)
            if url in visited or depth > max_depth:
                continue
            visited.add(url)

            try:
                resp = await client.get(url)
                resp.raise_for_status()
                html = resp.text
                logging.info("Crawler visiting %s (depth %d)", url, depth)
            except Exception:
                continue

            if any(kw in html.lower() for kw in ["rector", "chancellor", "admissions", "contact", "president"]):
                if "mailto:" in html:
                    return url

            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("mailto:"):
                    continue
                abs_url = urljoin(url, href)
                if base_url in abs_url and abs_url not in visited:
                    queue.append((abs_url, depth + 1))

    return None

async def find_contact_page(base_url: str) -> Optional[str]:
    homepage = await fetch_page(base_url)
    if not homepage:
        return None

    # 1. AI slug suggestion
    slugs = await gpt_suggest_slugs(homepage, base_url)
    for slug in slugs:
        full_url = urljoin(base_url, slug)
        page = await fetch_page(full_url)
        if page and "mailto:" in page:
            if any(role in page.lower() for role in ["rector", "chancellor", "admissions", "president"]):
                return full_url

    # 2. Recursive crawler fallback
    page = await find_contact_page_via_crawler(base_url)
    if page:
        return page

    # 3. Brute-force old slug list fallback
    for slug in SLUGS:
        full = urljoin(base_url, slug)
        html = await fetch_page(full)
        if not html:
            continue
        if "mailto:" in html:
            return full

    return None

# ───────── CONTACT EXTRACTION ─────────
def simple_regex_contacts(text: str, page_url: str) -> List[Dict[str, str]]:
    contacts = []
    email_pattern = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    for match in email_pattern.finditer(text):
        email = match.group(0).lower()
        start = max(0, match.start() - 100)
        snippet = text[start: match.end() + 100]
        role_search = re.search(
            r"(director|dean|head|officer|coordinator|international|admissions|recruitment|manager|lead)",
            snippet,
            re.IGNORECASE,
        )
        role = role_search.group(0) if role_search else ""
        contacts.append({"role": role, "name": "", "email": email, "page_url": page_url})
    return contacts

async def gpt_extract(text: str, page: str) -> List[Dict[str, str]]:
    if len(text) < 100:
        return []
    snippet = text[:20000]
    prompt = (
        "Extract any named individual with a university-affiliated email address who appears to be in a leadership "
        "or outreach role (e.g. director, dean, coordinator, officer, rector, chancellor, admissions, recruitment, "
        "international, staff). "
        f"Source: {page}\n---\n{snippet}"
    )
    need = tokens_of(prompt) + 500
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
                    {
                        "role": "system",
                        "content": "You are a strict JSON emitter and must output a JSON object conforming to the provided schema."
                    },
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_schema", "json_schema": schema_contacts},
            )
        parsed = json.loads(resp.choices[0].message.content)
        contacts = parsed.get("contacts", [])
        if isinstance(contacts, list):
            return contacts
        logging.warning("Schema extraction returned unexpected contacts type: %r", contacts)
    except Exception as e:
        raw = ""
        if resp:
            raw = getattr(resp.choices[0].message, "content", "")
        logging.warning("Schema contact extraction failed for %s: %s. Raw: %s", page[:60], e, raw)

    # Fallback to looser json_object
    try:
        async with GPT_SEM:
            resp2 = await OAI.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "You are a strict JSON emitter that outputs a JSON object with a 'contacts' array."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
            )
        parsed2 = json.loads(resp2.choices[0].message.content)
        contacts2 = parsed2.get("contacts", [])
        if isinstance(contacts2, list):
            return contacts2
        logging.warning("Fallback object extraction returned unexpected contacts type: %r", contacts2)
    except Exception as e2:
        logging.warning("Fallback contact extraction failed for %s: %s. Using regex fallback.", page[:60], e2)

    # Last-resort regex extraction
    return simple_regex_contacts(text, page)

# ───────── OPENALEX INSTITUTIONS ─────────
async def fetch_openalex_unis(country: str, limit: Optional[int] = None):
    per_page = 200
    cursor = "*"
    seen = 0

    while True:
        url = (
            f"{OPENALEX_API}?filter=country_code:{country}"
            f"&per_page={per_page}&cursor={cursor}"
        )
        try:
            rsp = httpx.get(url, headers=HEADERS, timeout=30).json()
        except Exception as e:
            logging.warning("OpenAlex request failed: %s", e)
            break

        for r in rsp.get("results", []):
            if r.get("homepage_url"):
                yield {"name": r["display_name"], "url": r["homepage_url"]}
                seen += 1
                if limit is not None and seen >= limit:
                    return

        cursor = rsp.get("meta", {}).get("next_cursor")
        if not cursor:
            break

# ───────── PER-UNI PROCESSING ─────────
async def process_uni(
    uni: Dict[str, str],
    country: str,
    sem: asyncio.Semaphore,
    results: List[Dict[str, str]],
    seen: Set[Tuple[str, str]],
    stats: Dict[str, int],
):
    async with sem:
        home = uni["url"].rstrip("/")
        logging.info("Processing university: %s (%s)", uni.get("name"), home)
        page = await find_contact_page(home)
        if not page:
            logging.info("No contact page found for %s", uni.get("name"))
            return

        logging.info("Using page %s for %s", page, uni.get("name"))

        try:
            cached = await read_cache(page)
            html = cached if cached else await fetch_page(page)
            if html:
                await write_cache(page, html)
            else:
                return
        except Exception as e:
            logging.warning("get_html failed for %s: %s", page, e)
            return

        text = bs_text(html)
        try:
            contacts = await gpt_extract(text, page)
        except Exception as e:
            logging.warning("GPT extraction failed for %s: %s", page, e)
            return

        stats["tok_in"] += tokens_of(text[:8000])
        stats["tok_out"] += 500

        for c in contacts:
            email = (c.get("email") or "").lower()
            if not email:
                continue
            key = (uni["name"], email)
            if key in seen:
                continue
            results.append(
                {
                    "University": uni["name"],
                    "Country": country,
                    "Role": c.get("role", ""),
                    "Name": c.get("name", ""),
                    "Email": email,
                    "PageURL": c.get("page_url", page),
                }
            )
            seen.add(key)

# ───────── MAIN & ENTRYPOINT ─────────
async def main(country: str, limit: Optional[int], outfile: str):
    sem = asyncio.Semaphore(CONCURRENCY)
    results: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    stats = {"tok_in": 0, "tok_out": 0}

    unis = [u async for u in fetch_openalex_unis(country, limit)]

    tasks = [process_uni(u, country, sem, results, seen, stats) for u in unis]

    for t in tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="uni", desc="universities"):
        try:
            await t
        except Exception as e:
            logging.warning("[warn] task error: %s", repr(e))

    if not results:
        print("No contacts found")
        return

    df = pd.DataFrame(results).sort_values(["University", "Role"])

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

async def run_all(country: str, limit: Optional[int], outfile: str):
    try:
        await main(country, limit, outfile)
    finally:
        await OAI.aclose()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("country")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--outfile", default="contacts.csv")
    args = p.parse_args()
    asyncio.run(run_all(args.country.upper(), args.limit, args.outfile))
