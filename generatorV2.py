
#!/usr/bin/env python3
"""
generatorV2.py – async university-contact scraper (token-bucket safe)
"""
import argparse, asyncio, csv, hashlib, json, os, time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import aiofiles, httpx, pandas as pd
from bs4 import BeautifulSoup, FeatureNotFound
from openai import AsyncOpenAI
from dotenv import load_dotenv
from tqdm.asyncio import tqdm

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("❌ OPENAI_API_KEY not found in .env")

# ───────── CONFIG ─────────
OPENALEX_API = "https://api.openalex.org/institutions"
MODEL        = "gpt-4o-mini"
HEADERS      = {"User-Agent": "UniContactsAsync/2.2"}
TIMEOUT      = httpx.Timeout(10.0)

PROBE_LIMIT      = 15
CONCURRENCY      = 12
GPT_CONCURRENCY  = 4

TOK_BUCKET   = 200_000
TOK_REFRESH  = 60.0

CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)

COST_IN, COST_OUT = 0.60, 2.40

def tokens_of(text: str) -> int: return len(text) // 4

TOKENS   = ["staff","directory","people","leadership","administration",
            "contacts","recruitment","admissions","rector","chancellor",
            "governance","executive","faculty","personnel"]
PREFIXES = ["","about","about-us","administration","contact","hr","about/leadership"]
SLUGS    = [""] + [v for p in PREFIXES for t in TOKENS for v in {
               f"/{p}/{t}"            if p else f"/{t}",
               f"/{p}/{t}/"           if p else f"/{t}/",
               f"/{p}/{t}/index.html" if p else f"/{t}/index.html",
               f"/{p}/{t}.html"       if p else f"/{t}.html"}]

bucket_lock   = asyncio.Lock()
bucket_used   = 0
bucket_reset  = time.monotonic()

async def reserve(need:int):
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

def cache_path(url:str)->Path:
    return CACHE_DIR / (hashlib.sha1(url.encode()).hexdigest() + ".html")

async def read_cache(url):
    p = cache_path(url)
    if p.exists():
        async with aiofiles.open(p, "r", encoding="utf-8") as f:
            return await f.read()

async def write_cache(url, html):
    async with aiofiles.open(cache_path(url), "w", encoding="utf-8") as f:
        await f.write(html)

def bs_text(html:str)->str:
    try:
        soup = BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(html, "html.parser")
    return soup.get_text("\n", strip=True)

async def head_ok(url, head):
    try:
        r = await head.head(url)
        return r.status_code < 400 and r.headers.get("content-type", "").startswith("text/html")
    except httpx.HTTPError:
        return False

async def get_html(url, get):
    try:
        return (await get.get(url)).text
    except httpx.HTTPError:
        return None

async def gpt_extract(text:str, page:str, oai, sem)->List[Dict[str,str]]:
    if len(text) < 100: return []
    snippet = text[:8000]
    prompt = ("Extract an array 'contacts' (role,name,email,page_url).\n"
              "Keep only roles containing rector, chancellor, president, "
              "Head of Recruitment, admissions, recruitment. "
              "Email must belong to the institution domain. "
              f"Source: {page}\n---\n{snippet}")
    need = tokens_of(prompt) + 500
    await reserve(need)
    async with sem:
        resp = await oai.chat.completions.create(
            model=MODEL,
            messages=[{"role":"system","content":"You are a strict JSON emitter."},
                      {"role":"user","content":prompt}],
            response_format={"type":"json_object"})
    try:
        return json.loads(resp.choices[0].message.content)["contacts"]
    except Exception:
        print(f"[warn] JSON parse fail @ {page[:60]}…")
        return []

async def process_uni(uni, country, sem, probes, results, seen, stats, head, get, oai, gpt_sem):
    async with sem:
        home = uni["url"].rstrip("/")
        pages = []; checked = 0
        for slug in SLUGS:
            if checked >= probes: break
            url = home + slug; checked += 1
            if await head_ok(url, head): pages.append(url)
        if not pages: pages = [home]

        for page in pages:
            html = await read_cache(page) or await get_html(page, get)
            if not html: continue
            await write_cache(page, html) if not await read_cache(page) else None
            text = bs_text(html)
            contacts = await gpt_extract(text, page, oai, gpt_sem)

            stats["tok_in"]  += tokens_of(text[:8000])
            stats["tok_out"] += 500

            for c in contacts:
                email = (c.get("email") or "").lower()
                if not email: continue
                key = (uni["name"], email)
                if key in seen: continue
                results.append({
                    "University": uni["name"], "Country": country,
                    "Role": c.get("role", ""), "Name": c.get("name", ""),
                    "Email": email, "PageURL": c.get("page_url", page)})
                seen.add(key)

async def main(country, limit, outfile, probes, head, get, oai):
    import requests
    rsp = requests.get(f"{OPENALEX_API}?filter=country_code:{country}&per_page=200").json()
    unis = [{"name": r["display_name"], "url": r.get("homepage_url", "")}
            for r in rsp["results"] if r.get("homepage_url")][:limit]

    sem = asyncio.Semaphore(CONCURRENCY)
    gpt_sem = asyncio.Semaphore(GPT_CONCURRENCY)
    results = []; seen = set(); stats = {"tok_in": 0, "tok_out": 0}
    tasks = [process_uni(u, country, sem, probes, results, seen, stats, head, get, oai, gpt_sem) for u in unis]

    for _ in tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="uni", desc="universities"):
        try:
            await _
        except Exception as e:
            print("[warn] task error:", e)

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

    dollars = (stats["tok_in"]/1000)*COST_IN + (stats["tok_out"]/1000)*COST_OUT
    print(f"✅ {len(df)} rows  →  {outfile}\n≈{stats['tok_in']}/{stats['tok_out']} tokens  ≈  ${dollars:.2f}")

def run_generator(country: str, output_path: str = "downloads/contacts.csv", limit: int = 200, probes: int = PROBE_LIMIT):
    head = httpx.AsyncClient(headers=HEADERS, timeout=TIMEOUT, follow_redirects=True, http2=True)
    get  = httpx.AsyncClient(headers=HEADERS, timeout=TIMEOUT, follow_redirects=True, http2=True)
    oai  = AsyncOpenAI(api_key=OPENAI_API_KEY)
    try:
        asyncio.run(main(country.upper(), limit, output_path, probes, head, get, oai))
    finally:
        asyncio.run(asyncio.gather(head.aclose(), get.aclose(), oai.aclose()))
