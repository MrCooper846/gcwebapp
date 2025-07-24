#!/usr/bin/env python3
import argparse
import pandas as pd
import re
import sys
import time
import random
import secrets
import string
import socket
import sqlite3
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Iterable
from collections import defaultdict
import threading

from email_validator import validate_email, EmailNotValidError
import dns.resolver
from rapidfuzz.distance import Levenshtein
from tqdm import tqdm
import smtplib

# ---------------- Defaults / constants ----------------
COMMON_DOMAINS = [
    "gmail.com", "googlemail.com", "yahoo.com", "outlook.com", "hotmail.com",
    "icloud.com", "proton.me", "protonmail.com", "gmx.com", "aol.com", "live.com"
]

ROLE_LOCALPARTS = {
    "admin", "administrator", "billing", "contact", "csr", "customercare",
    "customerservice", "enquiries", "enquiry", "finance", "help", "helpdesk",
    "hr", "info", "it", "marketing", "news", "noreply", "no-reply", "office",
    "orders", "postmaster", "root", "sales", "security", "support", "team",
    "webmaster"
}

DEFAULT_DNS_TIMEOUT   = 5.0
DEFAULT_SMTP_TIMEOUT  = 10.0
DEFAULT_MAX_WORKERS   = 8   # safer default
DEFAULT_POLICY        = "balanced"  # strict | balanced | relaxed
DEFAULT_HELO          = socket.getfqdn() or "validator.example.com"

# Per-provider rate limits (tokens per period seconds) — conservative
DEFAULT_TOKENS = 10
DEFAULT_PERIOD = 60
MX_BUCKET_LIMITS = {
    "gmail":        (5, 60),
    "outlook":      (3, 60),
    "yahoodns":     (3, 60),
    "mimecast":     (1, 60),
    "secureserver": (1, 60),  # GoDaddy
    "proofpoint":   (2, 60),
}

DEFAULT_DB = ".email_validator_cache.sqlite"
# Long cache for definite answers, very short for soft/blocked
DEFAULT_TTL_VALID_DAYS   = 30
DEFAULT_TTL_SOFT_DAYS    = 1
DEFAULT_TTL_MX_DAYS      = 30

EMAIL_RE = re.compile(r'([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})')

# Classifications we use
SMTP_VALID_SET   = {"valid"}
SMTP_HARD_SET    = {"invalid"}
SMTP_SOFT_SET    = {"tempfail", "blocked", "error", "unknown", "not_tested"}

# ------------------------------------------------------


# ---------------- SQLite cache layer ------------------
class Cache:
    def __init__(self, path: str,
                 ttl_valid_days: int, ttl_soft_days: int, ttl_mx_days: int):
        self.ttl_valid_secs = ttl_valid_days * 86400 if ttl_valid_days > 0 else 0
        self.ttl_soft_secs  = ttl_soft_days  * 86400 if ttl_soft_days  > 0 else 0
        self.ttl_mx_secs    = ttl_mx_days    * 86400 if ttl_mx_days    > 0 else 0

        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.lock = threading.Lock()
        with self.lock:
            self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS email_cache (
                email TEXT PRIMARY KEY,
                normalized TEXT,
                bounce_risk INTEGER,
                reasons TEXT,
                mx_ok INTEGER,
                suggestion TEXT,
                smtp_status TEXT,
                smtp_code INTEGER,
                smtp_msg TEXT,
                catch_all TEXT,
                mailbox_full INTEGER,
                ts INTEGER
            );
            CREATE TABLE IF NOT EXISTS mx_cache (
                domain TEXT PRIMARY KEY,
                mx_ok INTEGER,
                mx_host TEXT,
                error TEXT,
                ts INTEGER
            );
            """)
            self.conn.commit()

    def _status_ttl(self, smtp_status: Optional[str]) -> int:
        """
        Returns the TTL (in seconds) that applies to a given smtp_status.
        """
        if smtp_status in SMTP_VALID_SET or smtp_status in SMTP_HARD_SET:
            return self.ttl_valid_secs
        return self.ttl_soft_secs

    # ---------- email ----------
    def get_email(self, email: str, force: bool=False) -> Optional[Dict[str, object]]:
        with self.lock:
            cur = self.conn.execute("SELECT * FROM email_cache WHERE email=?", (email,))
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
        if not row:
            return None
        data = dict(zip(cols, row))
        if force:
            return None

        ttl = self._status_ttl(data.get("smtp_status"))
        if ttl > 0 and (time.time() - data["ts"]) > ttl:
            return None

        data["bounce_risk"]  = bool(data["bounce_risk"])
        data["mx_ok"]        = bool(data["mx_ok"])
        data["mailbox_full"] = bool(data["mailbox_full"])
        return data

    def put_email(self, res: Dict[str, object]):
        with self.lock:
            self.conn.execute("""
                INSERT INTO email_cache(email, normalized, bounce_risk, reasons, mx_ok,
                                        suggestion, smtp_status, smtp_code, smtp_msg,
                                        catch_all, mailbox_full, ts)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(email) DO UPDATE SET
                    normalized=excluded.normalized,
                    bounce_risk=excluded.bounce_risk,
                    reasons=excluded.reasons,
                    mx_ok=excluded.mx_ok,
                    suggestion=excluded.suggestion,
                    smtp_status=excluded.smtp_status,
                    smtp_code=excluded.smtp_code,
                    smtp_msg=excluded.smtp_msg,
                    catch_all=excluded.catch_all,
                    mailbox_full=excluded.mailbox_full,
                    ts=excluded.ts
            """, (
                res["email"], res.get("normalized"),
                int(bool(res.get("bounce_risk"))),
                res.get("reasons"),
                int(bool(res.get("mx_ok"))),
                res.get("suggestion"),
                res.get("smtp_status"),
                res.get("smtp_code"),
                res.get("smtp_msg"),
                res.get("catch_all"),
                int(bool(res.get("mailbox_full"))),
                int(time.time())
            ))
            self.conn.commit()

    # ---------- MX ----------
    def get_mx(self, domain: str, force: bool=False) -> Optional[Tuple[bool, Optional[str], Optional[str]]]:
        with self.lock:
            cur = self.conn.execute(
                "SELECT mx_ok, mx_host, error, ts FROM mx_cache WHERE domain=?",
                (domain,))
            row = cur.fetchone()
        if not row:
            return None
        mx_ok, mx_host, err, ts = row
        if force:
            return None
        if self.ttl_mx_secs > 0 and (time.time() - ts) > self.ttl_mx_secs:
            return None
        return (bool(mx_ok), err, mx_host)

    def put_mx(self, domain: str, mx_ok: bool, mx_host: Optional[str], err: Optional[str]):
        with self.lock:
            self.conn.execute("""
                INSERT INTO mx_cache(domain, mx_ok, mx_host, error, ts)
                VALUES(?,?,?,?,?)
                ON CONFLICT(domain) DO UPDATE SET
                  mx_ok=excluded.mx_ok,
                  mx_host=excluded.mx_host,
                  error=excluded.error,
                  ts=excluded.ts
            """, (domain, int(mx_ok), mx_host, err, int(time.time())))
            self.conn.commit()


# ---------------- Small helpers -----------------------
def extract_first_email(text: str) -> Optional[str]:
    if not isinstance(text, str):
        return None
    # Clean obvious wrappers/noise
    text = text.replace("mailto:", " ")
    # remove angle brackets etc
    candidates = EMAIL_RE.findall(text)
    if not candidates:
        return None
    return candidates[0].strip().strip('>"\' ')

def guess_email_column(df: pd.DataFrame) -> Optional[str]:
    candidates = [c for c in df.columns if re.search(r'(^|\b)email(s)?(\b|$)', c.strip().lower())]
    return candidates[0] if candidates else None

def load_disposable_set(path: Optional[str]) -> set:
    if not path:
        return set()
    p = Path(path)
    if not p.exists():
        print(f"[WARN] disposable list file not found: {path}", file=sys.stderr)
        return set()
    with p.open() as f:
        return {line.strip().lower() for line in f if line.strip() and not line.startswith("#")}

_dns_cache: Dict[Tuple[str, str], Tuple[bool, Optional[str], Optional[str]]] = {}
_dns_lock = threading.Lock()

def domain_has_mx(domain: str, cache: Cache, dns_timeout: float, force_refresh: bool) -> Tuple[bool, Optional[str], Optional[str]]:
    key = ("mx", domain)
    with _dns_lock:
        if not force_refresh and key in _dns_cache:
            return _dns_cache[key]

    row = cache.get_mx(domain, force=force_refresh)
    if row is not None:
        with _dns_lock:
            _dns_cache[key] = row
        return row

    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=dns_timeout)
        if answers:
            best = sorted(answers, key=lambda r: r.preference)[0].exchange.to_text(omit_final_dot=True)
            res = (True, None, best)
        else:
            res = (False, "no MX records", None)
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers,
            dns.resolver.Timeout, dns.exception.DNSException) as e:
        res = (False, str(e), None)

    cache.put_mx(domain, res[0], res[2], res[1])
    with _dns_lock:
        _dns_cache[key] = res
    return res

def detect_typo(domain: str) -> Optional[str]:
    best, dist = min(((d, Levenshtein.distance(domain, d)) for d in COMMON_DOMAINS),
                     key=lambda t: t[1])
    return best if dist == 1 else None


# ---------------- Rate limiter ------------------------
class TokenBucket:
    def __init__(self, tokens: int, period: float):
        self.capacity = tokens
        self.tokens = tokens
        self.period = period
        self.updated = time.monotonic()
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.updated
            refill = elapsed * (self.capacity / self.period)
            self.tokens = min(self.capacity, self.tokens + refill)
            self.updated = now
            if self.tokens >= 1:
                self.tokens -= 1
                return 0.0
            need = 1 - self.tokens
            wait = need * (self.period / self.capacity)
            self.tokens = 0.0
            return wait

_rate_buckets: Dict[str, TokenBucket] = {}
_rate_lock = threading.Lock()

def bucket_name_for_mx(mx_host: str) -> str:
    h = mx_host.lower()
    if "google.com" in h or ".l.google.com" in h or "gsmtp" in h:
        return "gmail"
    if "protection.outlook.com" in h or "outlook.com" in h or "microsoft.com" in h or "eurprd" in h:
        return "outlook"
    if "yahoodns.net" in h or "yahoo.com" in h:
        return "yahoodns"
    if "mimecast" in h or ".uk" in h and "mimecast" in h:
        return "mimecast"
    if "secureserver.net" in h or "godaddy" in h:
        return "secureserver"
    if "pphosted.com" in h or "proofpoint" in h:
        return "proofpoint"
    return mx_host

def get_bucket(mx_host: str) -> TokenBucket:
    name = bucket_name_for_mx(mx_host)
    with _rate_lock:
        if name not in _rate_buckets:
            tokens, period = MX_BUCKET_LIMITS.get(name, (DEFAULT_TOKENS, DEFAULT_PERIOD))
            _rate_buckets[name] = TokenBucket(tokens, period)
        return _rate_buckets[name]


# ---------------- SMTP helpers ------------------------
def classify_smtp(code: Optional[int], msg: Optional[str]) -> str:
    """
    Map raw SMTP response to our status taxonomy.
    """
    if code is None:
        return "error"
    m = (msg or "").lower()

    # explicit "blocked" clues
    blocked_keywords = [
        "access denied", "not allowed", "antispam policy", "reverse dns",
        "abusix", "temporarily rejected", "too many connections",
        "helo command rejected", "rdns", "blacklist", "blocklist"
    ]
    if any(k in m for k in blocked_keywords):
        return "blocked"

    if code == 250:
        return "valid"
    if code == 552:  # over quota
        return "invalid"  # deliverability-wise, treat as hard bounce risk
    if 500 <= code < 600:
        return "invalid"
    if 400 <= code < 500:
        return "tempfail"
    return "unknown"

def smtp_open(mx_host: str, helo: str, timeout: float) -> Optional[smtplib.SMTP]:
    try:
        s = smtplib.SMTP(mx_host, 25, timeout=timeout)
        # EHLO/HELO
        try:
            s.ehlo(helo)
        except smtplib.SMTPHeloError:
            s.helo(helo)
        return s
    except Exception:
        return None

def batch_smtp_probe(mx_host: str,
                     sender: str,
                     targets: List[str],
                     helo: str,
                     timeout: float) -> Dict[str, Dict[str, object]]:
    """
    Reuse one connection per mx_host, RCPT all targets, catch-all once per domain.
    """
    results: Dict[str, Dict[str, object]] = {}
    gate = get_bucket(mx_host)

    # Open
    wait = gate.wait()
    if wait > 0:
        time.sleep(wait)
    s = smtp_open(mx_host, helo, timeout)
    if s is None:
        # whole bucket error
        for t in targets:
            results[t] = {
                "smtp_status": "error",
                "smtp_code": None,
                "smtp_msg": "connect_failed",
                "catch_all": "unknown",
                "mailbox_full": False,
            }
        return results

    # MAIL FROM once
    try:
        wait = gate.wait()
        if wait > 0:
            time.sleep(wait)
        s.mail(sender if sender else "")
    except Exception as e:
        s.close()
        for t in targets:
            results[t] = {
                "smtp_status": "error",
                "smtp_code": None,
                "smtp_msg": f"MAIL FROM failed: {e}",
                "catch_all": "unknown",
                "mailbox_full": False,
            }
        return results

    # Do RCPTs + catch-all per domain
    catchall_cache: Dict[str, str] = {}
    for addr in targets:
        domain = addr.split("@", 1)[1].lower()

        # RCPT for real address
        try:
            wait = gate.wait()
            if wait > 0:
                time.sleep(wait)
            code, msg = s.rcpt(addr)
            msg = msg.decode() if isinstance(msg, bytes) else (msg or "")
        except Exception as e:
            code, msg = None, str(e)

        status = classify_smtp(code, msg)
        mailbox_full = (code == 552)

        # catch-all test (only if the primary RCPT was 250 and we haven't tested this domain)
        catch_all = "unknown"
        if status == "valid":
            if domain not in catchall_cache:
                bogus_local = secrets.token_hex(8)
                bogus = f"{bogus_local}@{domain}"
                try:
                    wait = gate.wait()
                    if wait > 0:
                        time.sleep(wait)
                    code2, _ = s.rcpt(bogus)
                    catch_all = "yes" if code2 == 250 else "no"
                except Exception:
                    catch_all = "unknown"
                catchall_cache[domain] = catch_all
            else:
                catch_all = catchall_cache[domain]

        results[addr] = {
            "smtp_status": status,
            "smtp_code": code,
            "smtp_msg": msg,
            "catch_all": catch_all,
            "mailbox_full": mailbox_full,
        }

    try:
        s.quit()
    except Exception:
        pass
    return results


# ---------------- Core logic --------------------------
def compute_bounce_risk(policy: str,
                        reasons: List[str],
                        smtp_status: str,
                        catch_all: str,
                        mailbox_full: bool) -> bool:
    if policy not in {"strict", "balanced", "relaxed"}:
        policy = "balanced"

    hard_flags = {"invalid_syntax", "no_mx", "disposable_domain", "likely_typo_domain"}
    if mailbox_full:
        # treat as risky always; mail will likely bounce
        return True

    if policy == "strict":
        if any(r in reasons for r in hard_flags):
            return True
        if smtp_status in SMTP_HARD_SET | SMTP_SOFT_SET:
            return True
        if catch_all == "yes":
            return True
        return False

    if policy == "balanced":
        if any(r in reasons for r in {"invalid_syntax", "no_mx"}):
            return True
        if smtp_status in SMTP_HARD_SET:
            return True
        # tempfail / blocked / catch-all => unknown, not auto-bad
        return False

    # relaxed
    if any(r in reasons for r in {"invalid_syntax", "no_mx"}):
        return True
    if smtp_status in SMTP_HARD_SET:
        return True
    return False


def evaluate_offline(email: str,
                     disposable_domains: set,
                     cache: Cache,
                     dns_timeout: float,
                     force_refresh: bool) -> Dict[str, object]:
    """
    Do everything except SMTP. No cache write; we combine with SMTP result and then cache.
    """
    reasons: List[str] = []
    suggestion: Optional[str] = None
    normalized = None

    try:
        v = validate_email(email, allow_smtputf8=True)
        normalized = v.email
    except EmailNotValidError:
        return {
            "email": email,
            "normalized": None,
            "reasons": ["invalid_syntax"],
            "mx_ok": False,
            "mx_host": None,
            "suggestion": None,
        }

    local, domain = normalized.rsplit("@", 1)

    # role
    if local.lower() in ROLE_LOCALPARTS:
        reasons.append("role_address")

    # disposable
    if domain.lower() in disposable_domains:
        reasons.append("disposable_domain")

    # MX
    mx_ok, mx_err, mx_host = domain_has_mx(domain, cache, dns_timeout, force_refresh)
    if not mx_ok:
        reasons.append("no_mx")

    # typo suggestion
    typo_suggestion = detect_typo(domain.lower())
    if typo_suggestion:
        reasons.append("likely_typo_domain")
        suggestion = f"{local}@{typo_suggestion}"

    return {
        "email": email,
        "normalized": normalized,
        "reasons": reasons,
        "mx_ok": mx_ok,
        "mx_host": mx_host,
        "suggestion": suggestion,
    }


def process_dataframe(df: pd.DataFrame,
                      email_col: str,
                      disposable_domains: set,
                      do_smtp: bool,
                      mail_from: str,
                      helo: str,
                      workers: int,
                      cache: Cache,
                      dns_timeout: float,
                      smtp_timeout: float,
                      force_refresh: bool,
                      policy: str) -> pd.DataFrame:

    raw_series = df[email_col].astype(str)

    # Pre-clean + quick cache lookups
    offline_results: Dict[int, Dict[str, object]] = {}
    final_results: Dict[int, Dict[str, object]] = {}
    needs_smtp: Dict[str, List[int]] = defaultdict(list)  # mx_host -> [row_index]
    normalized_per_idx: Dict[int, str] = {}

    # 1) Pre-clean + cache
    for idx, raw in raw_series.items():
        cleaned = extract_first_email(raw)
        if not cleaned:
            final_results[idx] = {
                "email": raw,
                "normalized": None,
                "bounce_risk": True,
                "reasons": "invalid_syntax",
                "mx_ok": False,
                "suggestion": None,
                "smtp_status": "not_tested",
                "smtp_code": None,
                "smtp_msg": None,
                "catch_all": "unknown",
                "mailbox_full": False,
            }
            continue

        cached = cache.get_email(cleaned, force=force_refresh)
        if cached:
            final_results[idx] = cached
            continue

        # we will evaluate offline now
        offline_results[idx] = {"cleaned": cleaned}

    # 2) Offline checks (syntax, MX, etc.)
    #    (single-threaded for simplicity; could be parallelised if big)
    for idx, meta in tqdm(offline_results.items(), desc="Offline checks"):
        cleaned = meta["cleaned"]
        off = evaluate_offline(cleaned, disposable_domains, cache, dns_timeout, force_refresh)
        offline_results[idx].update(off)

        if off["normalized"]:
            normalized_per_idx[idx] = off["normalized"]

        # If no MX or invalid syntax, we can finalize now (no SMTP)
        if (not do_smtp) or (not off["mx_ok"]) or ("invalid_syntax" in off["reasons"]):
            # Will compute bounce_risk after we know smtp_status (not_tested)
            pass
        else:
            # group by mx_host for batching
            needs_smtp[off["mx_host"]].append(idx)

    # 3) SMTP batched per MX host
    smtp_results: Dict[int, Dict[str, object]] = {}

    if do_smtp and needs_smtp:
        for mx_host, idcs in tqdm(needs_smtp.items(), desc="SMTP batches"):
            targets = [offline_results[i]["normalized"] for i in idcs if offline_results[i]["normalized"]]
            batch_res = batch_smtp_probe(mx_host, mail_from, targets, helo, smtp_timeout)
            # map back to idx
            by_email = {offline_results[i]["normalized"]: i for i in idcs if offline_results[i]["normalized"]}
            for eml, info in batch_res.items():
                i = by_email[eml]
                smtp_results[i] = info

    # 4) Merge + compute bounce_risk, write cache
    for idx in range(len(df)):
        if idx in final_results:  # already decided from cache or invalid_syntax
            # ensure bounce_risk policy is applied (cache may have older policy)
            r = final_results[idx]
            reasons     = r.get("reasons", "")
            reasons_lst = reasons.split(",") if reasons else []
            smtp_status = r.get("smtp_status", "not_tested")
            catch_all   = r.get("catch_all", "unknown")
            mailbox_full = r.get("mailbox_full", False)
            r["bounce_risk"] = compute_bounce_risk(policy, reasons_lst, smtp_status, catch_all, mailbox_full)
            final_results[idx] = r
            continue

        off = offline_results[idx]
        reasons_lst = off["reasons"]
        normalized  = off["normalized"]
        mx_ok       = off["mx_ok"]
        suggestion  = off["suggestion"]

        smtp_info = {
            "smtp_status": "not_tested",
            "smtp_code": None,
            "smtp_msg": None,
            "catch_all": "unknown",
            "mailbox_full": False,
        }
        if idx in smtp_results:
            smtp_info = smtp_results[idx]

        bounce_risk = compute_bounce_risk(policy, reasons_lst,
                                          smtp_info["smtp_status"], smtp_info["catch_all"],
                                          smtp_info["mailbox_full"])

        res = {
            "email": off["email"],
            "normalized": normalized,
            "bounce_risk": bounce_risk,
            "reasons": ",".join(reasons_lst) if reasons_lst else "",
            "mx_ok": bool(mx_ok),
            "suggestion": suggestion,
            **smtp_info
        }
        final_results[idx] = res
        cache.put_email(res)

    verdict_df = pd.DataFrame.from_dict(final_results, orient="index")
    out = pd.concat([df.reset_index(drop=True), verdict_df.reset_index(drop=True)], axis=1)
    return out


# ---------------- main -------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="CSV email validator: offline checks + batched, rate-limited SMTP RCPT + SQLite cache + catch-all aware."
    )
    parser.add_argument("csv", help="Path to input CSV")
    parser.add_argument("-c", "--email-col", help="Email column name (auto-detect if omitted)")
    parser.add_argument("--disposable", help="Path to disposable domain list (one domain per line)")
    parser.add_argument("-o", "--output", help="Output CSV path (default: <input>.validated.csv)")
    parser.add_argument("--sep", default=",", help="CSV delimiter (default: ,)")
    parser.add_argument("--no-smtp", action="store_true", help="Disable SMTP RCPT probing")
    parser.add_argument("--from", dest="mail_from", default="[email protected]",
                        help="Envelope MAIL FROM used for SMTP probe ('' = null sender)")
    parser.add_argument("--helo", default=DEFAULT_HELO, help=f"HELO/EHLO hostname (default: {DEFAULT_HELO})")
    parser.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS, help="(unused placeholder now)")
    parser.add_argument("--dns-timeout", type=float, default=DEFAULT_DNS_TIMEOUT)
    parser.add_argument("--smtp-timeout", type=float, default=DEFAULT_SMTP_TIMEOUT)
    parser.add_argument("--db", default=DEFAULT_DB, help=f"SQLite cache path (default: {DEFAULT_DB})")
    parser.add_argument("--ttl-valid-days", type=int, default=DEFAULT_TTL_VALID_DAYS,
                        help=f"TTL for valid/invalid results (default: {DEFAULT_TTL_VALID_DAYS})")
    parser.add_argument("--ttl-soft-days", type=int, default=DEFAULT_TTL_SOFT_DAYS,
                        help=f"TTL for tempfail/blocked/error/unknown results (default: {DEFAULT_TTL_SOFT_DAYS})")
    parser.add_argument("--ttl-mx-days", type=int, default=DEFAULT_TTL_MX_DAYS,
                        help=f"TTL for MX lookups (default: {DEFAULT_TTL_MX_DAYS})")
    parser.add_argument("--force", action="store_true", help="Ignore cache and refresh everything")
    parser.add_argument("--policy", choices=["strict", "balanced", "relaxed"], default=DEFAULT_POLICY,
                        help=f"bounce_risk policy (default: {DEFAULT_POLICY})")
    args = parser.parse_args()

    in_path = Path(args.csv)
    if not in_path.exists():
        print(f"Input file not found: {in_path}", file=sys.stderr)
        sys.exit(1)

    cache = Cache(args.db, args.ttl_valid_days, args.ttl_soft_days, args.ttl_mx_days)
    disposable_domains = load_disposable_set(args.disposable)

    try:
        df = pd.read_csv(in_path, sep=args.sep)
    except Exception as e:
        print(f"Failed to read CSV: {e}", file=sys.stderr)
        sys.exit(1)

    email_col = args.email_col or guess_email_column(df)
    if not email_col or email_col not in df.columns:
        print("Could not find the email column. Use -c/--email-col to specify it.", file=sys.stderr)
        print("Columns available:", list(df.columns), file=sys.stderr)
        sys.exit(1)

    do_smtp = not args.no_smtp
    print(f"[INFO] SMTP checks: {'ENABLED' if do_smtp else 'DISABLED'}")
    print(f"[INFO] Policy: {args.policy}")
    print(f"[INFO] HELO: {args.helo}")
    print(f"[INFO] Cache: {args.db} (valid={args.ttl_valid_days}d, soft={args.ttl_soft_days}d, mx={args.ttl_mx_days}d)  Force={args.force}")

    # Configure resolver timeouts
    dns.resolver.default_resolver = dns.resolver.Resolver(configure=True)
    dns.resolver.default_resolver.lifetime = args.dns_timeout
    dns.resolver.default_resolver.timeout  = args.dns_timeout

    out_df = process_dataframe(
        df=df,
        email_col=email_col,
        disposable_domains=disposable_domains,
        do_smtp=do_smtp,
        mail_from=args.mail_from,
        helo=args.helo,
        workers=args.workers,
        cache=cache,
        dns_timeout=args.dns_timeout,
        smtp_timeout=args.smtp_timeout,
        force_refresh=args.force,
        policy=args.policy,
    )

    out_path = Path(args.output) if args.output else in_path.with_suffix(".validated.csv")
    out_df.to_csv(out_path, index=False)
    print(f"Done. Wrote: {out_path}")
    if "bounce_risk" in out_df.columns:
        print(out_df["bounce_risk"].value_counts())

if __name__ == "__main__":
    main()
