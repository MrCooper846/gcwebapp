from flask import Flask, render_template, request, send_from_directory, redirect, url_for, Response, abort
import os, sys, json, uuid, threading, queue
from datetime import datetime
from pathlib import Path

# Try to import the crawler module (best path: gives live progress)
try:
    from gc_contacts_v2_3_fix import run_all as crawler_run_all  # async coroutine
except Exception:
    crawler_run_all = None

app = Flask(__name__)
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "downloads"
DEBUG_ROOT = OUTPUT_DIR / "debug"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_ROOT.mkdir(parents=True, exist_ok=True)

# in-memory job registry
JOBS = {}  # job_id -> {"q": Queue, "done": bool, "outfile": str|None}

def safe_country(code: str) -> str:
    code = (code or "").strip().upper()
    return "".join(c for c in code if c.isalpha())[:3]

def _sse(msg: dict) -> str:
    return f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"

def start_job(country: str, limit: int | None, emit_all: bool, debug: bool,
              ignore_robots: bool, browser_ua: bool, verbose: bool) -> str:
    job_id = uuid.uuid4().hex[:12]
    q = queue.Queue()
    JOBS[job_id] = {"q": q, "done": False, "outfile": None}

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    outfile = OUTPUT_DIR / f"{country}_contacts_{ts}.csv"
    debug_dir = DEBUG_ROOT / f"{country}_{ts}"
    debug_dir.mkdir(parents=True, exist_ok=True)

    def progress_cb(ev: dict):
        # ev is {"event":"start"/"tick"/"finish", ...}
        q.put(ev)

    def worker_module():
        import asyncio
        try:
            asyncio.run(
                crawler_run_all(
                    country.upper(),
                    limit,
                    str(outfile),
                    emit_all,
                    debug,
                    str(debug_dir),
                    ignore_robots,
                    verbose,
                    browser_ua,
                    on_progress=progress_cb,   # <-- live progress
                )
            )
        except Exception as e:
            q.put({"event": "error", "message": str(e)})
        finally:
            JOBS[job_id]["outfile"] = str(outfile) if outfile.exists() else None
            JOBS[job_id]["done"] = True
            q.put({"event": "done"})

    def worker_subprocess():
        # Fallback if we couldn't import the module (no granular progress)
        import subprocess
        script = BASE_DIR / "gc_contacts_v2_3_fix.py"
        try:
            q.put({"event": "start", "total": None})
            cmd = [sys.executable, str(script), country.upper(), "--outfile", str(outfile)]
            if limit: cmd += ["--limit", str(limit)]
            if emit_all: cmd.append("--emit-all")
            if debug: cmd += ["--debug", "--debug-dir", str(debug_dir)]
            if ignore_robots: cmd.append("--ignore-robots")
            if browser_ua: cmd.append("--browser-ua")
            if verbose: cmd.append("--verbose")
            subprocess.run(cmd, check=True)
            q.put({"event": "finish", "outfile": str(outfile)})
        except Exception as e:
            q.put({"event": "error", "message": str(e)})
        finally:
            JOBS[job_id]["outfile"] = str(outfile) if outfile.exists() else None
            JOBS[job_id]["done"] = True
            q.put({"event": "done"})

    t = threading.Thread(target=worker_module if crawler_run_all else worker_subprocess, daemon=True)
    t.start()
    return job_id

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        country = safe_country(request.form.get('country', ''))
        if not country:
            abort(400, description="Country code is required.")
        limit_raw = request.form.get('limit', '').strip()
        limit = int(limit_raw) if limit_raw.isdigit() else None

        # defaults tuned for testing; adjust as you like
        emit_all = True
        debug = True
        ignore_robots = True
        browser_ua = True
        verbose = False

        job_id = start_job(country, limit, emit_all, debug, ignore_robots, browser_ua, verbose)
        return redirect(url_for('progress_page', job_id=job_id))
    return render_template('index.html')

@app.route('/progress/<job_id>')
def progress_page(job_id):
    if job_id not in JOBS:
        abort(404)
    return render_template('progress.html', job_id=job_id)

@app.route('/events/<job_id>')
def sse(job_id):
    if job_id not in JOBS:
        return Response(_sse({"event": "error", "message": "unknown job"}), mimetype='text/event-stream')
    q = JOBS[job_id]["q"]

    def stream():
        # send a hello so the client shows something
        yield _sse({"event": "hello"})
        while True:
            try:
                ev = q.get(timeout=0.5)
                yield _sse(ev)
                if ev.get("event") in ("done", "finish"):
                    break
            except queue.Empty:
                # keep the connection alive
                yield ": keep-alive\n\n"

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",  # for nginx
    }
    return Response(stream(), headers=headers)

@app.route('/download/<job_id>')
def download_page(job_id):
    job = JOBS.get(job_id)
    if not job:
        abort(404)
    outfile = job.get("outfile")
    return render_template('download.html', filename=os.path.basename(outfile) if outfile else None)

@app.route('/files/<path:filepath>')
def download_any(filepath):
    return send_from_directory(OUTPUT_DIR, filepath, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True)
