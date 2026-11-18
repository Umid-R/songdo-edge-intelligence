"""Record the console demo video (no audio) from a running node.

    bash scripts/start_demo.sh OS2 --fresh          # in another terminal
    pip install playwright && python -m playwright install chromium
    python scripts/demo/record_demo.py --out /tmp/demo
    python scripts/demo/assemble_demo.py --out /tmp/demo   # needs ffmpeg

The browser is driven through: Run all 25 -> NODE-OUTAGE (WAN cut 60 s) -> Live node
WAN cut / restore / reconcile. A small injected overlay draws a cursor, click ripples,
captions and a speed badge. Long waits are logged to segments.json so the assembler
can speed them up (default 2x for the 25-scenario batch).
"""
import argparse, asyncio, json, os, time, urllib.request
from playwright.async_api import async_playwright

B = "http://127.0.0.1:8002"
OUT = "/tmp/demo"
CHROME = None
BATCH_SPEED = 2
HERE = os.path.dirname(os.path.abspath(__file__))
W, H = 1600, 900
SEG = []          # (start_s, end_s, speed) in video time base (seconds since recording start)
T0 = None

OVERLAY = r"""
(() => {
 const css = `
 #dm-cur{position:fixed;z-index:99999;left:0;top:0;width:26px;height:26px;pointer-events:none;transition:transform .65s cubic-bezier(.3,.7,.2,1);}
 #dm-cur svg{filter:drop-shadow(0 2px 3px rgba(0,0,0,.35))}
 .dm-rip{position:fixed;z-index:99998;width:16px;height:16px;margin:-8px 0 0 -8px;border-radius:50%;border:3px solid #2a78d6;pointer-events:none;animation:dmr .7s ease-out forwards}
 @keyframes dmr{from{transform:scale(.3);opacity:1}to{transform:scale(4.2);opacity:0}}
 #dm-cap{position:fixed;z-index:99997;left:50%;bottom:34px;transform:translate(-50%,40px);opacity:0;max-width:1180px;
   background:rgba(15,25,42,.92);color:#fff;font:600 25px/1.35 "DejaVu Sans",sans-serif;padding:16px 30px;border-radius:14px;
   box-shadow:0 10px 30px rgba(0,0,0,.25);transition:all .45s cubic-bezier(.3,.7,.2,1);text-align:center;border-left:6px solid #1baf7a}
 #dm-cap.on{transform:translate(-50%,0);opacity:1}
 #dm-cap small{display:block;font-weight:400;font-size:18px;color:#b9cbe6;margin-top:3px}
 #dm-fast{position:fixed;z-index:99997;right:26px;top:84px;background:#eb6834;color:#fff;font:700 22px "DejaVu Sans";padding:8px 16px;border-radius:10px;opacity:0;transition:opacity .3s}
 #dm-fast.on{opacity:1;animation:dmp 1s infinite}
 @keyframes dmp{50%{opacity:.6}}
 .dm-hl{outline:4px solid #1baf7a !important;outline-offset:3px;border-radius:6px;transition:outline-color .3s}
 `;
 const boot = () => {
  if (document.getElementById('dm-cur')) return;
  const st = document.createElement('style'); st.textContent = css; document.head.appendChild(st);
  const c = document.createElement('div'); c.id = 'dm-cur';
  c.innerHTML = '<svg width="26" height="26" viewBox="0 0 24 24"><path d="M3 2l7 19 2.6-7.9L21 10z" fill="#fff" stroke="#111" stroke-width="1.6" stroke-linejoin="round"/></svg>';
  c.style.transform = `translate(${innerWidth*0.55}px,${innerHeight*0.6}px)`;
  document.body.appendChild(c);
  const cap = document.createElement('div'); cap.id = 'dm-cap'; document.body.appendChild(cap);
  const f = document.createElement('div'); f.id = 'dm-fast'; document.body.appendChild(f);
 };
 window.__dm = {
  boot,
  move(x, y){ boot(); document.getElementById('dm-cur').style.transform = `translate(${x}px,${y}px)`; },
  rip(x, y){ const r = document.createElement('div'); r.className = 'dm-rip'; r.style.left = x+'px'; r.style.top = y+'px';
             document.body.appendChild(r); setTimeout(() => r.remove(), 800); },
  cap(t, s){ boot(); const c = document.getElementById('dm-cap'); if (!t){ c.classList.remove('on'); return; }
             c.classList.remove('on'); setTimeout(() => { c.innerHTML = t + (s ? `<small>${s}</small>` : ''); c.classList.add('on'); }, 250); },
  fast(on, label){ boot(); const f = document.getElementById('dm-fast'); f.textContent = label || '▶▶ 4×'; f.classList.toggle('on', !!on); },
  hl(sel, on){ document.querySelectorAll(sel).forEach(e => e.classList.toggle('dm-hl', on)); },
 };
 if (document.readyState !== 'loading') boot(); else document.addEventListener('DOMContentLoaded', boot);
})();
"""

def now(): return time.monotonic() - T0

def api(path, body=None):
    req = urllib.request.Request(B + path, data=None if body is None else json.dumps(body).encode(),
                                 method="GET" if body is None else "POST", headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=10).read())

async def click(pg, sel, pause=0.5):
    el = pg.locator(sel).first
    await el.scroll_into_view_if_needed()
    bb = await el.bounding_box()
    x, y = bb["x"] + bb["width"] / 2, bb["y"] + bb["height"] / 2
    await pg.evaluate(f"__dm.move({x - 4},{y - 3})")
    await pg.wait_for_timeout(750)
    await pg.evaluate(f"__dm.rip({x},{y})")
    await el.click()
    await pg.wait_for_timeout(int(pause * 1000))

async def cap(pg, t, s=None, hold=0):
    await pg.evaluate("([t,s]) => __dm.cap(t,s)", [t, s])
    if hold: await pg.wait_for_timeout(int(hold * 1000))

async def card(pg, name, secs):
    await pg.goto(f"file://{HERE}/cards.html?c={name}")
    await pg.wait_for_timeout(int(secs * 1000))

async def fast_wait(pg, cond, speed=4, label=None, captions=()):
    """Wait until cond() is true; the span is sped up in post. captions: [(after_s, text, sub)]"""
    await pg.evaluate("([l]) => __dm.fast(true, l)", [label or f"▶▶ {speed}×"])
    s = now(); shown = set()
    while not cond():
        el = now() - s
        for i, (at, t, sub) in enumerate(captions):
            if el >= at and i not in shown:
                shown.add(i); await cap(pg, t, sub)
        await asyncio.sleep(0.5)
    await pg.evaluate("__dm.fast(false)")
    SEG.append((s, now(), speed))

async def main():
    global T0
    async with async_playwright() as p:
        b = await p.chromium.launch(executable_path=CHROME, args=["--no-sandbox"])
        ctx = await b.new_context(viewport={"width": W, "height": H}, record_video_dir=os.path.join(OUT, "raw"),
                                  record_video_size={"width": W, "height": H})
        await ctx.add_init_script(OVERLAY)
        T0 = time.monotonic()
        pg = await ctx.new_page()

        await card(pg, "intro", 7)
        await card(pg, "s1", 4.5)

        # ---- 1. console, run all 25 ----
        await pg.goto(B); await pg.wait_for_timeout(1800)
        await cap(pg, "One offline console — every handbook task in one place", f"Served by the node itself · {B}", 4)
        await pg.evaluate("window.scrollTo({top: 700, behavior: 'smooth'})"); await pg.wait_for_timeout(1800)
        await pg.evaluate("window.scrollTo({top: 0, behavior: 'smooth'})"); await pg.wait_for_timeout(1400)
        await cap(pg, "Pick a task and a profile — or run all 25 at once", "Standard check · WAN cut 60 s · WAN cut 600 s", 3)
        await click(pg, "#runAll", 1.0)
        await cap(pg, "Every run: its own folder, fresh database and process group", "Cancel kills the scenario and all its children", 3)
        def batch_done():
            r = api("/api/runs"); return bool(r["batches"]) and r["batches"][0]["state"] != "running"
        await fast_wait(pg, batch_done, BATCH_SPEED, f"▶▶ {BATCH_SPEED}×", [
            (2, "Real Linux processes, real SQLite, real signals", "OS1 → OS5 run one after another"),
            (30, "Race conditions, deadlines, crashes, deadlocks…", "each scenario prints [PASS] only if every check holds"),
            (60, "OS4 and OS5: backups, health states, worker kills", "the badge turns green when the run passes")])
        await pg.wait_for_timeout(2500)
        await cap(pg, "25 / 25 passed", "same verdicts as the command-line runner (25 / 25 identical)")
        await pg.evaluate("window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'})"); await pg.wait_for_timeout(3500)
        await pg.evaluate("window.scrollTo({top: 0, behavior: 'smooth'})"); await pg.wait_for_timeout(2200)
        await click(pg, "tr:has-text('OS2-3') >> .badge", 1.5)
        await cap(pg, "Open any run: verdict, checks, telemetry, live log", "OS2-3 · e-stop below 0.8 m · commands expire after 500 ms", 4)
        await pg.hover("#dLog"); await pg.evaluate("document.getElementById('dLog').scrollTo({top: 0, behavior: 'smooth'})")
        await pg.wait_for_timeout(1500)
        await pg.evaluate("document.getElementById('dLog').scrollTo({top: 99999, behavior: 'smooth'})")
        await pg.wait_for_timeout(1500)
        btn = pg.locator("#dExport"); bb = await btn.bounding_box()
        await pg.evaluate(f"__dm.move({bb['x']+bb['width']/2},{bb['y']+bb['height']/2})"); await pg.wait_for_timeout(800)
        await pg.evaluate(f"__dm.rip({bb['x']+bb['width']/2},{bb['y']+bb['height']/2})")
        await cap(pg, "Export evidence JSON — argv, exit code, checks, source hash, platform", None, 3.5)
        await cap(pg, None)

        # ---- 2. NODE-OUTAGE ----
        await card(pg, "s2", 4.5)
        await pg.goto(B); await pg.wait_for_timeout(1500)
        await click(pg, "#themes >> text=NODE", 0.8)
        await cap(pg, "NODE-OUTAGE: the whole supervised node + its receiver process", "fresh node · WAN cut 60 s · Processor SIGKILL halfway", 3)
        await click(pg, "tr:has-text('NODE-OUTAGE') >> text=Run", 1.5)
        def node_done():
            r = api("/api/runs"); return r["runs"] and r["runs"][0]["state"] not in ("queued", "running")
        await fast_wait(pg, node_done, 4, "▶▶ 4×", [
            (1, "Node starts: SensorGen, Processor, SyncWorker, receiver…", "every box is a separate Linux process"),
            (14, "WAN cut — the node keeps committing, the outbox grows", "the receiver gets nothing while the link is down"),
            (44, "Processor SIGKILLed — the supervisor restarts it", "nothing admitted is lost: it is in the durable ingress table"),
            (76, "WAN restored — the backlog drains to the receiver", "only IDs the receiver acknowledged are marked sent")])
        await pg.wait_for_timeout(2000)
        await pg.evaluate("document.getElementById('dLog').scrollTo({top: 99999, behavior: 'smooth'})")
        await cap(pg, "[PASS] generated = committed = acknowledged = received", "0 pending · 0 duplicates · console API answered every poll", 5)
        await cap(pg, None)

        # ---- 3. Live node ----
        await card(pg, "s3", 4.5)
        await pg.goto(B); await pg.wait_for_timeout(1200)
        await click(pg, "nav >> text=Live node", 1.2)
        await cap(pg, "The running node: live PIDs, CPU, RAM, context switches", "read from /proc every second", 3.5)
        await click(pg, "text=Simulate WAN Internet Cut", 1.0)
        await cap(pg, "WAN cut: events keep coming, the outbox grows", "local decisions continue — the receiver stays flat")
        await pg.evaluate("__dm.hl('#kOb, #kSink', true)")
        s = now(); await pg.evaluate("__dm.fast(true, '▶▶ 3×')"); await pg.wait_for_timeout(15000)
        await pg.evaluate("__dm.fast(false)"); SEG.append((s, now(), 3))
        await click(pg, "text=Restore WAN Connection", 1.0)
        await cap(pg, "WAN restored: the backlog drains in seconds", None, 4)
        await pg.evaluate("__dm.hl('#kOb, #kSink', false)")
        await click(pg, "text=Reconcile now", 2.2)
        await pg.evaluate("__dm.hl('#recon', true)")
        await cap(pg, "Reconcile: both sides of the receiver boundary match", "no false 'sent' · no unknown IDs · Conserved: yes", 5)
        await pg.evaluate("__dm.hl('#recon', false)"); await cap(pg, None)

        await card(pg, "outro", 7.5)
        total = now()
        await ctx.close(); await b.close()
        json.dump({"segments": SEG, "total": total}, open(os.path.join(OUT, "segments.json"), "w"), indent=1)
        print("recorded", round(total, 1), "s; fast segments", SEG)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--base", default=B, help="console URL of the running node")
    ap.add_argument("--out", default=OUT, help="output directory (raw/ video + segments.json)")
    ap.add_argument("--chrome", default=None, help="Chrome/Chromium executable (default: Playwright's)")
    ap.add_argument("--batch-speed", type=int, default=BATCH_SPEED, help="speed-up shown for the Run-all batch")
    a = ap.parse_args()
    B, OUT, CHROME, BATCH_SPEED = a.base.rstrip("/"), a.out, a.chrome, a.batch_speed
    os.makedirs(os.path.join(OUT, "raw"), exist_ok=True)
    asyncio.run(main())
