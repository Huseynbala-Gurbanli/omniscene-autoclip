#!/usr/bin/env python3
"""
autoclip.py - auto-clip Twitch streams when chat spikes.

  streamlink  -> records the live stream to a .ts file
  twitch chat -> read anonymously (no account, no token)
  spike check -> counts chat messages every 10 s
  ffmpeg      -> cuts 30 s around each spike, makes it 9:16 vertical
  groq        -> Whisper in the cloud writes captions (3 words at a time)
  dashboard   -> live control room at http://localhost:8787

Setup (Mac):
  brew install streamlink ffmpeg
  pip3 install pillow
  export GROQ_API_KEY=gsk_...      # free key: console.groq.com/keys

Run:
  python3 autoclip.py                      # then add channels in the dashboard
  python3 autoclip.py ohnepixel kick.com/xqc youtube.com/@handle

Platforms:
  Twitch   built in
  Kick     pip3 install websocket-client curl_cffi
  YouTube  brew install yt-dlp
  (without these the channel still records; clip with the C button)
"""
import argparse, json, os, re, shutil, socket, subprocess, sys, threading, time, webbrowser

import ai
import layout
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WINDOW = 10          # seconds per chat bucket
HISTORY = 30         # buckets used for the baseline (5 min)
SPIKE_X = 2.5        # spike = this many times the average
MIN_MSGS = 15        # ...and at least this many messages in the bucket
CLIP_LEN = 30        # seconds per clip
PRE = 20             # seconds before the spike to include
COOLDOWN = 60        # min seconds between clips

HERE = os.path.dirname(os.path.abspath(__file__))
STATE = {"captions": False, "settings": {}, "channels": {}, "clips": [], "log": [],
         "caption_style": {}, "fonts": [], "presets": {}, "jobs": [], "ai": False, "setup": {}}
STATE_LOCK = threading.Lock()
CHANNELS = {}      # name -> Channel
CTX = {}           # out dir, captions flag
RENDER_SLOTS = threading.Semaphore(2)
SENSITIVITY = {"low": (3.5, 25), "medium": (2.5, 15), "high": (1.8, 10)}   # (x baseline, min msgs/10s)
CLIP_CFG = {"length": 30, "lead": 20, "cooldown": 60}                       # auto-clip defaults   # max clips rendering at once (keeps the Mac responsive)


def log(msg):
    line = f"[{datetime.now():%H:%M:%S}] {msg}"
    print(line, flush=True)
    with STATE_LOCK:
        STATE["log"] = (STATE["log"] + [line])[-60:]


# ---------------------------------------------------------------- one channel = recorder + chat + spike loop

KICK_PUSHER_KEYS = ["32cbd69e4b950bf97679", "eb1d5f283081a78b932c"]
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")


def parse_source(raw):
    """'xqc' | 'twitch.tv/xqc' -> twitch;  'kick.com/xqc' | 'kick:xqc' -> kick;
    'youtube.com/@handle' | '@handle' | 'yt:@handle' | youtube watch/live URL -> youtube."""
    t = raw.strip()
    low = t.lower()
    if m := re.search(r"kick\.com/([\w-]+)", low) or re.fullmatch(r"kick:([\w-]+)", low):
        return "kick", m.group(1), f"https://kick.com/{m.group(1)}"
    if "youtu" in low or low.startswith(("@", "yt:")):
        if m := re.search(r"(?:v=|youtu\.be/|/live/)([\w-]{11})", t):
            return "youtube", m.group(1), f"https://www.youtube.com/watch?v={m.group(1)}"
        if m := re.search(r"@([\w.-]{3,30})", t):
            h = m.group(1)
            return "youtube", "@" + h.lower(), f"https://www.youtube.com/@{h}/live"
        return None, None, None
    m = re.search(r"twitch\.tv/(\w+)", low) or re.fullmatch(r"(?:twitch:)?#?(\w{2,25})", low)
    if m:
        return "twitch", m.group(1), f"https://twitch.tv/{m.group(1)}"
    return None, None, None


def http_json(url):
    """GET JSON with a browser fingerprint (Kick sits behind Cloudflare)."""
    try:
        from curl_cffi import requests as creq          # best: real Chrome TLS fingerprint
        r = creq.get(url, impersonate="chrome", timeout=15)
        r.raise_for_status()
        return r.json()
    except ImportError:
        pass
    r = subprocess.run(["curl", "-sS", "--fail", "-A", UA, "-H", "Accept: application/json", url],
                       capture_output=True, text=True, timeout=20)
    if r.returncode != 0:
        raise RuntimeError("blocked by Kick (try: pip3 install curl_cffi)")
    return json.loads(r.stdout)


class Channel:
    def __init__(self, platform, slug, url):
        self.platform, self.slug, self.url = platform, slug, url
        self.key = slug if platform == "twitch" else f"{platform}:{slug}"
        self.file_id = re.sub(r"[^\w-]", "", slug if platform == "twitch" else f"{platform}-{slug}")
        self.stop = threading.Event()
        self.manual = threading.Event()
        self.counter, self.lock = [0], threading.Lock()
        self.rec = os.path.join(CTX["out"], f"{self.file_id}_{datetime.now():%Y%m%d_%H%M%S}.ts")
        self.t0 = time.time()
        self.proc = None
        self.chat_proc = None
        self.view = {"name": self.key, "label": slug, "platform": platform, "url": url,
                     "started": self.t0, "recording": self.rec, "rec_bytes": 0, "status": "starting",
                     "chat_connected": False, "chat_note": "", "buckets": [], "embed": self._embed(),
                     "auto": True, "sensitivity": "medium", "mark": None, "recorded": 0}
        self.requests, self.rec_t0 = [], None

    @property
    def name(self):
        return self.key

    def _embed(self, video_id=None):
        """Official player URL for the live view in the dashboard (muted autoplay)."""
        if self.platform == "twitch":
            return (f"https://player.twitch.tv/?channel={self.slug}&parent=localhost&parent=127.0.0.1"
                    "&muted=true&autoplay=true")
        if self.platform == "kick":
            return f"https://player.kick.com/{self.slug}?muted=true&autoplay=true"
        vid = video_id or (self.slug if re.fullmatch(r"[\w-]{11}", self.slug) else None)
        return f"https://www.youtube.com/embed/{vid}?autoplay=1&mute=1&playsinline=1" if vid else ""

    def _resolve_youtube(self):
        """@handle/live -> the current live video id, so the player can be embedded."""
        if not shutil.which("yt-dlp"):
            return
        try:
            r = subprocess.run(["yt-dlp", "--no-warnings", "--skip-download", "--print", "id", self.url],
                               capture_output=True, text=True, timeout=40)
            vid = (r.stdout.strip().splitlines() or [""])[-1]
            if re.fullmatch(r"[\w-]{11}", vid):
                self.set(embed=self._embed(vid))
        except Exception:
            pass

    def set(self, **kw):
        with STATE_LOCK:
            self.view.update(kw)

    def start(self):
        STATE["channels"][self.key] = self.view
        if self.platform == "youtube" and not self.view["embed"]:
            threading.Thread(target=self._resolve_youtube, daemon=True).start()
        threading.Thread(target=self._chat, daemon=True).start()
        threading.Thread(target=self._loop, daemon=True).start()

    def remove(self):
        self.stop.set()
        self.manual.set()
        for p in (self.proc, self.chat_proc):
            if p and p.poll() is None:
                p.terminate()
        with STATE_LOCK:
            STATE["channels"].pop(self.key, None)
        log(f"removed {self.key}")

    def _count(self, n=1):
        with self.lock:
            self.counter[0] += n

    # ---- chat readers: each just calls self._count() per message
    def _chat(self):
        reader = {"twitch": self._chat_twitch, "kick": self._chat_kick,
                  "youtube": self._chat_youtube}[self.platform]
        fails = 0
        while not self.stop.is_set():
            try:
                reader()
                fails = 0
            except Exception as e:
                fails += 1
                if fails >= 5:
                    self.set(chat_note="manual only · chat unavailable")
                    log(f"{self.key}: chat keeps failing ({e}) -> manual clips only (press C)")
                elif not self.stop.is_set():
                    log(f"{self.key}: chat error ({e}), retrying...")
            self.set(chat_connected=False)
            if self.view.get("chat_note") or self.stop.is_set():
                return                       # permanent problem -> manual-only mode
            time.sleep(5)

    def _chat_twitch(self):
        s = socket.create_connection(("irc.chat.twitch.tv", 6667), timeout=30)
        s.sendall(b"NICK justinfan%d\r\n" % (int(time.time() * 1000) % 1000000))
        s.sendall(f"JOIN #{self.slug}\r\n".encode())
        self.set(chat_connected=True)
        log(f"{self.key}: chat connected")
        buf = ""
        try:
            while not self.stop.is_set():
                data = s.recv(4096).decode("utf-8", "ignore")
                if not data:
                    break
                buf += data
                *lines, buf = buf.split("\r\n")
                for line in lines:
                    if line.startswith("PING"):
                        s.sendall(b"PONG :tmi.twitch.tv\r\n")
                    elif "PRIVMSG" in line:
                        self._count()
        finally:
            s.close()

    def _chat_kick(self):
        try:
            import websocket  # websocket-client
        except ImportError:
            self.set(chat_note="manual only · pip3 install websocket-client")
            log(f"{self.key}: websocket-client missing -> pip3 install websocket-client")
            return
        try:
            info = http_json(f"https://kick.com/api/v2/channels/{self.slug}")
            room = info["chatroom"]["id"]
        except Exception as e:
            self.set(chat_note="manual only · Kick API blocked")
            log(f"{self.key}: can't read Kick chat ({e}) -> manual clips only (press C)")
            return
        for key in KICK_PUSHER_KEYS:
            ws = websocket.create_connection(
                f"wss://ws-us2.pusher.com/app/{key}?protocol=7&client=js&version=8.4.0&flash=false",
                timeout=40, header=[f"User-Agent: {UA}"], origin="https://kick.com")
            try:
                ok = False
                deadline = time.time() + 10
                while not self.stop.is_set():
                    msg = json.loads(ws.recv())
                    ev = msg.get("event", "")
                    if ev == "pusher:connection_established":
                        ws.send(json.dumps({"event": "pusher:subscribe",
                                            "data": {"auth": "", "channel": f"chatrooms.{room}.v2"}}))
                    elif ev == "pusher_internal:subscription_succeeded":
                        ok = True
                        self.set(chat_connected=True)
                        log(f"{self.key}: chat connected")
                    elif ev == "pusher:ping":
                        ws.send(json.dumps({"event": "pusher:pong", "data": {}}))
                    elif ev.endswith("ChatMessageEvent"):
                        self._count()
                    elif ev == "pusher:error" and not ok:
                        break                 # wrong app key -> try the next one
                    if not ok and time.time() > deadline:
                        break
                if ok:
                    return                    # connection dropped after working -> outer loop retries
            finally:
                ws.close()
        raise RuntimeError("could not subscribe to Kick chat")

    def _chat_youtube(self):
        if not shutil.which("yt-dlp"):
            self.set(chat_note="manual only · brew install yt-dlp")
            log(f"{self.key}: yt-dlp missing -> brew install yt-dlp")
            return
        base = os.path.join(CTX["out"], f".chat_{self.file_id}")
        out = base + ".live_chat.json"
        if os.path.exists(out):
            os.remove(out)
        self.chat_proc = subprocess.Popen(
            ["yt-dlp", "--skip-download", "--write-subs", "--sub-langs", "live_chat",
             "--no-part", "-o", base, self.url],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        pos, announced = 0, False
        try:
            while not self.stop.is_set() and self.chat_proc.poll() is None:
                time.sleep(1)
                if not os.path.exists(out):
                    continue
                if not announced:
                    announced = True
                    self.set(chat_connected=True)
                    log(f"{self.key}: chat connected")
                with open(out, "rb") as f:
                    f.seek(pos)
                    chunk = f.read()
                cut = chunk.rfind(b"\n") + 1      # only complete lines
                pos += cut
                n = chunk[:cut].count(b"liveChatTextMessageRenderer") \
                    + chunk[:cut].count(b"liveChatPaidMessageRenderer")
                if n:
                    self._count(n)
        finally:
            if self.chat_proc.poll() is None:
                self.chat_proc.terminate()
            if os.path.exists(out):
                os.remove(out)

    def _fire(self, start, reason, peak, length=None):
        name = f"{self.file_id}_{datetime.now():%Y%m%d_%H%M%S}.mp4"
        threading.Thread(target=make_clip, args=(self.rec, start, CTX["out"], name, captions_ready(),
                                                 reason, peak, self.key),
                         kwargs={"length": length}, daemon=True).start()

    def rec_pos(self):
        """Seconds of stream recorded so far (clock starts when the first bytes land)."""
        return time.time() - self.rec_t0 if self.rec_t0 else 0.0

    def request(self, **req):
        """Manual actions from the dashboard: {'seconds': 30} or {'mark': 'start'|'end'}."""
        with self.lock:
            self.requests.append(req)
        self.manual.set()

    def _recorders(self):
        """Every streamlink we can find: the private env's first, then Homebrew/system ones."""
        found = []
        for p in [os.path.join(os.path.dirname(VENV_PY), "streamlink"), shutil.which("streamlink"),
                  "/opt/homebrew/bin/streamlink", "/usr/local/bin/streamlink"]:
            if p and os.path.exists(p) and os.path.realpath(p) not in [os.path.realpath(f) for f in found]:
                found.append(p)
        return found or ["streamlink"]

    def _start_recorder(self, exe):
        extra = ["--twitch-disable-ads"] if self.platform == "twitch" else []
        self.rec_log = self.rec + ".log"
        self.proc = subprocess.Popen([exe, *extra, self.url, "best", "-o", self.rec, "-f"],
                                     stdout=subprocess.DEVNULL, stderr=open(self.rec_log, "w"))

    def _recorder_error(self):
        try:
            with open(self.rec_log) as f:
                lines = [l.strip() for l in f if l.strip()]
        except OSError:
            return ""
        err = [l for l in lines if re.search(r"error|no playable|unable|could not|failed|traceback", l, re.I)]
        return (err or lines or [""])[-1][:220]

    def _loop(self):
        log(f"recording {self.url}")
        recorders = self._recorders()
        self._start_recorder(recorders.pop(0))
        self.set(status="live")
        hist, last_clip = deque(maxlen=HISTORY), -COOLDOWN
        next_tick = time.time() + WINDOW
        while not self.stop.is_set():
            self.manual.wait(max(0.05, next_tick - time.time()))
            if self.stop.is_set():
                break
            if self.proc.poll() is not None:
                started_ok = os.path.exists(self.rec) and os.path.getsize(self.rec) > 0
                err = self._recorder_error()
                if not started_ok and recorders:        # this streamlink failed -> try another one
                    exe = recorders.pop(0)
                    log(f"{self.key}: recorder failed ({err or 'no output'}), retrying with {exe}")
                    self._start_recorder(exe)
                    continue
                self.set(status="ended" if started_ok else "offline", error=err)
                log(f"{self.key}: " + ("stream ended" if started_ok else "can't record") + (f" - {err}" if err else ""))
                break
            if not self.rec_t0 and os.path.exists(self.rec) and os.path.getsize(self.rec) > 0:
                self.rec_t0 = time.time()
            now = self.rec_pos()
            self.set(recorded=round(now))
            if self.manual.is_set():
                self.manual.clear()
                with self.lock:
                    reqs, self.requests = self.requests, []
                for r in reqs:
                    self._handle(r, now)
            if time.time() < next_tick:          # manual action woke us early: keep 10 s chat windows exact
                continue
            next_tick += WINDOW
            with self.lock:
                n, self.counter[0] = self.counter[0], 0
            avg = sum(hist) / len(hist) if hist else 0
            x, min_msgs = SENSITIVITY.get(self.view["sensitivity"], SENSITIVITY["medium"])
            spike = len(hist) >= 6 and n >= min_msgs and n > avg * x
            print(f"[{datetime.now():%H:%M:%S}] {self.key:<18} chat {n:4d}/10s  avg {avg:6.1f}"
                  + ("  🔥 SPIKE" if spike else ""), flush=True)
            with STATE_LOCK:
                self.view["buckets"] = (self.view["buckets"] + [
                    {"t": time.time(), "n": n, "avg": round(avg, 1), "spike": spike}])[-360:]
            hist.append(n)
            if spike and self.view["auto"] and now - last_clip > CLIP_CFG["cooldown"] and now > 15:
                last_clip = now
                length, lead = CLIP_CFG["length"], min(CLIP_CFG["lead"], CLIP_CFG["length"] - 5)
                log(f"🔥 {self.key}: spike {n} msgs (avg {avg:.0f}) -> {length}s clip")
                threading.Timer(max(0, length - lead), self._fire,
                                args=(now - WINDOW - lead, "spike", n, length)).start()

    def _handle(self, r, now):
        if "seconds" in r:
            secs = max(5, min(300, int(r["seconds"])))
            if now < 8:
                log(f"{self.key}: too early - nothing recorded yet")
                return
            secs = min(secs, int(now))
            log(f"✂️  {self.key}: clip of the last {secs}s")
            self._fire(now - secs, "manual", 0, secs)
        elif r.get("mark") == "start":
            self.set(mark=round(now, 1), mark_at=time.time())
            log(f"⏺  {self.key}: clip started")
        elif r.get("mark") == "end" and self.view.get("mark") is not None:
            start = max(0.0, self.view["mark"] - 1.5)
            length = min(300.0, now - start + 1.0)
            self.set(mark=None, mark_at=None)
            if length < 3:
                return
            log(f"⏹  {self.key}: clip from your marks ({length:.0f}s)")
            self._fire(start, "manual", 0, length)
        elif r.get("mark") == "cancel":
            self.set(mark=None, mark_at=None)


def add_channel(raw):
    platform, slug, url = parse_source(raw)
    if not platform:
        return None, "not a Twitch / Kick / YouTube channel"
    ch = Channel(platform, slug, url)
    if ch.key in CHANNELS and not CHANNELS[ch.key].stop.is_set():
        return None, "already added"
    CHANNELS[ch.key] = ch
    ch.start()
    return ch, None


# ---------------------------------------------------------------- clipping

def set_clip(name, **kw):
    with STATE_LOCK:
        for c in STATE["clips"]:
            if c["name"] == name:
                c.update(kw)
                return
        STATE["clips"].insert(0, {"name": name, **kw})


def make_clip(src, start, out_dir, name, captions, reason="spike", peak=0, channel="",
              length=None, words=None, meta=None, context=""):
    """Cut -> transcribe -> AI score -> layout -> captions -> final 9:16 mp4 (+ .json sidecar)."""
    length = length or CLIP_LEN
    raw = os.path.join(out_dir, name.replace(".mp4", "_raw.mp4"))
    final = os.path.join(out_dir, name)
    capdir = raw.replace(".mp4", "_caps")
    st = clean_style(STATE.get("caption_style"))
    set_clip(name, status="cutting", reason=reason, peak=peak, created=time.time(), caption="",
             channel=channel)
    RENDER_SLOTS.acquire()
    try:
        start = max(0.0, float(start))
        # 1) frame-accurate cut (re-encode, fast) so word timings line up exactly
        subprocess.run(["ffmpeg", "-y", "-loglevel", "fatal", "-ss", f"{start:.3f}", "-i", src,
                        "-t", f"{length:.3f}", "-map", "0:v:0", "-map", "0:a:0?",
                        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "16",
                        "-c:a", "aac", "-b:a", "192k", raw], check=True)
        # 2) transcript
        note = ""
        if words is None:
            words = []
            if ai.has_key():
                set_clip(name, status="transcribing")
                try:
                    words = ai.transcribe(raw)
                    if not words:
                        note = "No speech found, so no captions"
                except Exception as e:
                    note = "Captions failed: " + str(e)[:120]
                    log(f"transcription failed ({e})")
            else:
                note = "No captions: add your Groq key in Settings"
        text = " ".join(w["word"].strip() for w in words)
        # 3) AI score / title / hashtags
        if meta is None and ai.has_key():
            set_clip(name, status="scoring")
            try:
                meta = ai.score_clip(text, context or f"{channel} livestream, clipped because: {reason}"
                                     + (f" (chat spiked to {peak} msgs/10s)" if peak else ""))
            except Exception as e:
                log(f"AI scoring failed ({e})")
        meta = meta or {}
        # 4) layout
        set_clip(name, status="rendering", **meta)
        sw, sh = layout.probe_size(raw)
        face = layout.detect_facecam(raw) if st["layout"] == "split" else None
        if st["layout"] == "split" and not face:
            log(f"{name}: no facecam found -> blur layout")
        vf, used = layout.build_filter(st["layout"], sw, sh, face)
        # 5) captions (transparent PNG track -> no libass needed)
        inputs = ["-i", raw]
        if words and not has_module("PIL"):
            note = "No captions: Pillow is missing (Settings → Fix)"
        elif words and not st["enabled"]:
            note = "Captions are turned off in Settings"
        if words and captions and st["enabled"]:
            playlist = render_captions(words, capdir, st)
            inputs += ["-f", "concat", "-safe", "0", "-i", playlist]
            vf += ";[base][1:v]overlay=0:0:eof_action=repeat[v]"
        else:
            vf += ";[base]null[v]"
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", *inputs, "-filter_complex", vf,
                        "-map", "[v]", "-map", "0:a?", "-t", f"{length:.3f}", "-c:v", "libx264",
                        "-preset", "veryfast", "-crf", "20", "-pix_fmt", "yuv420p",
                        "-c:a", "aac", "-b:a", "160k", "-movflags", "+faststart", final],
                       check=True)
        info = {"status": "ready", "size": os.path.getsize(final), "caption": text[:140],
                "transcript": text[:4000], "layout": used, "length": round(length, 1),
                "note": note, **meta}
        set_clip(name, **info)
        with open(final.replace(".mp4", ".json"), "w") as f:
            json.dump({"reason": reason, "peak": peak, "channel": channel, **info}, f)
        log(f"✅ saved {final}" + (f"  · score {meta['score']} · {meta.get('title', '')}" if meta.get("score") is not None else ""))
    except Exception as e:
        set_clip(name, status="failed")
        log(f"clip failed: {e}")
    finally:
        RENDER_SLOTS.release()
        if os.path.exists(raw):
            os.remove(raw)
        shutil.rmtree(capdir, ignore_errors=True)


# ---------------------------------------------------------------- caption studio

# font name -> candidate files (macOS first, Linux fallbacks); (path, ttc index)
FONT_FILES = {
    "Arial Black": [("/System/Library/Fonts/Supplemental/Arial Black.ttf", 0),
                    ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 0)],
    "Impact":      [("/System/Library/Fonts/Supplemental/Impact.ttf", 0),
                    ("/usr/share/fonts/truetype/liberation/LiberationSansNarrow-Bold.ttf", 0)],
    "Helvetica":   [("/System/Library/Fonts/Helvetica.ttc", 1),
                    ("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf", 0)],
    "Rounded":     [("/System/Library/Fonts/Supplemental/Arial Rounded Bold.ttf", 0),
                    ("/usr/share/fonts/truetype/google-fonts/Poppins-Bold.ttf", 0)],
    "Georgia":     [("/System/Library/Fonts/Supplemental/Georgia Bold.ttf", 0),
                    ("/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf", 0)],
}

PRESETS = {
    "bold":    {"font": "Arial Black", "size": 100, "uppercase": True, "words": 3, "color": "#FFFFFF",
                "highlight": "#FFFFFF", "outline": 9, "box": False, "shadow": False, "pop": False},
    "karaoke": {"font": "Arial Black", "size": 100, "uppercase": True, "words": 3, "color": "#FFFFFF",
                "highlight": "#D4FF3A", "outline": 9, "box": False, "shadow": False, "pop": False},
    "pop":     {"font": "Impact", "size": 120, "uppercase": True, "words": 2, "color": "#FFFFFF",
                "highlight": "#FFD400", "outline": 10, "box": False, "shadow": True, "pop": True},
    "box":     {"font": "Helvetica", "size": 76, "uppercase": False, "words": 4, "color": "#FFFFFF",
                "highlight": "#FFFFFF", "outline": 0, "box": True, "shadow": False, "pop": False},
    "minimal": {"font": "Rounded", "size": 70, "uppercase": False, "words": 5, "color": "#FFFFFF",
                "highlight": "#FFFFFF", "outline": 0, "box": False, "shadow": True, "pop": False},
}
DEFAULT_STYLE = {"enabled": True, "preset": "karaoke", "position": 75, "layout": "blur", **PRESETS["karaoke"]}
_font_cache = {}


def load_font(name, size):
    from PIL import ImageFont
    key = (name, int(size))
    if key not in _font_cache:
        f = None
        for path, idx in FONT_FILES.get(name, []) + FONT_FILES["Arial Black"]:
            if os.path.exists(path):
                try:
                    f = ImageFont.truetype(path, int(size), index=idx)
                    break
                except OSError:
                    continue
        _font_cache[key] = f or ImageFont.load_default()
    return _font_cache[key]


def clean_style(st):
    out = dict(DEFAULT_STYLE)
    if isinstance(st, dict):
        if st.get("preset") in PRESETS and len(st) <= 2:
            out.update(PRESETS[st["preset"]])          # bare preset switch -> take preset defaults
        for k, v in st.items():
            if k in out:
                out[k] = v
    out["size"] = max(40, min(160, int(out["size"])))
    out["position"] = max(8, min(92, int(out["position"])))
    out["words"] = max(1, min(6, int(out["words"])))
    out["outline"] = max(0, min(16, int(out["outline"])))
    for k in ("color", "highlight"):
        if not re.fullmatch(r"#[0-9a-fA-F]{6}", str(out[k])):
            out[k] = DEFAULT_STYLE[k]
    if out["layout"] not in ("blur", "split", "fill"):
        out["layout"] = "blur"
    if out["font"] not in FONT_FILES:
        out["font"] = DEFAULT_STYLE["font"]
    return out


def draw_caption(texts, active, st):
    """One caption frame: list of words, index of the active (spoken) word or -1."""
    from PIL import Image, ImageDraw, ImageFilter
    W, H, maxw = 1080, 1920, 960
    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    if not texts:
        return img
    size = st["size"]
    texts = [t.upper() if st["uppercase"] else t for t in texts]
    probe = ImageDraw.Draw(img)

    def layout(sz):
        base = load_font(st["font"], sz)
        big = load_font(st["font"], int(sz * 1.18)) if st["pop"] else base
        fonts = [big if (i == active and st["pop"]) else base for i in range(len(texts))]
        widths = [probe.textlength(t, font=f) for t, f in zip(texts, fonts)]
        return base, fonts, widths, probe.textlength(" ", font=base)

    def split_lines(widths, space):
        total = sum(widths) + space * (len(widths) - 1)
        if total <= maxw or len(widths) == 1:
            return [list(range(len(widths)))]
        best, cut = None, 1
        for c in range(1, len(widths)):
            l1 = sum(widths[:c]) + space * (c - 1)
            l2 = sum(widths[c:]) + space * (len(widths) - c - 1)
            if best is None or max(l1, l2) < best:
                best, cut = max(l1, l2), c
        return [list(range(cut)), list(range(cut, len(widths)))]

    sz = size
    while True:
        base, fonts, widths, space = layout(sz)
        lines = split_lines(widths, space)
        too_wide = any(sum(widths[i] for i in ln) + space * (len(ln) - 1) > maxw for ln in lines)
        if not too_wide or sz <= 40:
            break
        sz -= 6
    asc, desc = base.getmetrics()
    line_h = int((asc + desc) * 1.08)
    cy = int(H * st["position"] / 100)
    top = cy - (line_h * len(lines)) // 2

    placed = []  # (x, baseline_y, text, font, is_active)
    for li, ln in enumerate(lines):
        lw = sum(widths[i] for i in ln) + space * (len(ln) - 1)
        x = (W - lw) / 2
        by = top + li * line_h + asc
        for i in ln:
            placed.append((x, by, texts[i], fonts[i], i == active))
            x += widths[i] + space
        if st["box"]:
            pad = int(sz * 0.32)
            box = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            ImageDraw.Draw(box).rounded_rectangle(
                ((W - lw) / 2 - pad, top + li * line_h - pad * 0.35, (W + lw) / 2 + pad,
                 top + (li + 1) * line_h + pad * 0.15), radius=int(sz * 0.28), fill=(10, 10, 12, 205))
            img = Image.alpha_composite(img, box)

    if st["shadow"]:
        sh = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        sd = ImageDraw.Draw(sh)
        off = max(4, sz // 18)
        for x, by, t, f, _ in placed:
            sd.text((x + off, by + off), t, font=f, anchor="ls", fill=(0, 0, 0, 170),
                    stroke_width=st["outline"], stroke_fill=(0, 0, 0, 170))
        img = Image.alpha_composite(img, sh.filter(ImageFilter.GaussianBlur(off)))

    d = ImageDraw.Draw(img)
    for x, by, t, f, is_act in placed:
        d.text((x, by), t, font=f, anchor="ls", fill=st["highlight"] if is_act else st["color"],
               stroke_width=st["outline"], stroke_fill="black")
    return img


def render_captions(words, outdir, style=None):
    """Words -> transparent PNG frames + concat playlist (a caption video track)."""
    from PIL import Image
    st = clean_style(style or STATE.get("caption_style"))
    os.makedirs(outdir, exist_ok=True)
    blank = os.path.abspath(os.path.join(outdir, "blank.png"))
    Image.new("RGBA", (1080, 1920), (0, 0, 0, 0)).save(blank)
    karaoke = st["highlight"].lower() != st["color"].lower() or st["pop"]
    segs = []   # (start, end, words_in_chunk, active)
    per = st["words"]
    for i in range(0, len(words), per):
        chunk = words[i:i + per]
        texts = [w["word"].strip() for w in chunk]
        c_end = float(chunk[-1]["end"])
        if karaoke:
            for j, w in enumerate(chunk):
                s0 = float(w["start"])
                s1 = float(chunk[j + 1]["start"]) if j + 1 < len(chunk) else c_end
                segs.append((s0, max(s1, s0 + 0.05), texts, j))
        else:
            segs.append((float(chunk[0]["start"]), c_end, texts, -1))
    lines, t, n = [], 0.0, 0
    for s0, s1, texts, act in segs:
        if s0 > t + 0.01:
            lines += [f"file '{blank}'", f"duration {s0 - t:.3f}"]
        png = os.path.abspath(os.path.join(outdir, f"c{n:04d}.png"))
        draw_caption(texts, act, st).save(png)
        n += 1
        dur = max(0.04, s1 - max(s0, t))
        lines += [f"file '{png}'", f"duration {dur:.3f}"]
        t = max(s1, t)
    lines += [f"file '{blank}'", "duration 0.1", f"file '{blank}'"]
    playlist = os.path.join(outdir, "caps.txt")
    with open(playlist, "w") as f:
        f.write("\n".join(lines) + "\n")
    return playlist


SAMPLE = [{"word": w} for w in "NO WAY HE JUST STOLE THE TRUCK".split()]


_face_cache = {}


def caption_preview(style, channel=None):
    """540x960 JPEG of the real vertical layout + captions (stream frame when available)."""
    from PIL import Image, ImageDraw
    import io
    st = clean_style(style)
    frame, face = None, None
    ch = CHANNELS.get(channel or "")
    if ch and os.path.exists(ch.rec) and os.path.getsize(ch.rec) > 2_000_000:
        jpg = os.path.join(CTX["out"], f".preview_{ch.file_id}.jpg")
        d = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of",
                            "csv=p=0", ch.rec], capture_output=True, text=True, timeout=15).stdout.strip()
        ss = max(0.0, float(d or 0) - 6) if re.fullmatch(r"[\d.]+", d or "") else 3.0
        if not os.path.exists(jpg) or time.time() - os.path.getmtime(jpg) > 60:
            subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{ss:.2f}", "-i", ch.rec,
                            "-frames:v", "1", jpg], timeout=20)
        if os.path.exists(jpg):
            frame = Image.open(jpg).convert("RGB")
        if st["layout"] == "split":
            hit = _face_cache.get(ch.key)
            if not hit or time.time() - hit[0] > 120:
                # detect on a small copy of the last 20 s (seeking a huge growing .ts is slow)
                tail = os.path.join(CTX["out"], f".facecheck_{ch.file_id}.ts")
                subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{max(0.0, ss - 14):.2f}",
                                "-i", ch.rec, "-t", "20", "-c", "copy", tail], timeout=30)
                _face_cache[ch.key] = hit = (time.time(), layout.detect_facecam(tail, samples=8))
                if os.path.exists(tail):
                    os.remove(tail)
            face = hit[1]
            if face and frame is not None:      # detection ran on recording pixels; frame may differ
                sw, _ = layout.probe_size(ch.rec)
                k = frame.width / sw
                face = tuple(int(v * k) for v in face)
    if frame is None:
        frame = Image.new("RGB", (1920, 1080), (34, 36, 42))
        dd = ImageDraw.Draw(frame)
        for x in range(0, 1920, 8):
            dd.line([(x, 0), (x + 400, 1080)], fill=(40 + (x * 7 % 13), 43, 52), width=3)
        if st["layout"] == "split":            # fake webcam so the layout is visible
            dd.rectangle((1500, 40, 1880, 330), fill=(70, 74, 84))
            dd.ellipse((1640, 110, 1740, 230), fill=(150, 125, 110))
            face = (1640, 110, 100, 120)
    canvas = layout.compose_preview(frame, st["layout"], face)
    texts = [w["word"] for w in SAMPLE[:st["words"]]]
    act = min(1, len(texts) - 1) if (st["highlight"].lower() != st["color"].lower() or st["pop"]) else -1
    canvas = Image.alpha_composite(canvas.convert("RGBA"), draw_caption(texts, act, st))
    buf = io.BytesIO()
    canvas.convert("RGB").resize((540, 960)).save(buf, "JPEG", quality=85)
    return buf.getvalue()


def save_style(st):
    STATE["caption_style"] = st
    try:
        with open(os.path.join(CTX["out"], ".caption_style.json"), "w") as f:
            json.dump(st, f)
    except OSError:
        pass


# ---------------------------------------------------------------- setup: key, dependencies, self-check

CONFIG = os.path.join(os.path.expanduser("~"), ".autoclip", "config.json")
PIP_PKGS = {"pillow": "PIL", "opencv-python-headless": "cv2", "websocket-client": "websocket",
            "curl_cffi": "curl_cffi", "streamlink": "streamlink", "yt-dlp": "yt_dlp"}


def load_config():
    try:
        with open(CONFIG) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_config(**kw):
    cfg = {**load_config(), **kw}
    os.makedirs(os.path.dirname(CONFIG), exist_ok=True)
    with open(CONFIG, "w") as f:
        json.dump(cfg, f)
    os.chmod(CONFIG, 0o600)


def has_module(mod):
    import importlib.util
    importlib.invalidate_caches()
    return importlib.util.find_spec(mod) is not None


def captions_ready():
    return not CTX.get("no_captions") and ai.has_key() and has_module("PIL")


def refresh_setup():
    st = {"groq": ai.has_key(), "groq_ok": STATE.get("setup", {}).get("groq_ok"),
          "pillow": has_module("PIL"), "opencv": has_module("cv2"),
          "websocket": has_module("websocket"), "curl_cffi": has_module("curl_cffi"),
          "yt_dlp": bool(shutil.which("yt-dlp")), "streamlink": bool(shutil.which("streamlink")),
          "ffmpeg": bool(shutil.which("ffmpeg")), "python": sys.executable,
          "installing": STATE.get("setup", {}).get("installing", ""),
          "restarting": STATE.get("setup", {}).get("restarting", False), "venv": in_venv(),
          "error": STATE.get("setup", {}).get("error", "")}
    with STATE_LOCK:
        STATE["setup"] = st
        STATE["captions"] = captions_ready()
        STATE["ai"] = ai.has_key()
        STATE["facecam"] = st["opencv"]
    return st


def test_groq_key(key):
    r = subprocess.run(["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
                        "https://api.groq.com/openai/v1/models", "-H", f"Authorization: Bearer {key}"],
                       capture_output=True, text=True, timeout=20)
    return r.stdout.strip()


def set_groq_key(key):
    key = (key or "").strip()
    if not re.fullmatch(r"gsk_[A-Za-z0-9]{20,}", key):
        return "That doesn't look like a Groq key (it starts with gsk_)"
    code = test_groq_key(key)
    if code == "401":
        return "Groq says this key is invalid"
    if code not in ("200", "429"):
        return f"Couldn't reach Groq to check the key (HTTP {code or 'error'})"
    os.environ["GROQ_API_KEY"] = key
    save_config(groq_key=key)
    STATE.setdefault("setup", {})["groq_ok"] = True
    refresh_setup()
    log("Groq key saved - captions and AI titles are on")
    return None


VENV = os.path.join(HERE, ".venv")
VENV_PY = os.path.join(VENV, "bin", "python")


def in_venv():
    return os.path.realpath(sys.prefix) == os.path.realpath(VENV)


def _pip_error(r):
    lines = [l for l in (r.stderr or r.stdout or "").strip().splitlines() if l.strip()]
    return lines[-1][:200] if lines else f"exit code {r.returncode}"


def _install_fail(msg):
    STATE["setup"]["error"] = msg
    log(msg)


def test_ai():
    """Real calls to Groq: speech-to-text + the title/score model. Tells the user plainly what works."""
    out = {"stt": "", "llm": "", "ok": False}
    if not ai.has_key():
        out["llm"] = out["stt"] = "No Groq key saved"
        return out
    tmp = os.path.join(CTX["out"], ".aitest.mp3")
    try:
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "lavfi", "-i", "sine=f=440:d=2",
                        "-ac", "1", "-ar", "16000", tmp], check=True, timeout=30)
        ai._curl([f"{ai.API}/audio/transcriptions", "-F", f"file=@{tmp}", "-F", f"model={ai.STT_MODEL}",
                  "-F", "response_format=verbose_json", "-F", "timestamp_granularities[]=word"], tries=2)
        out["stt"] = "ok"
    except Exception as e:
        out["stt"] = str(e)[:200]
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    try:
        m = ai.score_clip("no way he just stole the police truck and drove it off the pier, chat is going insane",
                          "test: GTA 6 livestream")
        out["llm"] = "ok" if m.get("title") else "the model answered but gave no title"
        out["sample"] = m
    except Exception as e:
        out["llm"] = str(e)[:200]
    out["ok"] = out["stt"] == "ok" and out["llm"] == "ok"
    log("AI test: " + ("everything works" if out["ok"] else f"speech={out['stt']} · titles={out['llm']}"))
    return out


def install_pkg(pkg):
    """Install Python parts into autoclip's own private environment (.venv in the project).
    Homebrew's Python refuses direct installs ("externally managed"), a private venv always works.
    After installing, autoclip restarts itself inside that environment."""
    if pkg not in PIP_PKGS or STATE["setup"].get("installing"):
        return
    STATE["setup"]["installing"] = pkg
    STATE["setup"]["error"] = ""
    log(f"installing {pkg} ...")
    try:
        if in_venv():
            r = subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg], capture_output=True, text=True)
            if r.returncode != 0:
                _install_fail(f"couldn't install {pkg}: {_pip_error(r)}")
            else:
                log(f"✓ {pkg} installed")
            return
        if not os.path.exists(VENV_PY):
            log("creating autoclip's private Python environment (one time) ...")
            r = subprocess.run([sys.executable, "-m", "venv", VENV], capture_output=True, text=True)
            if r.returncode != 0:
                _install_fail(f"couldn't create the environment: {_pip_error(r)}")
                return
        # bring along everything that already works + pillow (captions) + what was asked for
        want = {"pillow", pkg} | {p for p, m in PIP_PKGS.items() if has_module(m)}
        r = subprocess.run([VENV_PY, "-m", "pip", "install", "-q", *sorted(want)], capture_output=True, text=True)
        if r.returncode != 0:
            _install_fail(f"couldn't install {pkg}: {_pip_error(r)}")
            return
        log(f"✓ installed {', '.join(sorted(want))} - restarting autoclip to use them ...")
        STATE["setup"]["restarting"] = True
        threading.Timer(1.5, restart_in_venv).start()
    finally:
        STATE["setup"]["installing"] = ""
        refresh_setup()


def restart_in_venv():
    a = CTX["args"]
    urls = [ch.url for ch in CHANNELS.values() if not ch.stop.is_set()]
    for ch in list(CHANNELS.values()):
        ch.stop.set()
        for p in (ch.proc, ch.chat_proc):
            if p and p.poll() is None:
                p.terminate()
    argv = [VENV_PY, os.path.abspath(__file__), *urls, "--out", a.out, "--port", str(a.port), "--no-browser"]
    if a.no_captions:
        argv.append("--no-captions")
    time.sleep(0.5)
    os.execv(VENV_PY, argv)


# ---------------------------------------------------------------- long video -> best clips

def set_job(job, **kw):
    with STATE_LOCK:
        job.update(kw)


def run_vod(job):
    out = CTX["out"]
    work = os.path.join(out, f".vod_{job['id']}")
    os.makedirs(work, exist_ok=True)
    src, downloaded = job["url"], False
    try:
        if not ai.has_key():
            raise RuntimeError("needs GROQ_API_KEY (transcript + AI picking)")
        # 1) get the video
        if os.path.isfile(os.path.expanduser(src)):
            src = os.path.expanduser(src)
            job_title = os.path.basename(src)
        else:
            if not shutil.which("yt-dlp"):
                raise RuntimeError("yt-dlp missing -> brew install yt-dlp")
            set_job(job, stage="downloading", progress=0)
            p = subprocess.Popen(
                ["yt-dlp", "--newline", "--no-simulate", "--no-playlist",
                 "--print", "before_dl:TITLE:%(title)s", "--print", "after_move:FILE:%(filepath)s",
                 "-f", "bv*[height<=1080][ext=mp4]+ba[ext=m4a]/bv*[height<=1080]+ba/b[height<=1080]/b",
                 "--merge-output-format", "mp4", "-o", os.path.join(work, "video.%(ext)s"), src],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            job_title, path, tail = "", "", []
            for line in p.stdout:
                line = line.strip()
                tail = (tail + [line])[-5:]
                if line.startswith("TITLE:"):
                    job_title = line[6:]
                    set_job(job, title=job_title)
                elif line.startswith("FILE:"):
                    path = line[5:]
                elif (m := re.search(r"\[download\]\s+([\d.]+)%", line)):
                    set_job(job, progress=float(m.group(1)) / 100)
                if job.get("cancel"):
                    p.terminate()
            p.wait()
            if job.get("cancel"):
                raise RuntimeError("cancelled")
            if p.returncode != 0 or not path or not os.path.exists(path):
                raise RuntimeError("download failed: " + " | ".join(tail)[-200:])
            src, downloaded = path, True
        set_job(job, title=job.get("title") or job_title)
        dur = float(subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                                    "-of", "csv=p=0", src], capture_output=True, text=True).stdout or 0)
        set_job(job, duration=dur)
        # 2) transcript
        set_job(job, stage="transcribing", progress=0)
        words = ai.transcribe_long(src, os.path.join(work, "audio"),
                                   lambda d, t: set_job(job, progress=d / max(t, 1)))
        if job.get("cancel"):
            raise RuntimeError("cancelled")
        if len(words) < 30:
            raise RuntimeError("almost no speech found in this video")
        # 3) AI picks moments
        set_job(job, stage="finding moments", progress=0)
        moments = ai.find_moments(words, count=job["count"], min_len=job["min_len"], max_len=job["max_len"],
                                  context=f"video titled: {job.get('title') or job_title}",
                                  progress=lambda d, t: set_job(job, progress=d / max(t, 1)))
        if not moments:
            raise RuntimeError("AI found no strong moments")
        set_job(job, found=len(moments))
        # 4) render
        slug = re.sub(r"[^\w-]", "", re.sub(r"\s+", "-", (job.get("title") or "video").lower()))[:24] or "video"
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for k, m in enumerate(moments, 1):
            if job.get("cancel"):
                raise RuntimeError("cancelled")
            set_job(job, stage=f"rendering {k}/{len(moments)}", progress=(k - 1) / len(moments))
            name = f"vod-{slug}_{stamp}_{k}.mp4"
            meta = {key: m[key] for key in ("score", "title", "hook", "description", "hashtags", "why")}
            make_clip(src, m["start"], out, name, captions_ready(), reason="vod", channel=f"vod:{slug}",
                      length=m["end"] - m["start"], words=ai.words_between(words, m["start"], m["end"]),
                      meta=meta)
            job["clips"].append(name)
        set_job(job, stage="done", progress=1)
        log(f"🎬 long video done: {len(moments)} clips from {job.get('title') or job['url']}")
    except Exception as e:
        set_job(job, stage="failed", error=str(e)[:200])
        log(f"long video failed: {e}")
    finally:
        if downloaded and not job.get("keep"):
            shutil.rmtree(work, ignore_errors=True)
        else:
            shutil.rmtree(os.path.join(work, "audio"), ignore_errors=True)


def looks_like_video(raw):
    """Decide: live channel to watch, or a video to cut into clips."""
    t = raw.strip()
    if os.path.isfile(os.path.expanduser(t)):
        return True
    low = t.lower()
    if re.search(r"twitch\.tv/videos/|kick\.com/[\w-]+/videos?/|kick\.com/video/|youtube\.com/shorts/", low):
        return True
    if re.search(r"(?:youtube\.com/watch\?v=|youtu\.be/)", low):
        # a watch link can be a live stream; ask yt-dlp
        if shutil.which("yt-dlp"):
            try:
                r = subprocess.run(["yt-dlp", "--no-warnings", "--skip-download", "--print", "%(is_live)s", t],
                                   capture_output=True, text=True, timeout=25)
                return r.stdout.strip().splitlines()[-1:] != ["True"]
            except Exception:
                return True
        return True
    if re.match(r"https?://", low) and not re.search(r"(twitch\.tv|kick\.com|youtube\.com/@|youtube\.com/c/|youtube\.com/channel/)", low):
        return True            # any other site link: treat as a video
    return False


def start_any(raw, count=5, min_len=20, max_len=60):
    raw = (raw or "").strip()
    if not raw:
        return None, None, "Paste a link first"
    if looks_like_video(raw):
        job, err = start_vod(raw, count, min_len, max_len)
        return "video", (job or {}).get("id"), err
    ch, err = add_channel(raw)
    return "live", ch.key if ch else None, err


def start_vod(url, count=6, min_len=20, max_len=60):
    url = (url or "").strip()
    if not url:
        return None, "paste a link or a file path"
    job = {"id": f"{int(time.time() * 1000) % 10**9}", "url": url, "title": "", "stage": "queued",
           "progress": 0, "count": max(1, min(15, int(count))), "min_len": int(min_len),
           "max_len": int(max_len), "clips": [], "error": "", "created": time.time()}
    with STATE_LOCK:
        STATE["jobs"].insert(0, job)
    threading.Thread(target=run_vod, args=(job,), daemon=True).start()
    return job, None


# ---------------------------------------------------------------- dashboard server

def make_handler(out_dir):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype="application/json"):
            if isinstance(body, (dict, list)):
                body = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            try:
                return json.loads(self.rfile.read(n) or b"{}")
            except ValueError:
                return {}

        def do_GET(self):
            path = self.path.split("?")[0]
            if path == "/":
                with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                    return self._send(200, f.read(), "text/html; charset=utf-8")
            if path == "/api/state":
                if time.time() - CTX.get("setup_at", 0) > 5:
                    CTX["setup_at"] = time.time()
                    refresh_setup()
                with STATE_LOCK:
                    for v in STATE["channels"].values():
                        if os.path.exists(v["recording"]):
                            v["rec_bytes"] = os.path.getsize(v["recording"])
                    body = json.dumps({**STATE, "now": time.time()}).encode()
                return self._send(200, body)
            if path == "/api/caption_preview":
                from urllib.parse import parse_qs
                q = parse_qs(self.path.split("?", 1)[1] if "?" in self.path else "")
                try:
                    st = json.loads(q.get("s", ["{}"])[0])
                except ValueError:
                    st = {}
                try:
                    return self._send(200, caption_preview(st, q.get("ch", [""])[0]), "image/jpeg")
                except ImportError:
                    return self._send(500, {"error": "pip3 install pillow"})
            m = re.fullmatch(r"/clips/([\w.-]+\.mp4)", path)
            if m:
                return self._video(os.path.join(out_dir, m.group(1)))
            self._send(404, {"error": "not found"})

        def do_POST(self):
            path, body = self.path.split("?")[0], self._body()
            if path == "/api/channels":
                ch, err = add_channel(str(body.get("name", "")))
                return self._send(400 if err else 200, {"error": err} if err else {"ok": True, "name": ch.key})
            if path == "/api/channels/remove":
                ch = CHANNELS.pop(str(body.get("name", "")), None)
                if ch:
                    ch.remove()
                return self._send(200, {"ok": bool(ch)})
            if path == "/api/clip":
                ch = CHANNELS.get(str(body.get("name", "")))
                if not ch or ch.stop.is_set():
                    return self._send(400, {"error": "This stream isn't recording"})
                if body.get("mark") in ("start", "end", "cancel"):
                    ch.request(mark=body["mark"])
                else:
                    ch.request(seconds=body.get("seconds", 30))
                return self._send(200, {"ok": True})
            if path == "/api/channel/settings":
                ch = CHANNELS.get(str(body.get("name", "")))
                if not ch:
                    return self._send(400, {"error": "unknown stream"})
                if "auto" in body:
                    ch.set(auto=bool(body["auto"]))
                if body.get("sensitivity") in SENSITIVITY:
                    ch.set(sensitivity=body["sensitivity"])
                return self._send(200, {"ok": True})
            if path == "/api/clip_settings":
                for k, lo, hi in (("length", 10, 180), ("lead", 0, 120), ("cooldown", 10, 900)):
                    if k in body:
                        CLIP_CFG[k] = max(lo, min(hi, int(body[k])))
                STATE["clip_cfg"] = dict(CLIP_CFG)
                try:
                    with open(os.path.join(CTX["out"], ".clip_settings.json"), "w") as f:
                        json.dump(CLIP_CFG, f)
                except OSError:
                    pass
                return self._send(200, dict(CLIP_CFG))
            if path == "/api/test_ai":
                return self._send(200, test_ai())
            if path == "/api/start":
                kind, ident, err = start_any(body.get("input"), body.get("count", 5),
                                             body.get("min_len", 20), body.get("max_len", 60))
                return self._send(400 if err else 200, {"error": err} if err else {"ok": True, "kind": kind, "id": ident})
            if path == "/api/setup/key":
                err = set_groq_key(body.get("key"))
                return self._send(400 if err else 200, {"error": err} if err else {"ok": True})
            if path == "/api/setup/install":
                threading.Thread(target=install_pkg, args=(str(body.get("pkg", "")),), daemon=True).start()
                return self._send(200, {"ok": True})
            if path == "/api/clips/delete":
                n = str(body.get("name", ""))
                if re.fullmatch(r"[\w-]+\.mp4", n):
                    for ext in (".mp4", ".json"):
                        fp = os.path.join(out_dir, n.replace(".mp4", ext))
                        if os.path.exists(fp):
                            os.remove(fp)
                    with STATE_LOCK:
                        STATE["clips"] = [c for c in STATE["clips"] if c["name"] != n]
                return self._send(200, {"ok": True})
            if path == "/api/vod":
                job, err = start_vod(body.get("url"), body.get("count", 6),
                                     body.get("min_len", 20), body.get("max_len", 60))
                return self._send(400 if err else 200, {"error": err} if err else {"ok": True, "id": job["id"]})
            if path == "/api/vod/cancel":
                for j in STATE["jobs"]:
                    if j["id"] == str(body.get("id")):
                        j["cancel"] = True
                return self._send(200, {"ok": True})
            if path == "/api/caption_style":
                st = clean_style(body)
                save_style(st)
                return self._send(200, st)
            if path == "/api/reveal":
                subprocess.run(["open", out_dir])
                return self._send(200, {"ok": True})
            self._send(404, {"error": "not found"})

        def _video(self, fp):
            if not os.path.isfile(fp):
                return self._send(404, {"error": "not found"})
            size = os.path.getsize(fp)
            start, end = 0, size - 1
            rng = self.headers.get("Range")
            if rng and (m := re.match(r"bytes=(\d*)-(\d*)", rng)):
                if m.group(1):
                    start = int(m.group(1))
                    end = int(m.group(2)) if m.group(2) else size - 1
                else:
                    start = size - int(m.group(2))
            end = min(end, size - 1)
            self.send_response(206 if rng else 200)
            self.send_header("Content-Type", "video/mp4")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(end - start + 1))
            if rng:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            with open(fp, "rb") as f:
                f.seek(start)
                left = end - start + 1
                try:
                    while left > 0:
                        chunk = f.read(min(1 << 16, left))
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        left -= len(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    pass
    return H


def start_dashboard(out_dir, port, open_browser):
    srv = ThreadingHTTPServer(("127.0.0.1", port), make_handler(out_dir))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://localhost:{port}"
    log(f"dashboard -> {url}")
    if open_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()


def load_existing(out_dir):
    for f in sorted(os.listdir(out_dir)):
        if re.fullmatch(r"[\w-]+\.mp4", f) and not f.endswith("_raw.mp4"):
            fp = os.path.join(out_dir, f)
            m = re.match(r"(.+?)_\d{8}_\d{6}(?:_\d+)?\.mp4$", f)
            clip = {"name": f, "status": "ready", "reason": "earlier", "peak": 0,
                    "created": os.path.getmtime(fp), "size": os.path.getsize(fp), "caption": "",
                    "channel": re.sub(r"^(kick|youtube)-", r"\1:", m.group(1)) if m else ""}
            try:
                with open(fp.replace(".mp4", ".json")) as j:
                    clip.update(json.load(j))
                    clip["status"] = "ready"
            except (OSError, ValueError):
                pass
            STATE["clips"].insert(0, clip)


# ---------------------------------------------------------------- zero-setup bootstrap

GOOD_PY = ((3, 10), (3, 14))     # versions the video/AI libraries fully support (3.15 is too new)


def _py_version(exe):
    try:
        r = subprocess.run([exe, "-c", "import sys; print(sys.version_info[0], sys.version_info[1])"],
                           capture_output=True, text=True, timeout=20)
        major, minor = map(int, r.stdout.split())
        return (major, minor)
    except Exception:
        return None


def _good(v):
    return v is not None and GOOD_PY[0] <= v <= GOOD_PY[1]


def pick_python():
    """A stable Python for the private environment; installs one with Homebrew if needed."""
    if _good(sys.version_info[:2]) and not in_venv():
        return sys.executable
    names = ["python3.13", "python3.12", "python3.11", "python3.14"]
    dirs = ["/opt/homebrew/bin", "/usr/local/bin", "/opt/homebrew/opt/python@{v}/bin", "/usr/local/opt/python@{v}/bin"]
    cands = [shutil.which(n) for n in names]
    for n in names:
        v = n.replace("python", "")
        cands += [os.path.join(d.format(v=v), n) for d in dirs]
    for c in cands:
        if c and os.path.exists(c) and _good(_py_version(c)):
            return c
    brew = shutil.which("brew") or next((b for b in ("/opt/homebrew/bin/brew", "/usr/local/bin/brew")
                                         if os.path.exists(b)), None)
    if brew:
        print("  Your Python is very new and some video parts don't support it yet.")
        print("  Installing a stable Python 3.13 with Homebrew (one time, a minute or two)...", flush=True)
        subprocess.run([brew, "install", "python@3.13"])
        for c in ("/opt/homebrew/opt/python@3.13/bin/python3.13", "/usr/local/opt/python@3.13/bin/python3.13"):
            if os.path.exists(c) and _good(_py_version(c)):
                return c
    return None


def bootstrap():
    """Everything installs itself: a private Python environment (.venv) on a stable Python with all
    parts, and ffmpeg via Homebrew if it's missing. First run takes a minute or two; later runs are instant."""
    if os.environ.get("AUTOCLIP_NO_VENV"):
        return
    venv_ok = os.path.exists(VENV_PY) and _good(_py_version(VENV_PY))
    if not venv_ok:
        base = pick_python()
        if not base:
            print("  ! Couldn't find a stable Python (3.10-3.14). Install one: brew install python@3.13", flush=True)
            return
        if os.path.exists(VENV):
            print("  Rebuilding autoclip's environment on a stable Python (one time)...", flush=True)
            shutil.rmtree(VENV, ignore_errors=True)
        else:
            print("\n  First run: setting autoclip up for you (one time, about a minute)...\n", flush=True)
        r = subprocess.run([base, "-m", "venv", VENV], capture_output=True, text=True)
        if r.returncode != 0:
            print(f"  Couldn't create the environment ({_pip_error(r)}).", flush=True)
            return
    if not in_venv() or not venv_ok:
        os.execv(VENV_PY, [VENV_PY, os.path.abspath(__file__), *sys.argv[1:]])
    # inside the private environment: its tools (streamlink, yt-dlp) come first on PATH
    os.environ["PATH"] = os.path.dirname(VENV_PY) + os.pathsep + os.environ.get("PATH", "")
    missing = [pkg for pkg, mod in PIP_PKGS.items() if not has_module(mod)]
    if missing:
        print(f"  Installing parts: {', '.join(missing)} ...", flush=True)
        pip = [sys.executable, "-m", "pip", "install", "-q", "--disable-pip-version-check"]
        r = subprocess.run([*pip, *missing], capture_output=True, text=True)
        if r.returncode != 0:                       # one bad package shouldn't block the rest
            for pkg in missing:
                r1 = subprocess.run([*pip, pkg], capture_output=True, text=True)
                if r1.returncode != 0:
                    print(f"  ! couldn't install {pkg}: {_pip_error(r1)}", flush=True)
        print("  Parts ready.", flush=True)
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        brew = shutil.which("brew") or next((b for b in ("/opt/homebrew/bin/brew", "/usr/local/bin/brew")
                                             if os.path.exists(b)), None)
        if brew:
            print("  Installing ffmpeg with Homebrew (one time, a few minutes)...", flush=True)
            subprocess.run([brew, "install", "ffmpeg"])
        else:
            print("  ! ffmpeg is missing. Install Homebrew (https://brew.sh), then run: brew install ffmpeg", flush=True)


# ---------------------------------------------------------------- main

def main():
    p = argparse.ArgumentParser(description="Auto-clip Twitch streams. Add channels here or in the dashboard.")
    p.add_argument("channels", nargs="*", help="e.g. ohnepixel kaicenat")
    p.add_argument("--out", default="clips")
    p.add_argument("--no-captions", action="store_true")
    p.add_argument("--port", type=int, default=8787)
    p.add_argument("--no-browser", action="store_true")
    a = p.parse_args()
    bootstrap()
    CTX["args"] = a

    os.makedirs(a.out, exist_ok=True)
    load_existing(a.out)
    if not os.environ.get("GROQ_API_KEY") and load_config().get("groq_key"):
        os.environ["GROQ_API_KEY"] = load_config()["groq_key"]
    CTX.update(out=a.out, no_captions=a.no_captions)
    setup = refresh_setup()
    if not setup["groq"]:
        log("No Groq key yet - add it in the dashboard (Settings) to turn on captions + AI titles")
    if not setup["pillow"]:
        log("Caption engine (Pillow) is missing - stop autoclip (Ctrl+C) and start it again to finish setup")
    try:
        with open(os.path.join(a.out, ".caption_style.json")) as f:
            STATE["caption_style"] = clean_style(json.load(f))
    except (OSError, ValueError):
        STATE["caption_style"] = dict(DEFAULT_STYLE)
    STATE["fonts"] = list(FONT_FILES)
    try:
        with open(os.path.join(a.out, ".clip_settings.json")) as f:
            CLIP_CFG.update({k: int(v) for k, v in json.load(f).items() if k in CLIP_CFG})
    except (OSError, ValueError):
        pass
    STATE["clip_cfg"] = dict(CLIP_CFG)
    STATE["presets"] = PRESETS
    STATE.update(settings={"window": WINDOW, "spike_x": SPIKE_X, "min_msgs": MIN_MSGS,
                           "clip_len": CLIP_LEN, "cooldown": COOLDOWN})
    start_dashboard(a.out, a.port, not a.no_browser)
    for name in a.channels:
        _, err = add_channel(name)
        if err:
            log(f"{name}: {err}")
    log("ready - open http://localhost:%d and paste a link (Ctrl+C to stop)" % a.port)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("stopping...")
    finally:
        for ch in list(CHANNELS.values()):
            ch.stop.set()
            if ch.proc and ch.proc.poll() is None:
                ch.proc.terminate()


if __name__ == "__main__":
    main()
