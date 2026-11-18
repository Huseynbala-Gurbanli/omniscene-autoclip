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
import argparse, json, os, re, shutil, socket, subprocess, threading, time, webbrowser
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
STATE = {"captions": False, "settings": {}, "channels": {}, "clips": [], "log": []}
STATE_LOCK = threading.Lock()
CHANNELS = {}      # name -> Channel
CTX = {}           # out dir, captions flag


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
                     "chat_connected": False, "chat_note": "", "buckets": []}

    @property
    def name(self):
        return self.key

    def set(self, **kw):
        with STATE_LOCK:
            self.view.update(kw)

    def start(self):
        STATE["channels"][self.key] = self.view
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

    def _fire(self, start, reason, peak):
        name = f"{self.file_id}_{datetime.now():%Y%m%d_%H%M%S}.mp4"
        threading.Thread(target=make_clip, args=(self.rec, start, CTX["out"], name, CTX["captions"],
                                                 reason, peak, self.key), daemon=True).start()

    def _loop(self):
        log(f"recording {self.url}")
        extra = ["--twitch-disable-ads"] if self.platform == "twitch" else []
        self.proc = subprocess.Popen(
            ["streamlink", *extra, self.url, "best", "-o", self.rec, "-f"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.set(status="live")
        hist, last_clip = deque(maxlen=HISTORY), -COOLDOWN
        while not self.stop.is_set():
            self.manual.wait(WINDOW)
            if self.stop.is_set():
                break
            if self.proc.poll() is not None:
                started_ok = os.path.exists(self.rec) and os.path.getsize(self.rec) > 0
                self.set(status="ended" if started_ok else "offline")
                log(f"{self.key}: " + ("stream ended" if started_ok else "offline or not found"))
                break
            now = time.time() - self.t0
            if self.manual.is_set():
                self.manual.clear()
                if now < 15:
                    log(f"{self.key}: too early - wait until 15 s is recorded")
                else:
                    log(f"✂️  {self.key}: manual clip")
                    self._fire(now - CLIP_LEN, "manual", 0)
                continue
            with self.lock:
                n, self.counter[0] = self.counter[0], 0
            avg = sum(hist) / len(hist) if hist else 0
            spike = len(hist) >= 6 and n >= MIN_MSGS and n > avg * SPIKE_X
            print(f"[{datetime.now():%H:%M:%S}] {self.key:<18} chat {n:4d}/10s  avg {avg:6.1f}"
                  + ("  🔥 SPIKE" if spike else ""), flush=True)
            with STATE_LOCK:
                self.view["buckets"] = (self.view["buckets"] + [
                    {"t": time.time(), "n": n, "avg": round(avg, 1), "spike": spike}])[-360:]
            hist.append(n)
            if spike and now - last_clip > COOLDOWN:
                last_clip = now
                log(f"🔥 {self.key}: spike {n} msgs (avg {avg:.0f})")
                threading.Timer(max(0, CLIP_LEN - PRE), self._fire,
                                args=(now - WINDOW - PRE, "spike", n)).start()


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


def make_clip(src, start, out_dir, name, captions, reason="spike", peak=0, channel=""):
    raw = os.path.join(out_dir, name.replace(".mp4", "_raw.mp4"))
    final = os.path.join(out_dir, name)
    set_clip(name, status="cutting", reason=reason, peak=peak, created=time.time(), caption="",
             channel=channel)
    try:
        start = max(0, start)
        # 1) cut
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-ss", str(start), "-i", src,
                        "-t", str(CLIP_LEN), "-c", "copy", "-avoid_negative_ts", "make_zero", raw],
                       check=True)
        # 2) captions
        sub_filter, words = "", []
        capdir = raw.replace(".mp4", "_caps")
        if captions:
            set_clip(name, status="captioning")
            try:
                words = groq_words(raw)
            except Exception as e:
                log(f"captions failed ({e}) -> clip without captions")
        # 3) 9:16 vertical: blurred background + full gameplay in the middle
        set_clip(name, status="rendering")
        vf = ("[0:v]setpts=PTS-STARTPTS,split[a][b];"
              "[a]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,boxblur=20:5[bg];"
              "[b]scale=1080:-2[fg];"
              "[bg][fg]overlay=(W-w)/2:(H-h)/2")
        inputs = ["-i", raw]
        if words:
            # captions = transparent PNGs played as a 2nd video track -> needs no libass/drawtext
            playlist = render_captions(words, capdir)
            inputs += ["-f", "concat", "-safe", "0", "-i", playlist]
            vf += "[base];[base][1:v]overlay=0:0:eof_action=repeat"
        vf += "[v]"
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", *inputs, "-filter_complex", vf,
                        "-map", "[v]", "-map", "0:a?", "-t", str(CLIP_LEN), "-c:v", "libx264",
                        "-preset", "veryfast", "-crf", "20", "-pix_fmt", "yuv420p",
                        "-c:a", "aac", "-b:a", "160k", "-movflags", "+faststart", final],
                       check=True)
        set_clip(name, status="ready", size=os.path.getsize(final),
                 caption=" ".join(w["word"].strip() for w in words)[:140])
        log(f"✅ saved {final}")
    except Exception as e:
        set_clip(name, status="failed")
        log(f"clip failed: {e}")
    finally:
        if os.path.exists(raw):
            os.remove(raw)
        shutil.rmtree(capdir, ignore_errors=True)


def groq_words(video):
    """Send the clip's audio to Groq Whisper, get word timestamps back."""
    mp3 = video.replace(".mp4", ".mp3")
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", video, "-vn",
                    "-ac", "1", "-ar", "16000", "-b:a", "48k", mp3], check=True)
    r = subprocess.run(
        ["curl", "-sS", "--fail-with-body", "https://api.groq.com/openai/v1/audio/transcriptions",
         "-H", f"Authorization: Bearer {os.environ['GROQ_API_KEY']}",
         "-F", f"file=@{mp3}", "-F", "model=whisper-large-v3-turbo",
         "-F", "response_format=verbose_json", "-F", "timestamp_granularities[]=word"],
        capture_output=True, text=True)
    os.remove(mp3)
    if r.returncode != 0:
        raise RuntimeError(r.stdout or r.stderr)
    return json.loads(r.stdout).get("words", [])


FONTS = ["/System/Library/Fonts/Supplemental/Arial Black.ttf",
         "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
         "/Library/Fonts/Arial Bold.ttf",
         "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"]


def render_captions(words, outdir, per=3, size=96):
    """Shorts-style captions: 3 words at a time, uppercase, white with thick black outline,
    just below the gameplay. Each chunk -> transparent 1080x1920 PNG; returns concat playlist."""
    from PIL import Image, ImageDraw, ImageFont
    os.makedirs(outdir, exist_ok=True)
    font = next((ImageFont.truetype(f, size) for f in FONTS if os.path.exists(f)), None) \
        or ImageFont.load_default()
    blank = os.path.abspath(os.path.join(outdir, "blank.png"))
    Image.new("RGBA", (1080, 1920), (0, 0, 0, 0)).save(blank)
    lines, t = [], 0.0
    for i in range(0, len(words), per):
        chunk = words[i:i + per]
        start, end = float(chunk[0]["start"]), float(chunk[-1]["end"])
        if start > t:
            lines += [f"file '{blank}'", f"duration {start - t:.3f}"]
        img = Image.new("RGBA", (1080, 1920), (0, 0, 0, 0))
        d = ImageDraw.Draw(img)
        text = " ".join(w["word"].strip() for w in chunk).upper()
        f = font
        while d.textlength(text, font=f) > 1000 and getattr(f, "size", 0) > 40:   # shrink long chunks
            f = f.font_variant(size=f.size - 6)
        d.text((540, 1440), text, font=f, anchor="mm", fill="white",
               stroke_width=9, stroke_fill="black")
        png = os.path.join(outdir, f"c{i:04d}.png")
        img.save(png)
        lines += [f"file '{os.path.abspath(png)}'", f"duration {max(0.05, end - max(start, t)):.3f}"]
        t = max(end, t)
    lines += [f"file '{blank}'", "duration 0.1", f"file '{blank}'"]
    playlist = os.path.join(outdir, "caps.txt")
    with open(playlist, "w") as f:
        f.write("\n".join(lines) + "\n")
    return playlist


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
                with STATE_LOCK:
                    for v in STATE["channels"].values():
                        if os.path.exists(v["recording"]):
                            v["rec_bytes"] = os.path.getsize(v["recording"])
                    body = json.dumps({**STATE, "now": time.time()}).encode()
                return self._send(200, body)
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
                    return self._send(400, {"error": "channel not running"})
                ch.manual.set()
                return self._send(200, {"ok": True})
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
            m = re.match(r"(.+?)_\d{8}_\d{6}\.mp4$", f)
            STATE["clips"].insert(0, {"name": f, "status": "ready", "reason": "earlier",
                                      "peak": 0, "created": os.path.getmtime(fp),
                                      "size": os.path.getsize(fp), "caption": "",
                                      "channel": re.sub(r"^(kick|youtube)-", r"\1:", m.group(1)) if m else ""})


# ---------------------------------------------------------------- main

def main():
    p = argparse.ArgumentParser(description="Auto-clip Twitch streams. Add channels here or in the dashboard.")
    p.add_argument("channels", nargs="*", help="e.g. ohnepixel kaicenat")
    p.add_argument("--out", default="clips")
    p.add_argument("--no-captions", action="store_true")
    p.add_argument("--port", type=int, default=8787)
    p.add_argument("--no-browser", action="store_true")
    a = p.parse_args()

    os.makedirs(a.out, exist_ok=True)
    load_existing(a.out)
    captions = not a.no_captions and bool(os.environ.get("GROQ_API_KEY"))
    if not a.no_captions and not captions:
        log("GROQ_API_KEY not set -> clips without captions")
    try:
        import PIL  # noqa: F401
    except ImportError:
        if captions:
            log("Pillow missing -> run: pip3 install pillow  (captions off)")
        captions = False
    CTX.update(out=a.out, captions=captions)
    STATE.update(captions=captions,
                 settings={"window": WINDOW, "spike_x": SPIKE_X, "min_msgs": MIN_MSGS,
                           "clip_len": CLIP_LEN, "cooldown": COOLDOWN})
    start_dashboard(a.out, a.port, not a.no_browser)
    for name in a.channels:
        _, err = add_channel(name)
        if err:
            log(f"{name}: {err}")
    log("running - add channels in the dashboard (Ctrl+C to stop)")
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
