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
  python3 autoclip.py ohnepixel kaicenat   # or start with some
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

class Channel:
    def __init__(self, name):
        self.name = name.lower().strip().lstrip("#").split("/")[-1]
        self.stop = threading.Event()
        self.manual = threading.Event()
        self.counter, self.lock = [0], threading.Lock()
        self.rec = os.path.join(CTX["out"], f"{self.name}_{datetime.now():%Y%m%d_%H%M%S}.ts")
        self.t0 = time.time()
        self.proc = None
        self.view = {"name": self.name, "started": self.t0, "recording": self.rec, "rec_bytes": 0,
                     "status": "starting", "chat_connected": False, "buckets": []}

    def set(self, **kw):
        with STATE_LOCK:
            self.view.update(kw)

    def start(self):
        STATE["channels"][self.name] = self.view
        threading.Thread(target=self._chat, daemon=True).start()
        threading.Thread(target=self._loop, daemon=True).start()

    def remove(self):
        self.stop.set()
        self.manual.set()
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
        with STATE_LOCK:
            STATE["channels"].pop(self.name, None)
        log(f"removed {self.name}")

    def _chat(self):
        while not self.stop.is_set():
            try:
                s = socket.create_connection(("irc.chat.twitch.tv", 6667), timeout=30)
                s.sendall(b"NICK justinfan%d\r\n" % (int(time.time() * 1000) % 1000000))
                s.sendall(f"JOIN #{self.name}\r\n".encode())
                self.set(chat_connected=True)
                log(f"{self.name}: chat connected")
                buf = ""
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
                            with self.lock:
                                self.counter[0] += 1
                s.close()
            except Exception as e:
                if not self.stop.is_set():
                    log(f"{self.name}: chat error ({e}), reconnecting...")
            self.set(chat_connected=False)
            if not self.stop.is_set():
                time.sleep(5)

    def _fire(self, start, reason, peak):
        name = f"{self.name}_{datetime.now():%Y%m%d_%H%M%S}.mp4"
        threading.Thread(target=make_clip, args=(self.rec, start, CTX["out"], name, CTX["captions"],
                                                 reason, peak, self.name), daemon=True).start()

    def _loop(self):
        log(f"recording twitch.tv/{self.name}")
        self.proc = subprocess.Popen(
            ["streamlink", "--twitch-disable-ads", f"https://twitch.tv/{self.name}", "best",
             "-o", self.rec, "-f"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.set(status="live")
        hist, last_clip = deque(maxlen=HISTORY), -COOLDOWN
        while not self.stop.is_set():
            self.manual.wait(WINDOW)
            if self.stop.is_set():
                break
            if self.proc.poll() is not None:
                started_ok = os.path.exists(self.rec) and os.path.getsize(self.rec) > 0
                self.set(status="ended" if started_ok else "offline")
                log(f"{self.name}: " + ("stream ended" if started_ok else "offline or not found"))
                break
            now = time.time() - self.t0
            if self.manual.is_set():
                self.manual.clear()
                if now < 15:
                    log(f"{self.name}: too early - wait until 15 s is recorded")
                else:
                    log(f"✂️  {self.name}: manual clip")
                    self._fire(now - CLIP_LEN, "manual", 0)
                continue
            with self.lock:
                n, self.counter[0] = self.counter[0], 0
            avg = sum(hist) / len(hist) if hist else 0
            spike = len(hist) >= 6 and n >= MIN_MSGS and n > avg * SPIKE_X
            print(f"[{datetime.now():%H:%M:%S}] {self.name:<14} chat {n:4d}/10s  avg {avg:6.1f}"
                  + ("  🔥 SPIKE" if spike else ""), flush=True)
            with STATE_LOCK:
                self.view["buckets"] = (self.view["buckets"] + [
                    {"t": time.time(), "n": n, "avg": round(avg, 1), "spike": spike}])[-360:]
            hist.append(n)
            if spike and now - last_clip > COOLDOWN:
                last_clip = now
                log(f"🔥 {self.name}: spike {n} msgs (avg {avg:.0f})")
                threading.Timer(max(0, CLIP_LEN - PRE), self._fire,
                                args=(now - WINDOW - PRE, "spike", n)).start()


def add_channel(name):
    ch = Channel(name)
    if not re.fullmatch(r"[a-z0-9_]{2,25}", ch.name):
        return None, "invalid channel name"
    if ch.name in CHANNELS and not CHANNELS[ch.name].stop.is_set():
        return None, "already added"
    CHANNELS[ch.name] = ch
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
                return self._send(400 if err else 200, {"error": err} if err else {"ok": True, "name": ch.name})
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
                                      "channel": m.group(1) if m else ""})


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
