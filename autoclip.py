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
  export GROQ_API_KEY=gsk_...      # free key: console.groq.com/keys

Run:
  python3 autoclip.py ohnepixel
"""
import argparse, json, os, re, socket, subprocess, threading, time, webbrowser
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
STATE = {"channel": "", "started": 0, "recording": "", "rec_bytes": 0, "live": False,
         "chat_connected": False, "captions": False, "settings": {},
         "buckets": [], "clips": [], "log": []}
STATE_LOCK = threading.Lock()
MANUAL = threading.Event()


def log(msg):
    line = f"[{datetime.now():%H:%M:%S}] {msg}"
    print(line, flush=True)
    with STATE_LOCK:
        STATE["log"] = (STATE["log"] + [line])[-60:]


# ---------------------------------------------------------------- recording + chat

def start_recording(channel, out):
    log(f"recording twitch.tv/{channel} -> {out}")
    return subprocess.Popen(
        ["streamlink", "--twitch-disable-ads", f"https://twitch.tv/{channel}", "best", "-o", out, "-f"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def chat_reader(channel, counter, lock, stop):
    while not stop.is_set():
        try:
            s = socket.create_connection(("irc.chat.twitch.tv", 6667), timeout=30)
            s.sendall(b"NICK justinfan%d\r\n" % (int(time.time()) % 100000))
            s.sendall(f"JOIN #{channel.lower()}\r\n".encode())
            log("chat connected")
            STATE["chat_connected"] = True
            buf = ""
            while not stop.is_set():
                data = s.recv(4096).decode("utf-8", "ignore")
                if not data:
                    break
                buf += data
                *lines, buf = buf.split("\r\n")
                for line in lines:
                    if line.startswith("PING"):
                        s.sendall(b"PONG :tmi.twitch.tv\r\n")
                    elif "PRIVMSG" in line:
                        with lock:
                            counter[0] += 1
        except Exception as e:
            STATE["chat_connected"] = False
            log(f"chat error: {e}, reconnecting...")
            time.sleep(5)


# ---------------------------------------------------------------- clipping

def set_clip(name, **kw):
    with STATE_LOCK:
        for c in STATE["clips"]:
            if c["name"] == name:
                c.update(kw)
                return
        STATE["clips"].insert(0, {"name": name, **kw})


def make_clip(src, start, out_dir, name, captions, reason="spike", peak=0):
    raw = os.path.join(out_dir, name.replace(".mp4", "_raw.mp4"))
    final = os.path.join(out_dir, name)
    set_clip(name, status="cutting", reason=reason, peak=peak, created=time.time(), caption="")
    try:
        start = max(0, start)
        # 1) cut
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-ss", str(start), "-i", src,
                        "-t", str(CLIP_LEN), "-c", "copy", "-avoid_negative_ts", "make_zero", raw],
                       check=True)
        # 2) captions
        sub_filter, words = "", []
        srt = raw.replace(".mp4", ".srt")
        if captions:
            set_clip(name, status="captioning")
            try:
                words = groq_words(raw)
            except Exception as e:
                log(f"captions failed ({e}) -> clip without captions")
            write_srt(words, srt)
            if words:
                esc = srt.replace("\\", "/").replace(":", "\\:").replace("'", "\\'")
                sub_filter = (f",subtitles='{esc}':force_style='Fontname=Arial,Fontsize=22,Bold=1,"
                              "PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,Outline=4,"
                              "Alignment=2,MarginV=70'")
        # 3) 9:16 vertical: blurred background + full gameplay in the middle
        set_clip(name, status="rendering")
        vf = ("[0:v]setpts=PTS-STARTPTS,split[a][b];"
              "[a]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,boxblur=20:5[bg];"
              "[b]scale=1080:-2[fg];"
              f"[bg][fg]overlay=(W-w)/2:(H-h)/2{sub_filter}[v]")
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", raw, "-filter_complex", vf,
                        "-map", "[v]", "-map", "0:a?", "-c:v", "libx264", "-preset", "veryfast",
                        "-crf", "20", "-c:a", "aac", "-b:a", "160k", "-movflags", "+faststart", final],
                       check=True)
        set_clip(name, status="ready", size=os.path.getsize(final),
                 caption=" ".join(w["word"].strip() for w in words)[:140])
        log(f"✅ saved {final}")
    except Exception as e:
        set_clip(name, status="failed")
        log(f"clip failed: {e}")
    finally:
        for f in (raw, srt if captions else ""):
            if f and os.path.exists(f):
                os.remove(f)


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


def write_srt(words, srt, per=3):
    """Shorts-style captions: 3 words at a time, uppercase."""
    def ts(t):
        h, m, s = int(t // 3600), int(t % 3600 // 60), t % 60
        return f"{h:02d}:{m:02d}:{s:06.3f}".replace(".", ",")
    with open(srt, "w") as f:
        for i in range(0, len(words), per):
            chunk = words[i:i + per]
            text = " ".join(w["word"].strip() for w in chunk).upper()
            f.write(f"{i // per + 1}\n{ts(chunk[0]['start'])} --> {ts(chunk[-1]['end'])}\n{text}\n\n")


# ---------------------------------------------------------------- dashboard server

def make_handler(out_dir):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?")[0]
            if path == "/":
                with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                    return self._send(200, f.read(), "text/html; charset=utf-8")
            if path == "/api/state":
                with STATE_LOCK:
                    if STATE["recording"] and os.path.exists(STATE["recording"]):
                        STATE["rec_bytes"] = os.path.getsize(STATE["recording"])
                    body = json.dumps({**STATE, "now": time.time()}).encode()
                return self._send(200, body, "application/json")
            m = re.fullmatch(r"/clips/([\w.-]+\.mp4)", path)
            if m:
                return self._video(os.path.join(out_dir, m.group(1)))
            self._send(404, b"not found", "text/plain")

        def do_POST(self):
            if self.path == "/api/clip":
                MANUAL.set()
                return self._send(200, b'{"ok":true}', "application/json")
            if self.path == "/api/reveal":
                subprocess.run(["open", out_dir])
                return self._send(200, b'{"ok":true}', "application/json")
            self._send(404, b"not found", "text/plain")

        def _video(self, fp):
            if not os.path.isfile(fp):
                return self._send(404, b"not found", "text/plain")
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
            STATE["clips"].insert(0, {"name": f, "status": "ready", "reason": "earlier",
                                      "peak": 0, "created": os.path.getmtime(fp),
                                      "size": os.path.getsize(fp), "caption": ""})


# ---------------------------------------------------------------- main loop

def main():
    p = argparse.ArgumentParser()
    p.add_argument("channel")
    p.add_argument("--out", default="clips")
    p.add_argument("--no-captions", action="store_true")
    p.add_argument("--port", type=int, default=8787)
    p.add_argument("--no-browser", action="store_true")
    a = p.parse_args()

    os.makedirs(a.out, exist_ok=True)
    load_existing(a.out)
    rec = os.path.join(a.out, f"{a.channel}_{datetime.now():%Y%m%d_%H%M}.ts")
    captions = not a.no_captions and bool(os.environ.get("GROQ_API_KEY"))
    STATE.update(channel=a.channel, started=time.time(), recording=rec, live=True,
                 captions=captions,
                 settings={"window": WINDOW, "spike_x": SPIKE_X, "min_msgs": MIN_MSGS,
                           "clip_len": CLIP_LEN, "cooldown": COOLDOWN})
    start_dashboard(a.out, a.port, not a.no_browser)

    proc = start_recording(a.channel, rec)
    t0 = time.time()
    if not a.no_captions and not captions:
        log("GROQ_API_KEY not set -> clips without captions")

    counter, lock, stop = [0], threading.Lock(), threading.Event()
    threading.Thread(target=chat_reader, args=(a.channel, counter, lock, stop), daemon=True).start()

    hist, last_clip = deque(maxlen=HISTORY), -COOLDOWN

    def fire(start, reason, peak):
        name = f"{a.channel}_{datetime.now():%Y%m%d_%H%M%S}.mp4"
        threading.Thread(target=make_clip, args=(rec, start, a.out, name, captions, reason, peak),
                         daemon=True).start()

    log("watching chat for spikes... (Ctrl+C to stop)")
    try:
        while proc.poll() is None:
            MANUAL.wait(WINDOW)
            if MANUAL.is_set():          # "Clip now" pressed on the dashboard
                MANUAL.clear()
                now = time.time() - t0
                log("✂️  manual clip")
                fire(now - CLIP_LEN, "manual", 0)
                continue
            with lock:
                n, counter[0] = counter[0], 0
            avg = sum(hist) / len(hist) if hist else 0
            now = time.time() - t0
            spike = len(hist) >= 6 and n >= MIN_MSGS and n > avg * SPIKE_X
            print(f"[{datetime.now():%H:%M:%S}] chat {n:4d}/10s  avg {avg:6.1f}"
                  + ("  🔥 SPIKE" if spike else ""), flush=True)
            with STATE_LOCK:
                STATE["buckets"] = (STATE["buckets"] + [
                    {"t": time.time(), "n": n, "avg": round(avg, 1), "spike": spike}])[-360:]
            hist.append(n)
            if spike and now - last_clip > COOLDOWN:
                last_clip = now
                log(f"🔥 spike: {n} msgs (avg {avg:.0f})")
                start = now - WINDOW - PRE
                # let the moment finish recording, without blocking the chat loop
                threading.Timer(max(0, CLIP_LEN - PRE), fire, args=(start, "spike", n)).start()
        log("stream ended")
    except KeyboardInterrupt:
        log("stopping...")
    finally:
        STATE["live"] = False
        stop.set()
        proc.terminate()


if __name__ == "__main__":
    main()
