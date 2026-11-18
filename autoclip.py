#!/usr/bin/env python3
"""
autoclip.py - auto-clip Twitch streams when chat spikes.

  streamlink  -> records the live stream to a .ts file
  twitch chat -> read anonymously (no account, no token)
  spike check -> counts chat messages every 10 s
  ffmpeg      -> cuts 30 s around each spike, makes it 9:16 vertical
  groq        -> Whisper in the cloud writes captions (3 words at a time)

Setup (Mac):
  brew install streamlink ffmpeg
  export GROQ_API_KEY=gsk_...      # free key: console.groq.com/keys

Run:
  python3 autoclip.py ohnepixel
"""
import argparse, json, os, socket, subprocess, sys, threading, time
from collections import deque
from datetime import datetime

WINDOW = 10          # seconds per chat bucket
HISTORY = 30         # buckets used for the baseline (5 min)
SPIKE_X = 2.5        # spike = this many times the average
MIN_MSGS = 15        # ...and at least this many messages in the bucket
CLIP_LEN = 30        # seconds per clip
PRE = 20             # seconds before the spike to include
COOLDOWN = 60        # min seconds between clips


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


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
            log(f"chat error: {e}, reconnecting...")
            time.sleep(5)


def make_clip(src, start, out_dir, idx, captions):
    raw = os.path.join(out_dir, f"clip_{idx:03d}_raw.mp4")
    final = os.path.join(out_dir, f"clip_{idx:03d}.mp4")
    start = max(0, start)
    # 1) cut
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-ss", str(start), "-i", src,
                    "-t", str(CLIP_LEN), "-c", "copy", "-avoid_negative_ts", "make_zero", raw], check=True)
    # 2) captions
    sub_filter = ""
    if captions:
        srt = raw.replace(".mp4", ".srt")
        try:
            words = groq_words(raw)
        except Exception as e:
            log(f"captions failed ({e}) -> clip without captions")
            words = []
        write_srt(words, srt)
        if words:
            esc = srt.replace("\\", "/").replace(":", "\\:").replace("'", "\\'")
            sub_filter = (f",subtitles='{esc}':force_style='Fontname=Arial,Fontsize=22,Bold=1,"
                          "PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,Outline=4,"
                          "Alignment=2,MarginV=70'")
    # 3) 9:16 vertical: blurred background + full gameplay in the middle
    vf = ("[0:v]setpts=PTS-STARTPTS,split[a][b];"
          "[a]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,boxblur=20:5[bg];"
          "[b]scale=1080:-2[fg];"
          f"[bg][fg]overlay=(W-w)/2:(H-h)/2{sub_filter}[v]")
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", raw, "-filter_complex", vf,
                    "-map", "[v]", "-map", "0:a?", "-c:v", "libx264", "-preset", "veryfast",
                    "-crf", "20", "-c:a", "aac", "-b:a", "160k", final], check=True)
    os.remove(raw)
    if os.path.exists(raw.replace(".mp4", ".srt")):
        os.remove(raw.replace(".mp4", ".srt"))
    log(f"✅ saved {final}")


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


def main():
    p = argparse.ArgumentParser()
    p.add_argument("channel")
    p.add_argument("--out", default="clips")
    p.add_argument("--no-captions", action="store_true")
    a = p.parse_args()

    os.makedirs(a.out, exist_ok=True)
    rec = os.path.join(a.out, f"{a.channel}_{datetime.now():%Y%m%d_%H%M}.ts")
    proc = start_recording(a.channel, rec)
    t0 = time.time()

    captions = not a.no_captions and bool(os.environ.get("GROQ_API_KEY"))
    if not a.no_captions and not captions:
        log("GROQ_API_KEY not set -> clips without captions")

    counter, lock, stop = [0], threading.Lock(), threading.Event()
    threading.Thread(target=chat_reader, args=(a.channel, counter, lock, stop), daemon=True).start()

    hist, last_clip, idx = deque(maxlen=HISTORY), -COOLDOWN, 1
    log("watching chat for spikes... (Ctrl+C to stop)")
    try:
        while proc.poll() is None:
            time.sleep(WINDOW)
            with lock:
                n, counter[0] = counter[0], 0
            avg = sum(hist) / len(hist) if hist else 0
            now = time.time() - t0
            spike = len(hist) >= 6 and n >= MIN_MSGS and n > avg * SPIKE_X
            log(f"chat {n:4d}/10s  avg {avg:6.1f}" + ("  🔥 SPIKE" if spike else ""))
            hist.append(n)
            if spike and now - last_clip > COOLDOWN:
                last_clip = now
                start = now - WINDOW - PRE
                time.sleep(max(0, CLIP_LEN - PRE))  # let the moment finish recording
                threading.Thread(target=make_clip,
                                 args=(rec, start, a.out, idx, captions),
                                 daemon=True).start()
                idx += 1
        log("stream ended")
    except KeyboardInterrupt:
        log("stopping...")
    finally:
        stop.set()
        proc.terminate()


if __name__ == "__main__":
    main()
