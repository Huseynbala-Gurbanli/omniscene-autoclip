"""
ai.py - Groq-powered brains for autoclip.

  transcribe()        Whisper on Groq -> word timestamps (one clip)
  transcribe_long()   same for hours-long videos (10-min chunks, offsets stitched)
  score_clip()        viral score 0-100 + hook title + description + hashtags
  find_moments()      reads a long transcript and picks the best self-contained moments

Only needs curl + ffmpeg + GROQ_API_KEY. Model can be changed with GROQ_LLM_MODEL.
"""
import json, os, re, subprocess, time

LLM_MODEL = os.environ.get("GROQ_LLM_MODEL", "openai/gpt-oss-120b")
STT_MODEL = os.environ.get("GROQ_STT_MODEL", "whisper-large-v3-turbo")
API = "https://api.groq.com/openai/v1"


def has_key():
    return bool(os.environ.get("GROQ_API_KEY"))


# ---------------------------------------------------------------- HTTP (curl, with 429 retry)

def _curl(args, tries=6):
    last = ""
    for attempt in range(tries):
        r = subprocess.run(["curl", "-sS", "-w", "\n%{http_code}", *args,
                            "-H", f"Authorization: Bearer {os.environ['GROQ_API_KEY']}"],
                           capture_output=True, text=True)
        body, _, code = r.stdout.rpartition("\n")
        if code == "200":
            return json.loads(body)
        last = body or r.stderr
        if code in ("429", "500", "502", "503"):
            # Groq says e.g. "Please try again in 7.66s" -> respect it
            m = re.search(r"try again in (?:(\d+)m)?([\d.]+)s", body)
            wait = (int(m.group(1) or 0) * 60 + float(m.group(2))) if m else 4 * (attempt + 1)
            time.sleep(min(wait + 0.5, 90))
            continue
        break
    raise RuntimeError(f"Groq error: {last[:300]}")


def _chat(system, user, max_tokens=1500):
    payload = {"model": LLM_MODEL, "temperature": 0.4, "max_tokens": max_tokens,
               "response_format": {"type": "json_object"},
               "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]}
    out = _curl([f"{API}/chat/completions", "-H", "Content-Type: application/json",
                 "-d", json.dumps(payload)])
    text = out["choices"][0]["message"]["content"] or "{}"
    try:
        return json.loads(text)
    except ValueError:
        m = re.search(r"\{.*\}", text, re.S)
        return json.loads(m.group(0)) if m else {}


# ---------------------------------------------------------------- speech to text

def transcribe(media, start=None, length=None):
    """Word timestamps for a clip (or a slice of a bigger file)."""
    mp3 = f"{media}.{os.getpid()}.{int(time.time() * 1000)}.mp3"
    cut = (["-ss", str(start)] if start is not None else []) + (["-t", str(length)] if length else [])
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", *cut, "-i", media, "-vn",
                    "-ac", "1", "-ar", "16000", "-b:a", "48k", mp3], check=True)
    try:
        out = _curl([f"{API}/audio/transcriptions", "-F", f"file=@{mp3}", "-F", f"model={STT_MODEL}",
                     "-F", "response_format=verbose_json", "-F", "timestamp_granularities[]=word"])
    finally:
        if os.path.exists(mp3):
            os.remove(mp3)
    return [{"word": w["word"], "start": float(w["start"]), "end": float(w["end"])}
            for w in out.get("words", [])]


def transcribe_long(media, workdir, progress=lambda done, total: None, chunk=600):
    """Hours-long video -> one word list. Audio is split into 10-min chunks (small uploads)."""
    os.makedirs(workdir, exist_ok=True)
    pattern = os.path.join(workdir, "a%04d.mp3")
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", media, "-vn", "-ac", "1", "-ar", "16000",
                    "-b:a", "32k", "-f", "segment", "-segment_time", str(chunk), pattern], check=True)
    parts = sorted(f for f in os.listdir(workdir) if re.fullmatch(r"a\d{4}\.mp3", f))
    words = []
    for i, p in enumerate(parts):
        progress(i, len(parts))
        out = _curl([f"{API}/audio/transcriptions", "-F", f"file=@{os.path.join(workdir, p)}",
                     "-F", f"model={STT_MODEL}", "-F", "response_format=verbose_json",
                     "-F", "timestamp_granularities[]=word"])
        off = i * chunk
        words += [{"word": w["word"], "start": float(w["start"]) + off, "end": float(w["end"]) + off}
                  for w in out.get("words", [])]
        os.remove(os.path.join(workdir, p))
    progress(len(parts), len(parts))
    return words


def words_between(words, start, end):
    """Words inside [start, end], re-timed relative to start."""
    return [{"word": w["word"], "start": w["start"] - start, "end": w["end"] - start}
            for w in words if w["start"] >= start - 0.05 and w["end"] <= end + 0.05]


# ---------------------------------------------------------------- scoring one clip

SCORE_SYSTEM = """You are a short-form video editor who has studied thousands of viral TikToks,
YouTube Shorts and Reels from gaming livestreams (GTA, Twitch/Kick streamers).
Judge a 20-60 second clip from its transcript and context. Be honest: most clips are 30-60.
Score 80+ only for a strong hook, a clear payoff, and big emotion (shock, laughter, rage, hype).
Reply as JSON only."""


def score_clip(transcript, context=""):
    """-> {score, title, hook, description, hashtags[], reason}"""
    user = f"""Context: {context or 'gaming livestream clip'}
Transcript (may be messy, auto-generated):
\"\"\"{transcript[:3000] or '(no speech - judge from context only)'}\"\"\"

Return JSON:
{{"score": 0-100 integer (virality potential),
  "title": "punchy title under 60 chars, no clickbait lies, max 1 emoji",
  "hook": "on-screen hook text for the first 2 seconds, under 6 words",
  "description": "1-2 sentence post caption",
  "hashtags": ["5-8 relevant hashtags without #"],
  "reason": "one short sentence why this score"}}"""
    j = _chat(SCORE_SYSTEM, user, max_tokens=700)
    return _clean_meta(j)


def _clean_meta(j):
    try:
        score = int(float(j.get("score", 0)))
    except (TypeError, ValueError):
        score = 0
    tags = j.get("hashtags") or []
    if isinstance(tags, str):
        tags = re.split(r"[\s,]+", tags)
    tags = [re.sub(r"[^\w]", "", t) for t in tags if t][:8]
    return {"score": max(0, min(100, score)), "title": str(j.get("title", ""))[:90],
            "hook": str(j.get("hook", ""))[:60], "description": str(j.get("description", ""))[:400],
            "hashtags": [t for t in tags if t], "why": str(j.get("reason", ""))[:200]}


# ---------------------------------------------------------------- picking moments in long videos

MOMENTS_SYSTEM = """You find the best moments in long livestreams/videos to cut into vertical shorts.
A great moment is self-contained (makes sense without context), starts with a hook in the first
2 seconds, and ends right after the payoff (punchline, reaction, reveal, fail, clutch play).
Avoid moments that start mid-sentence or need earlier context. Reply as JSON only."""


def _lines(words, size=14):
    out = []
    for i in range(0, len(words), size):
        ch = words[i:i + size]
        out.append(f"[{ch[0]['start']:.1f}] " + " ".join(w["word"].strip() for w in ch))
    return "\n".join(out)


def find_moments(words, count=6, min_len=20, max_len=60, context="",
                 progress=lambda done, total: None, window=480):
    """Scan the transcript in ~8-min windows, collect candidates, return the best `count`
    non-overlapping moments as [{start, end, score, title, hook, description, hashtags, reason}]."""
    if not words:
        return []
    total_end = words[-1]["end"]
    windows, t = [], 0.0
    while t < total_end:
        windows.append((t, t + window))
        t += window - 30                     # small overlap so moments on edges aren't lost
    cands = []
    for i, (w0, w1) in enumerate(windows):
        progress(i, len(windows))
        ws = [w for w in words if w0 <= w["start"] < w1]
        if len(ws) < 25:
            continue
        user = f"""Context: {context or 'livestream / long video'}
Transcript with timestamps in seconds (each line starts at [time]):
{_lines(ws)}

Pick up to 3 of the best moments in this part. Each must be {min_len}-{max_len} seconds long.
Return JSON: {{"moments": [{{"start": seconds, "end": seconds, "score": 0-100,
  "title": "punchy title under 60 chars", "hook": "on-screen hook under 6 words",
  "description": "1-2 sentence post caption", "hashtags": ["5-8 tags without #"],
  "reason": "why it works"}}]}}
Return {{"moments": []}} if nothing here is genuinely good."""
        try:
            j = _chat(MOMENTS_SYSTEM, user, max_tokens=1800)
        except Exception:
            continue
        for m in j.get("moments", []) or []:
            try:
                s, e = float(m["start"]), float(m["end"])
            except (KeyError, TypeError, ValueError):
                continue
            if e - s < min_len * 0.75:
                e = s + min_len
            e = min(e, s + max_len)
            s, e = _snap(words, s, e)
            meta = _clean_meta(m)
            cands.append({"start": s, "end": e, **meta})
    progress(len(windows), len(windows))
    cands.sort(key=lambda c: -c["score"])
    picked = []
    for c in cands:
        if all(c["end"] <= p["start"] or c["start"] >= p["end"] for p in picked):
            picked.append(c)
        if len(picked) >= count:
            break
    return sorted(picked, key=lambda c: c["start"])


def _snap(words, s, e):
    """Move cut points to word boundaries so clips don't start/end mid-word."""
    starts = [w["start"] for w in words if abs(w["start"] - s) < 3]
    ends = [w["end"] for w in words if abs(w["end"] - e) < 3]
    s2 = min(starts, key=lambda x: abs(x - s)) if starts else s
    e2 = min(ends, key=lambda x: abs(x - e)) if ends else e
    return max(0.0, s2 - 0.25), e2 + 0.4


# ---------------------------------------------------------------- faceless videos: script + voice

TTS_MODEL = os.environ.get("GROQ_TTS_MODEL", "canopylabs/orpheus-v1-english")
VOICES = ["troy", "austin", "daniel", "autumn", "diana", "hannah"]

SCRIPT_SYSTEM = """You write scripts for faceless short-form videos (TikTok, YouTube Shorts, Reels)
that get watched to the end. Hook in the first sentence, short punchy sentences, one clear story or
list, a satisfying last line. Spoken words only: no stage directions, no emojis, no hashtags in the
script. Reply as JSON only."""


def write_script(topic, seconds=45):
    words = int(seconds * 2.6)
    user = f"""Topic: {topic}
Write a voiceover script of about {words} words (~{seconds} seconds spoken).
Return JSON: {{"title": "punchy title under 60 chars", "script": "the voiceover text",
  "description": "1-2 sentence post caption", "hashtags": ["5-8 tags without #"]}}"""
    j = _chat(SCRIPT_SYSTEM, user, max_tokens=1200)
    meta = _clean_meta({**j, "score": 0})
    return {"title": meta["title"], "script": str(j.get("script", "")).strip(),
            "description": meta["description"], "hashtags": meta["hashtags"]}


def _tts_chunks(text, limit=380):
    parts, cur = [], ""
    for s in re.split(r"(?<=[.!?])\s+", text.strip()):
        while len(s) > limit:                               # very long sentence: split on commas/spaces
            cut = s.rfind(",", 0, limit)
            cut = cut if cut > 50 else s.rfind(" ", 0, limit)
            parts.append(s[:cut + 1].strip())
            s = s[cut + 1:].strip()
        if len(cur) + len(s) + 1 > limit and cur:
            parts.append(cur)
            cur = s
        else:
            cur = (cur + " " + s).strip()
    if cur:
        parts.append(cur)
    return [p for p in parts if p]


def speak(text, out_wav, voice="troy", workdir=None):
    """Voiceover -> WAV. Groq Orpheus first; macOS `say` as a fallback. Returns which engine was used."""
    import shutil, tempfile
    work = workdir or tempfile.mkdtemp(prefix="tts_")
    os.makedirs(work, exist_ok=True)
    pieces, err = [], ""
    if has_key() and voice in VOICES:
        try:
            for i, chunk in enumerate(_tts_chunks(text)):
                p = os.path.join(work, f"t{i:03d}.wav")
                payload = {"model": TTS_MODEL, "voice": voice, "input": chunk, "response_format": "wav"}
                r = subprocess.run(["curl", "-sS", "-w", "\n%{http_code}", "-o", p, f"{API}/audio/speech",
                                    "-H", f"Authorization: Bearer {os.environ['GROQ_API_KEY']}",
                                    "-H", "Content-Type: application/json", "-d", json.dumps(payload)],
                                   capture_output=True, text=True)
                code = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else ""
                if code != "200":
                    with open(p, errors="ignore") as f:
                        err = f.read()[:300]
                    raise RuntimeError(f"Groq voice HTTP {code}: {err}")
                pieces.append(p)
        except Exception as e:
            err = str(e)
            pieces = []
    engine = "groq"
    if not pieces:
        if not shutil.which("say"):
            raise RuntimeError(err or "No voice available (needs Groq voice access or a Mac)")
        engine = "mac"
        aiff = os.path.join(work, "say.aiff")
        subprocess.run(["say", "-o", aiff, text], check=True)
        pieces = [aiff]
    lst = os.path.join(work, "list.txt")
    with open(lst, "w") as f:
        f.write("".join(f"file '{os.path.abspath(p)}'\n" for p in pieces))
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", lst,
                    "-ac", "1", "-ar", "44100", out_wav], check=True)
    if not workdir:
        shutil.rmtree(work, ignore_errors=True)
    return engine, err
