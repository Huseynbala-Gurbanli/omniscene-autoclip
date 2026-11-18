# omniscene-autoclip

Open-source, local **OpusClip-style clipper** for livestreams and long videos.
Watches Twitch / Kick / YouTube chat, cuts vertical Shorts when chat explodes, scores every
clip with AI, and turns long VODs into the best 5–10 clips — with animated captions and a
facecam split layout.

| Step | Tool | What it does |
|---|---|---|
| Record | streamlink | Saves each live stream to disk |
| Detect | chat (Twitch IRC / Kick Pusher / YouTube via yt-dlp) | Counts messages every 10 s, flags spikes (2.5× baseline) |
| Transcribe | Groq Whisper | Word-level timestamps |
| Judge | Groq LLM | **Viral score 0–100**, hook title, caption, hashtags |
| Layout | ffmpeg + OpenCV YuNet | Blur, Fill, or **Split** (streamer facecam on top) |
| Captions | Pillow | Karaoke / pop word-by-word captions, 5 presets |
| Watch | Dashboard | Control room at `http://localhost:8787` |

## Setup (macOS)

```bash
python3 autoclip.py
```

That's it — nothing to install. The first run sets itself up (about a minute): it creates a
private environment in `.venv` with everything inside (captions, facecam detection, streamlink,
yt-dlp, Kick chat) and installs ffmpeg with Homebrew if it's missing. Later runs start instantly.
Then paste your free Groq key (https://console.groq.com/keys) once in the app; it's stored in
`~/.autoclip/config.json` on your Mac only.

## Run

```bash
python3 autoclip.py                                         # then paste links in the app
python3 autoclip.py ohnepixel kick.com/xqc                  # or start watching right away
```

## How to use

1. **Paste a link, press Start.**
   - A live stream (Twitch name, `kick.com/x`, `youtube.com/@x`) → autoclip watches it and clips
     the moments chat goes wild. Press **Clip now** any time.
   - A video (YouTube / Twitch VOD link or a file on your Mac) → AI picks the best 3–12 moments.
2. **Your clips** appear below with a score (🔥 Great / Good / OK), a title, **Download** and
   **Copy caption** (title + caption + hashtags).
3. **Settings** → pick a caption style and layout (Full video / Facecam on top / Zoomed in) with a
   live preview. More options and a setup check are tucked away below.

Clips + a `.json` sidecar (score, title, hashtags, transcript) are saved to `clips/`.

## Options

`--port 8787` · `--no-browser` · `--no-captions` · `--out clips`
Env: `GROQ_LLM_MODEL` (default `openai/gpt-oss-120b`), `GROQ_STT_MODEL` (default `whisper-large-v3-turbo`).
Tuning constants at the top of `autoclip.py`: `SPIKE_X`, `MIN_MSGS`, `CLIP_LEN`, `PRE`, `COOLDOWN`.

## Note

Only post clips of streams/videos you have permission to use (e.g. a streamer's clipping program).
Face model: YuNet from OpenCV Zoo (MIT) — see `models/README.md`.

## License

MIT
