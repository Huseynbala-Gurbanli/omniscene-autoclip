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
brew install streamlink ffmpeg yt-dlp           # yt-dlp: YouTube chat + long videos
pip3 install pillow opencv-python-headless      # captions + facecam split layout
pip3 install websocket-client curl_cffi         # Kick chat (optional)
export GROQ_API_KEY=gsk_...                     # free key: https://console.groq.com/keys
```

## Run

```bash
python3 autoclip.py                                         # add channels in the dashboard
python3 autoclip.py ohnepixel kick.com/xqc youtube.com/@IShowSpeed
```

## Dashboard (http://localhost:8787)

- **Channels** — add Twitch names, Kick links or YouTube channels; one tab each, live chat chart
- **Clip last 30 s** — button or `C`
- **Long video → clips** — paste a YouTube / VOD link or a local file; AI reads the whole
  transcript and cuts the strongest 3–12 moments (progress + cancel)
- **AI on every clip** — viral score badge, hook title, hashtags, *Copy post* button, sort by top score
- **Caption studio** — layout (Blur / Split facecam / Fill), 5 caption presets, font, size,
  position, words on screen, colors, active-word highlight / pop, outline, box, shadow — live preview

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
