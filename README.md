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
3. **Format**: Vertical 9:16, Portrait 4:5, Square 1:1, Classic 4:3 or Horizontal 16:9 —
   on the main screen, per faceless video, or per clip in the editor.
4. **Edit** any clip: drag the start/end, fix caption words, change title/caption/hashtags,
   style, layout, format, and add your logo → it re-makes the clip in place.
5. **✨ Make a faceless video**: type a topic → AI writes the script (editable) → a Groq voice
   reads it (Mac voice as fallback) → your recorded gameplay or any video plays behind it.
6. **Settings** → caption style (Hype, Punchy, Karaoke, Bold, Pop, Box, Minimal — bounce/zoom
   animations, emoji, 11 fonts), layout, logo, auto-clip timing, setup check and **Test AI**.

Clips + a `.json` sidecar (score, title, hashtags, transcript) are saved to `clips/`.

## Options

`--port 8787` · `--no-browser` · `--no-captions` · `--out clips`
Env: `GROQ_LLM_MODEL` (default `openai/gpt-oss-120b`), `GROQ_STT_MODEL` (default `whisper-large-v3-turbo`).
Tuning constants at the top of `autoclip.py`: `SPIKE_X`, `MIN_MSGS`, `CLIP_LEN`, `PRE`, `COOLDOWN`.

## Note

Only post clips of streams/videos you have permission to use (e.g. a streamer's clipping program).
Face model: YuNet from OpenCV Zoo (MIT) — see `models/README.md`. Fonts: OFL / Apache — see `fonts/README.md`.
Groq voices (Orpheus) need a one-time terms acceptance in the Groq Playground.

## License

MIT
