# omniscene-autoclip

Auto-clips Twitch livestreams into vertical Shorts/Reels when chat spikes.

| Step | Tool | What it does |
|---|---|---|
| Record | streamlink | Saves the live stream to disk |
| Detect | Twitch chat (anonymous) | Counts messages every 10 s, flags spikes (2.5× average) |
| Cut | ffmpeg | 30 s around each spike, 9:16 with blurred background |
| Caption | Groq Whisper API | Shorts-style captions, 3 words at a time |
| Watch | Dashboard | Live control room at `http://localhost:8787` |

## Setup (macOS)

```bash
brew install streamlink ffmpeg
pip3 install pillow
export GROQ_API_KEY=gsk_...   # free key: https://console.groq.com/keys
```

## Run

```bash
python3 autoclip.py <channel>          # e.g. ohnepixel
python3 autoclip.py <channel> --no-captions
```

The dashboard opens automatically at **http://localhost:8787**:

- live chat-velocity chart with baseline, spike threshold and spike markers
- current rate, baseline, peak, recorded size, chat status
- **Clip last 30 s** button (or press `C`) for manual clips
- clip gallery: play, download, see captions and processing status

Clips are saved to `clips/`. Stop with `Ctrl+C`.

Options: `--port 8787`, `--no-browser`, `--no-captions`, `--out clips`.

## Tuning

Edit the constants at the top of `autoclip.py`: `SPIKE_X`, `MIN_MSGS`, `CLIP_LEN`, `PRE`, `COOLDOWN`.

## Note

Only post clips of streams you have permission to use (e.g. a streamer's clipping program).

## License

MIT
