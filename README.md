# omniscene-autoclip

Auto-clips Twitch livestreams into vertical Shorts/Reels when chat spikes.

| Step | Tool | What it does |
|---|---|---|
| Record | streamlink | Saves the live stream to disk |
| Detect | Twitch chat (anonymous) | Counts messages every 10 s, flags spikes (2.5× average) |
| Cut | ffmpeg | 30 s around each spike, 9:16 with blurred background |
| Caption | Groq Whisper API | Shorts-style captions, 3 words at a time |

## Setup (macOS)

```bash
brew install streamlink ffmpeg
export GROQ_API_KEY=gsk_...   # free key: https://console.groq.com/keys
```

## Run

```bash
python3 autoclip.py <channel>          # e.g. ohnepixel
python3 autoclip.py <channel> --no-captions
```

Clips are saved to `clips/`. Stop with `Ctrl+C`.

## Tuning

Edit the constants at the top of `autoclip.py`: `SPIKE_X`, `MIN_MSGS`, `CLIP_LEN`, `PRE`, `COOLDOWN`.

## Note

Only post clips of streams you have permission to use (e.g. a streamer's clipping program).

## License

MIT
