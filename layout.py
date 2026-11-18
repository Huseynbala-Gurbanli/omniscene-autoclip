"""
layout.py - how the 16:9 stream becomes a 1080x1920 vertical clip.

  blur   full gameplay in the middle, blurred copy as background (works for everything)
  fill   crop the middle of the frame to fill the whole screen
  split  streamer's facecam on top, gameplay below (OpusClip-style)

Facecam detection (split): OpenCV finds faces in several frames across the clip. In-game
characters move around; a webcam face stays in the same spot. The most stable face wins.
Needs: pip3 install opencv-python-headless   (without it, split falls back to blur)
"""
import json, os, subprocess

os.environ.setdefault("OPENCV_LOG_LEVEL", "ERROR")

W, H = 1080, 1920
TOP_H = 768                      # facecam panel height in split layout
BOT_H = H - TOP_H


def probe_size(path):
    r = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                        "stream=width,height", "-of", "json", path], capture_output=True, text=True)
    try:
        s = json.loads(r.stdout)["streams"][0]
        return int(s["width"]), int(s["height"])
    except (ValueError, KeyError, IndexError):
        return 1920, 1080


MODEL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models", "face_detection_yunet_2023mar.onnx")


def _face_finder(cv2):
    """YuNet (OpenCV 4.8+/5, model shipped in repo); Haar cascade as fallback for old OpenCV."""
    if hasattr(cv2, "FaceDetectorYN") and os.path.exists(MODEL):
        det = cv2.FaceDetectorYN.create(MODEL, "", (320, 320), 0.75, 0.3, 50)

        def find(img):
            det.setInputSize((img.shape[1], img.shape[0]))
            _, faces = det.detect(img)
            return [tuple(int(v) for v in f[:4]) for f in (faces if faces is not None else [])]
        return find
    xml = os.path.join(getattr(getattr(cv2, "data", None), "haarcascades", ""), "haarcascade_frontalface_default.xml")
    if os.path.exists(xml):
        casc = cv2.CascadeClassifier(xml)

        def find(img):
            gray = cv2.equalizeHist(cv2.cvtColor(img, cv2.COLOR_BGR2GRAY))
            return [tuple(int(v) for v in f) for f in casc.detectMultiScale(gray, 1.1, 6, minSize=(28, 28))]
        return find
    return None


def detect_facecam(path, samples=10):
    """-> (x, y, w, h) of the webcam face in source pixels, or None."""
    try:
        import cv2
    except ImportError:
        return None
    find = _face_finder(cv2)
    if find is None:
        return None
    frames_faces, fw = [], 1920
    for frame in _sample_frames(path, samples):
        fw = frame.shape[1]
        scale = fw / 960
        small = cv2.resize(frame, (960, int(frame.shape[0] / scale)))
        frames_faces.append([tuple(int(v * scale) for v in f) for f in find(small)])
    if not frames_faces:
        return None
    # cluster: a candidate is "stable" if it shows up near the same place in many frames
    best, best_hits = None, 0
    tol = fw * 0.04
    for faces in frames_faces:
        for (x, y, w, h) in faces:
            if w <= 0:
                continue
            cx, cy = x + w / 2, y + h / 2
            hits, boxes = 0, []
            for other in frames_faces:
                for (x2, y2, w2, h2) in other:
                    if abs(x2 + w2 / 2 - cx) < tol and abs(y2 + h2 / 2 - cy) < tol and 0.7 < w2 / w < 1.4:
                        hits += 1
                        boxes.append((x2, y2, w2, h2))
                        break
            if hits > best_hits:
                best_hits = hits
                best = tuple(int(sum(b[i] for b in boxes) / len(boxes)) for i in range(4))
    need = max(3, int(len(frames_faces) * 0.5))
    return best if best_hits >= need else None


def _sample_frames(path, samples):
    """Grab ~`samples` frames spread over the video with ffmpeg (keyframes only = fast)."""
    import cv2, glob, shutil, tempfile
    tmp = tempfile.mkdtemp(prefix="facecam_")
    try:
        subprocess.run(["ffmpeg", "-loglevel", "error", "-skip_frame", "nokey", "-i", path,
                        "-vsync", "vfr", "-vf", "scale=960:-2", "-q:v", "4",
                        os.path.join(tmp, "k%04d.jpg")], timeout=60)
        files = sorted(glob.glob(os.path.join(tmp, "k*.jpg")))
        if len(files) < 4:                      # few keyframes -> decode a handful of evenly spaced frames
            for f in files:
                os.remove(f)
            subprocess.run(["ffmpeg", "-loglevel", "error", "-i", path, "-vf",
                            f"fps=1/3,scale=960:-2", "-frames:v", str(samples), "-q:v", "4",
                            os.path.join(tmp, "k%04d.jpg")], timeout=120)
            files = sorted(glob.glob(os.path.join(tmp, "k*.jpg")))
        if len(files) > samples:
            step = len(files) / samples
            files = [files[int(i * step)] for i in range(samples)]
        sw, _ = probe_size(path)
        out = []
        for f in files:
            img = cv2.imread(f)
            if img is not None:
                if img.shape[1] != sw:          # report coordinates in source pixels
                    img = cv2.resize(img, (sw, int(img.shape[0] * sw / img.shape[1])))
                out.append(img)
        return out
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def cam_box(face, sw, sh):
    """Area around the face for the top panel (aspect 1080:768)."""
    x, y, w, h = face
    bw = min(sw, max(w * 2.6, 260))
    bh = bw * TOP_H / W
    if bh > sh:
        bh = sh
        bw = bh * W / TOP_H
    cx, cy = x + w / 2, y + h / 2 + h * 0.15
    bx = min(max(0, cx - bw / 2), sw - bw)
    by = min(max(0, cy - bh / 2), sh - bh)
    return int(bx) // 2 * 2, int(by) // 2 * 2, int(bw) // 2 * 2, int(bh) // 2 * 2


def build_filter(layout, sw, sh, face=None):
    """ffmpeg filter_complex from [0:v] to [base] (1080x1920). Returns (filter, layout_used)."""
    pre = "[0:v]setpts=PTS-STARTPTS"
    if layout == "split" and face:
        cx, cy, cw, ch = cam_box(face, sw, sh)
        gw = min(sw, int(sh * W / BOT_H)) // 2 * 2         # gameplay crop with the bottom panel's aspect
        gx = (sw - gw) // 2
        return (f"{pre},split[a][b];"
                f"[a]crop={cw}:{ch}:{cx}:{cy},scale={W}:{TOP_H}:force_original_aspect_ratio=increase,"
                f"crop={W}:{TOP_H}[top];"
                f"[b]crop={gw}:{sh}:{gx}:0,scale={W}:{BOT_H}:force_original_aspect_ratio=increase,"
                f"crop={W}:{BOT_H}[bot];"
                f"[top][bot]vstack=inputs=2[base]"), "split"
    if layout == "fill":
        return (f"{pre},scale=-2:{H},crop={W}:{H}[base]"), "fill"
    return (f"{pre},split[a][b];"
            f"[a]scale={W}:{H}:force_original_aspect_ratio=increase,crop={W}:{H},boxblur=20:5[bg];"
            f"[b]scale={W}:-2[fg];"
            f"[bg][fg]overlay=(W-w)/2:(H-h)/2[base]"), "blur"


def compose_preview(frame, layout, face=None):
    """PIL version of build_filter for the caption-studio preview."""
    from PIL import Image, ImageFilter
    sw, sh = frame.size
    if layout == "split" and face:
        cx, cy, cw, ch = cam_box(face, sw, sh)
        top = _cover(frame.crop((cx, cy, cx + cw, cy + ch)), W, TOP_H)
        gw = min(sw, int(sh * W / BOT_H))
        gx = (sw - gw) // 2
        bot = _cover(frame.crop((gx, 0, gx + gw, sh)), W, BOT_H)
        out = Image.new("RGB", (W, H))
        out.paste(top, (0, 0))
        out.paste(bot, (0, TOP_H))
        return out
    if layout == "fill":
        return _cover(frame, W, H)
    bg = _cover(frame, W, H).filter(ImageFilter.GaussianBlur(28))
    fg = frame.resize((W, int(sh * W / sw)))
    bg.paste(fg, (0, (H - fg.height) // 2))
    return bg


def _cover(img, w, h):
    s = max(w / img.width, h / img.height)
    r = img.resize((max(w, int(img.width * s + 0.5)), max(h, int(img.height * s + 0.5))))
    left, top = (r.width - w) // 2, (r.height - h) // 2
    return r.crop((left, top, left + w, top + h))
