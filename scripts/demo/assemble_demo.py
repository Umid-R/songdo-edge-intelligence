"""Turn the raw recording from record_demo.py into the final MP4.

Reads <out>/raw/*.webm and <out>/segments.json, encodes each normal-speed and
sped-up span separately (one ffmpeg process per span keeps memory low), then
joins them with the concat demuxer. No audio.

    python scripts/demo/assemble_demo.py --out /tmp/demo [--name TeamA_Demo.mp4]
"""
import argparse, glob, json, os, subprocess


def spans(segments, total):
    """[(start, end, speed)] covering the whole recording, fast spans as logged."""
    out, t = [], 0.0
    for s, e, v in segments:
        if s > t:
            out.append((t, s, 1))
        out.append((s, e, v))
        t = e
    out.append((t, total + 1.0, 1))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", default="/tmp/demo")
    ap.add_argument("--name", default="TeamA_Demo.mp4")
    ap.add_argument("--size", default="1600:900")
    a = ap.parse_args()

    raw = sorted(glob.glob(os.path.join(a.out, "raw", "*.webm")))[-1]
    meta = json.load(open(os.path.join(a.out, "segments.json")))
    parts = []
    for i, (s, e, v) in enumerate(spans(meta["segments"], meta["total"])):
        part = os.path.join(a.out, f"part_{i:02d}.mp4")
        subprocess.run(["ffmpeg", "-y", "-v", "error", "-ss", f"{s:.3f}", "-to", f"{e:.3f}", "-i", raw,
                        "-vf", f"setpts=PTS/{v},fps=30,scale={a.size}", "-an",
                        "-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-pix_fmt", "yuv420p", part],
                       check=True)
        parts.append(part)
    listing = os.path.join(a.out, "parts.txt")
    with open(listing, "w") as f:
        f.writelines(f"file '{os.path.basename(p)}'\n" for p in parts)
    final = os.path.join(a.out, a.name)
    subprocess.run(["ffmpeg", "-y", "-v", "error", "-f", "concat", "-safe", "0", "-i", listing,
                    "-c", "copy", "-movflags", "+faststart", final], check=True)
    print("wrote", final)


if __name__ == "__main__":
    main()
