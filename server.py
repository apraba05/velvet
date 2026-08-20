#!/usr/bin/env python3
"""Offline source → verify → deliver simulator for the Velvet demo."""

import json
import os
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).parent
CLIPS = json.loads((ROOT / "data" / "clips.json").read_text())
LOCK = threading.Lock()


def now_ms():
    return int(time.time() * 1000)


class Pipeline:
    def __init__(self):
        self.threshold = 7
        self.redis_up = True
        self.running = True
        self.cache = {}
        self.events = []
        self.manifest_version = 1
        self.reset(seed=True)

    def reset(self, seed=False):
        self.items = []
        self.cache = {}
        self.events = []
        self.started = now_ms()
        for index, clip in enumerate(CLIPS):
            item = {"clip": clip, "stage": "queued", "result": None, "latency": None}
            self.items.append(item)
            if seed and index < 4:
                self._verify(item, quiet=True)
        self._rebuild_manifest()
        self.log("source", f"{len(CLIPS)} metadata records discovered")
        if seed:
            self.log("deliver", f"manifest v{self.manifest_version} warm-started from cached results")

    def log(self, kind, message):
        self.events.insert(0, {"at": time.strftime("%H:%M:%S"), "kind": kind, "message": message})
        self.events = self.events[:30]

    def _compliance(self, clip):
        flags = []
        if clip["faces"]:
            flags.append("face")
        if clip["text"]:
            flags.append("visible text")
        if clip["watermark"]:
            flags.append("watermark")
        return {"passed": not flags, "flags": flags}

    def _quality(self, clip):
        score = round(
            min(10, 2 + clip["camera_motion"] * 0.35
                + clip["multi_object_interaction"] * 0.35
                + clip["depth_cues"] * 0.3)
        )
        return {"score": score, "rationale": clip["quality_rationale"]}

    def _verify(self, item, quiet=False):
        clip = item["clip"]
        latency = 340 + (sum(map(ord, clip["id"])) % 9) * 47
        compliance = self._compliance(clip)
        quality = self._quality(clip)
        result = {"compliance": compliance, "quality": quality, "verified_at": now_ms()}
        item.update({"stage": "verified", "result": result, "latency": latency})
        self.cache[clip["id"]] = {"result": result, "expires": time.time() + 90}
        if not quiet:
            self.log("tool", f"compliance_check({clip['id']}) → {'PASS' if compliance['passed'] else 'FLAG'}")
            self.log("tool", f"spatial_quality_score({clip['id']}) → {quality['score']}/10")
            self.log("cache", f"Redis SET verification:{clip['id']} EX 90")

    def step(self):
        if not self.running:
            return
        pending = next((x for x in self.items if x["stage"] in ("queued", "retrying")), None)
        if pending:
            clip_id = pending["clip"]["id"]
            if not self.redis_up:
                pending["stage"] = "retrying"
                self.log("fail", f"Redis unavailable — {clip_id} held in retry queue")
                return
            cached = self.cache.get(clip_id)
            if cached and cached["expires"] > time.time():
                pending.update({"stage": "verified", "result": cached["result"], "latency": 8})
                self.log("hit", f"Cache HIT {clip_id} — skipped agent")
            else:
                self.log("miss", f"Cache MISS {clip_id} — invoking Bedrock agent stub")
                self._verify(pending)
            self._rebuild_manifest()
        else:
            self.log("deliver", "Queue drained — delivery manifest is current")
            self.running = False

    def _rebuild_manifest(self):
        delivered = []
        for item in self.items:
            result = item["result"]
            if result and result["compliance"]["passed"] and result["quality"]["score"] >= self.threshold:
                item["stage"] = "delivered"
                delivered.append({
                    "clip_id": item["clip"]["id"],
                    "uri": f"s3://velvet-demo/datasets/v{self.manifest_version}/{item['clip']['id']}.mp4",
                    "spatial_quality": result["quality"]["score"],
                    "tags": item["clip"]["tags"],
                })
            elif result:
                item["stage"] = "rejected"
        self.manifest = {
            "dataset": "spatial-reasoning-curated",
            "version": self.manifest_version,
            "generated_at": now_ms(),
            "quality_threshold": self.threshold,
            "records": delivered,
        }

    def action(self, data):
        action = data.get("action")
        if action == "step":
            self.step()
        elif action == "toggle":
            self.running = not self.running
            self.log("source", "Pipeline resumed" if self.running else "Pipeline paused")
        elif action == "reset":
            self.manifest_version += 1
            self.redis_up = True
            self.running = True
            self.reset(seed=False)
        elif action == "replay":
            for item in self.items:
                item["stage"] = "queued"
                item["result"] = None
                item["latency"] = None
            self.running = True
            self._rebuild_manifest()
            self.log("hit", "Batch replay queued — warm Redis keys will bypass the agent")
        elif action == "redis":
            self.redis_up = not self.redis_up
            self.log("fail" if not self.redis_up else "recover",
                     "Redis connection dropped — fallback retry queue active"
                     if not self.redis_up else "Redis recovered — draining retry queue")
        elif action == "threshold":
            self.threshold = max(1, min(10, int(data.get("value", 7))))
            self.manifest_version += 1
            self._rebuild_manifest()
            self.log("deliver", f"Quality gate changed to {self.threshold}/10; manifest v{self.manifest_version} rebuilt")

    def snapshot(self):
        counts = {key: 0 for key in ("queued", "retrying", "verified", "rejected", "delivered")}
        for item in self.items:
            counts[item["stage"]] += 1
        latencies = [x["latency"] for x in self.items if x["latency"]]
        return {
            "items": self.items,
            "counts": counts,
            "events": self.events,
            "manifest": self.manifest,
            "redis_up": self.redis_up,
            "running": self.running,
            "threshold": self.threshold,
            "cache_size": len(self.cache),
            "avg_latency": round(sum(latencies) / len(latencies)) if latencies else 0,
        }


PIPELINE = Pipeline()


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if urlparse(self.path).path == "/api/state":
            with LOCK:
                body = json.dumps(PIPELINE.snapshot()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self):
        if self.path != "/api/action":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        data = json.loads(self.rfile.read(length) or b"{}")
        with LOCK:
            PIPELINE.action(data)
            body = json.dumps(PIPELINE.snapshot()).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        if args and str(args[0]).startswith("GET /api/state"):
            return
        super().log_message(fmt, *args)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    os.chdir(ROOT)
    print(f"Velvet pipeline demo → http://localhost:{port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
