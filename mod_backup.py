from __future__ import annotations

import asyncio
import json
import os
import random
import re
import shutil
import time
import traceback
import urllib.request
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import DocumentAttributeVideo

from database import get_db_connection
from tg_client import get_tg_client

router = APIRouter()

MODULE_INFO = {
    "id": "backup",
    "name": "🛟 Backup Media",
    "html_file": "tab_backup.html",
    "color": "emerald",
    "order": 5,
}

TMP_DIR = Path("downloads/backup_tmp")
TMP_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOADS_DIR = Path("downloads")
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
FFMPEG_BIN = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE_BIN = shutil.which("ffprobe") or "ffprobe"

MAX_LAST_ERROR = 240
FLOOD_BUFFER_SECONDS = 2
DEFAULT_UPLOAD_RETRIES = 3
DEFAULT_FETCH_TIMEOUT = int(os.getenv("BACKUP_FETCH_TIMEOUT", "60"))
DEFAULT_DOWNLOAD_TIMEOUT = int(os.getenv("BACKUP_DOWNLOAD_TIMEOUT", "3600"))
DEFAULT_DOWNLOAD_RETRIES = int(os.getenv("BACKUP_DOWNLOAD_RETRIES", "3"))
DEFAULT_UPLOAD_TIMEOUT = int(os.getenv("BACKUP_UPLOAD_TIMEOUT", "3600"))
FAST_ITER_REQUEST_MB = 2
TMP_CLEANUP_MAX_AGE_SEC = int(os.getenv("BACKUP_TMP_MAX_AGE_SEC", "21600"))
DISK_MIN_FREE_MB = int(os.getenv("BACKUP_DISK_MIN_FREE_MB", "4096"))
DISK_WAIT_TIMEOUT_SEC = int(os.getenv("BACKUP_DISK_WAIT_TIMEOUT_SEC", "600"))
DISK_WAIT_INTERVAL_SEC = int(os.getenv("BACKUP_DISK_WAIT_INTERVAL_SEC", "5"))
VIDEO_TMP_OVERHEAD_RATIO = float(os.getenv("BACKUP_VIDEO_TMP_OVERHEAD_RATIO", "1.7"))
GENERIC_TMP_OVERHEAD_RATIO = float(os.getenv("BACKUP_GENERIC_TMP_OVERHEAD_RATIO", "1.15"))
DISK_ABS_MIN_FREE_MB = int(os.getenv("BACKUP_DISK_ABS_MIN_FREE_MB", "768"))

backup_flags: dict[int, bool] = {}
backup_runtime: dict[int, dict[str, Any]] = {}
backup_queue: list[int] = []
backup_queue_running: bool = False
backup_queue_stop: bool = False
backup_queue_current: int | None = None


class BackupReq(BaseModel):
    name: str
    start_link: str
    end_link: str = ""
    target_link: str
    media_filter: str = "all"
    caption_mode: str = "source"
    caption: str = ""
    batch_limit: int = 50
    delay_min: float = 1
    delay_max: float = 3
    stop_on_error: bool = False
    fast_download: bool = True
    upload_retry_max: int = DEFAULT_UPLOAD_RETRIES
    download_workers: int = 1
    upload_workers: int = 1


@dataclass
class VideoPlanAttempt:
    name: str
    out_suffix: str
    mode: str


def init_db(conn):
    conn.execute(
        '''CREATE TABLE IF NOT EXISTS backup_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        source_chat TEXT,
        source_topic_id INTEGER,
        target_link TEXT,
        target_topic_id INTEGER,
        start_id INTEGER,
        end_id INTEGER,
        last_processed_id INTEGER,
        media_filter TEXT DEFAULT 'all',
        caption_mode TEXT DEFAULT 'source',
        caption TEXT,
        batch_limit INTEGER DEFAULT 50,
        delay_min REAL DEFAULT 1,
        delay_max REAL DEFAULT 3,
        stop_on_error BOOLEAN DEFAULT 0,
        fast_download BOOLEAN DEFAULT 1,
        upload_retry_max INTEGER DEFAULT 3,
        download_workers INTEGER DEFAULT 4,
        upload_workers INTEGER DEFAULT 1,
        processed_count INTEGER DEFAULT 0,
        uploaded_count INTEGER DEFAULT 0,
        skipped_count INTEGER DEFAULT 0,
        error_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'Sẵn sàng',
        last_error TEXT DEFAULT ''
    )'''
    )

    conn.execute(
        '''CREATE TABLE IF NOT EXISTS backup_file_telemetry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        source_message_id INTEGER NOT NULL,
        media_kind TEXT NOT NULL,
        plan_selected TEXT DEFAULT '',
        action_used TEXT DEFAULT '',
        probe_input_json TEXT DEFAULT '',
        retries_json TEXT DEFAULT '',
        verify_json TEXT DEFAULT '',
        result TEXT DEFAULT '',
        error_class TEXT DEFAULT '',
        error_reason TEXT DEFAULT '',
        created_at INTEGER NOT NULL,
        FOREIGN KEY(job_id) REFERENCES backup_jobs(id) ON DELETE CASCADE
    )'''
    )
    conn.execute(
        '''CREATE TABLE IF NOT EXISTS backup_job_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        seq INTEGER NOT NULL,
        source_message_id INTEGER NOT NULL,
        media_kind TEXT NOT NULL,
        grouped_id INTEGER DEFAULT 0,
        state TEXT NOT NULL DEFAULT 'pending',
        sub_state TEXT DEFAULT '',
        sub_progress INTEGER DEFAULT 0,
        phase_started_at INTEGER DEFAULT 0,
        last_progress_at INTEGER DEFAULT 0,
        attempt_count INTEGER DEFAULT 0,
        last_error TEXT DEFAULT '',
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        UNIQUE(job_id, source_message_id),
        FOREIGN KEY(job_id) REFERENCES backup_jobs(id) ON DELETE CASCADE
    )'''
    )
    conn.execute(
        '''CREATE TABLE IF NOT EXISTS backup_job_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        level TEXT NOT NULL DEFAULT 'info',
        phase TEXT NOT NULL DEFAULT '',
        message TEXT NOT NULL,
        source_message_id INTEGER DEFAULT 0,
        created_at INTEGER NOT NULL,
        FOREIGN KEY(job_id) REFERENCES backup_jobs(id) ON DELETE CASCADE
    )'''
    )

    existing = {row[1] for row in conn.execute("PRAGMA table_info(backup_jobs)").fetchall()}
    columns = {
        "stop_on_error": "BOOLEAN DEFAULT 0",
        "fast_download": "BOOLEAN DEFAULT 1",
        "upload_retry_max": "INTEGER DEFAULT 3",
        "download_workers": "INTEGER DEFAULT 4",
        "upload_workers": "INTEGER DEFAULT 1",
        "last_error": "TEXT DEFAULT ''",
        "scan_complete": "BOOLEAN DEFAULT 0",
        "scanned_total": "INTEGER DEFAULT 0",
    }
    for column, col_type in columns.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE backup_jobs ADD COLUMN {column} {col_type}")

    queue_existing = {row[1] for row in conn.execute("PRAGMA table_info(backup_job_queue)").fetchall()}
    queue_columns = {
        "grouped_id": "INTEGER DEFAULT 0",
        "sub_state": "TEXT DEFAULT ''",
        "sub_progress": "INTEGER DEFAULT 0",
        "phase_started_at": "INTEGER DEFAULT 0",
        "last_progress_at": "INTEGER DEFAULT 0",
        "attempt_count": "INTEGER DEFAULT 0",
    }
    for column, col_type in queue_columns.items():
        if column not in queue_existing:
            conn.execute(f"ALTER TABLE backup_job_queue ADD COLUMN {column} {col_type}")

    conn.commit()


def clamp(value, default, min_value, max_value):
    try:
        n = type(default)(value)
    except Exception:
        n = default
    return max(min_value, min(max_value, n))


def normalize_short_error(err: Exception | str) -> str:
    return str(err).replace("\n", " ").strip()[:MAX_LAST_ERROR]


def cleanup_stale_tmp_files(max_age_sec: int = TMP_CLEANUP_MAX_AGE_SEC) -> tuple[int, int]:
    removed = 0
    freed_bytes = 0
    now_ts = time.time()
    try:
        names = os.listdir(TMP_DIR)
    except Exception:
        return 0, 0
    for name in names:
        p = TMP_DIR / name
        try:
            if not p.is_file():
                continue
            st = p.stat()
            if max_age_sec > 0 and (now_ts - st.st_mtime) < max_age_sec:
                continue
            freed_bytes += int(st.st_size or 0)
            p.unlink()
            removed += 1
        except Exception:
            continue
    return removed, freed_bytes


def cleanup_download_junk(max_age_sec: int = TMP_CLEANUP_MAX_AGE_SEC) -> tuple[int, int]:
    removed = 0
    freed_bytes = 0
    now_ts = time.time()
    junk_suffixes = (
        ".part",
        ".tmp",
        ".temp",
        ".crdownload",
        ".download",
        ".aria2",
        ".log",
    )
    junk_names = ("ffmpeg2pass", "thumb", "remux", "safe", "low", "src")
    if not DOWNLOADS_DIR.exists():
        return 0, 0
    for p in DOWNLOADS_DIR.rglob("*"):
        try:
            if not p.is_file():
                continue
            rel = str(p.relative_to(DOWNLOADS_DIR))
            if rel.startswith("backup_tmp/"):
                continue
            name = p.name.lower()
            if max_age_sec > 0 and (now_ts - p.stat().st_mtime) < max_age_sec:
                continue
            if (name.endswith(junk_suffixes) or any(k in name for k in junk_names) or p.stat().st_size == 0):
                freed_bytes += int(p.stat().st_size or 0)
                p.unlink()
                removed += 1
        except Exception:
            continue
    # Remove empty folders except backup_tmp
    for d in sorted(DOWNLOADS_DIR.rglob("*"), reverse=True):
        try:
            if d.is_dir() and d != TMP_DIR and not any(d.iterdir()):
                d.rmdir()
        except Exception:
            continue
    return removed, freed_bytes


def tmp_dir_free_bytes() -> int:
    return int(shutil.disk_usage(TMP_DIR).free)


def estimate_tmp_need_bytes(msg, kind: str) -> int:
    src_size = int(getattr(getattr(msg, "file", None), "size", 0) or 0)
    ratio = VIDEO_TMP_OVERHEAD_RATIO if kind == "video" else GENERIC_TMP_OVERHEAD_RATIO
    if src_size <= 0:
        # Unknown size: reserve a conservative 256MB to reduce ENOSPC risk.
        return 256 * 1024 * 1024
    # Cap oversized estimation on free-tier disks while keeping safety margin.
    est = int(src_size * ratio)
    if kind == "video":
        extra_cap = min(2 * 1024 * 1024 * 1024, int(src_size * 0.8))
        est = max(src_size + 256 * 1024 * 1024, src_size + extra_cap)
    return est


def probe_network_mbps() -> float:
    test_urls = [
        "https://speed.cloudflare.com/__down?bytes=1048576",
        "https://speed.hetzner.de/1MB.bin",
    ]
    for u in test_urls:
        try:
            start = time.time()
            with urllib.request.urlopen(u, timeout=6) as resp:
                payload = resp.read(512 * 1024)
            elapsed = max(time.time() - start, 0.001)
            if not payload:
                continue
            mbps = (len(payload) / elapsed) / (1024 * 1024)
            if mbps > 0:
                return round(mbps, 2)
        except Exception:
            continue
    return 0.0


def auto_tune_workers(job: dict[str, Any]) -> tuple[int, int, dict[str, float | int]]:
    configured_dl = clamp(job.get("download_workers"), 4, 1, 8)
    configured_ul = clamp(job.get("upload_workers"), 1, 1, 2)
    free_mb = round(tmp_dir_free_bytes() / (1024 * 1024), 1)
    net_mbps = probe_network_mbps()

    if free_mb < 4096:
        disk_dl_cap, disk_ul_cap = 1, 1
    elif free_mb < 8192:
        disk_dl_cap, disk_ul_cap = 2, 1
    elif free_mb < 16384:
        disk_dl_cap, disk_ul_cap = 3, 1
    else:
        disk_dl_cap, disk_ul_cap = 4, 2

    if net_mbps <= 0:
        net_dl_cap, net_ul_cap = 2, 1
    elif net_mbps < 8:
        net_dl_cap, net_ul_cap = 1, 1
    elif net_mbps < 20:
        net_dl_cap, net_ul_cap = 2, 1
    elif net_mbps < 40:
        net_dl_cap, net_ul_cap = 3, 1
    else:
        net_dl_cap, net_ul_cap = 4, 2

    dl_workers = max(1, min(configured_dl, disk_dl_cap, net_dl_cap))
    ul_workers = max(1, min(configured_ul, disk_ul_cap, net_ul_cap))
    return dl_workers, ul_workers, {
        "free_mb": free_mb,
        "net_mbps": net_mbps,
        "configured_dl": configured_dl,
        "configured_ul": configured_ul,
        "effective_dl": dl_workers,
        "effective_ul": ul_workers,
    }


async def ensure_disk_budget(job_id: int, msg_id: int, required_bytes: int):
    usage = shutil.disk_usage(TMP_DIR)
    total_bytes = int(usage.total)
    configured_reserve = max(0, DISK_MIN_FREE_MB) * 1024 * 1024
    hard_min_reserve = max(256, DISK_ABS_MIN_FREE_MB) * 1024 * 1024
    # Adaptive reserve: avoid over-conservative 4GB on small free-tier disks.
    adaptive_cap = int(total_bytes * 0.06)  # keep a practical reserve on small free-tier disks
    reserve_bytes = max(hard_min_reserve, min(configured_reserve, adaptive_cap))
    if reserve_bytes > configured_reserve:
        reserve_bytes = configured_reserve

    need_data = max(0, int(required_bytes))
    need_total = need_data + reserve_bytes
    # If computed budget is impossible on this disk, fallback to hard-min reserve.
    if need_total > int(total_bytes * 0.95):
        reserve_bytes = hard_min_reserve
        need_total = need_data + reserve_bytes

    deadline = time.time() + max(5, DISK_WAIT_TIMEOUT_SEC)
    cleaned_once = False
    while True:
        free_now = tmp_dir_free_bytes()
        if free_now >= need_total:
            return
        if not cleaned_once:
            cleanup_stale_tmp_files()
            cleanup_download_junk()
            cleaned_once = True
            free_now = tmp_dir_free_bytes()
            if free_now >= need_total:
                return
        if time.time() >= deadline:
            need_mb = round(need_total / (1024 * 1024), 1)
            free_mb = round(free_now / (1024 * 1024), 1)
            reserve_mb = round(reserve_bytes / (1024 * 1024), 1)
            update_runtime(
                job_id,
                action="disk_wait_timeout",
                state=f"Thiếu dung lượng: cần~{need_mb}MB (reserve {reserve_mb}), còn~{free_mb}MB",
                current_message_id=msg_id,
            )
            raise RuntimeError(f"disk_space_wait_timeout:need_mb={need_mb}:free_mb={free_mb}:reserve_mb={reserve_mb}")
        update_runtime(
            job_id,
            action="disk_wait",
            state=f"Chờ dung lượng trống cho #{msg_id}",
            current_message_id=msg_id,
        )
        await asyncio.sleep(max(1, DISK_WAIT_INTERVAL_SEC))


def update_job(job_id: int, **fields):
    if not fields:
        return
    keys = list(fields.keys())
    query = "UPDATE backup_jobs SET " + ", ".join([f"{k}=?" for k in keys]) + " WHERE id=?"
    params = [fields[k] for k in keys] + [job_id]
    with closing(get_db_connection()) as conn:
        conn.execute(query, params)
        conn.commit()


def fetch_job(job_id: int) -> dict[str, Any] | None:
    with closing(get_db_connection()) as conn:
        conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        return conn.execute("SELECT * FROM backup_jobs WHERE id=?", (job_id,)).fetchone()


def record_telemetry(job_id: int, source_message_id: int, media_kind: str, **data):
    with closing(get_db_connection()) as conn:
        conn.execute(
            '''INSERT INTO backup_file_telemetry (
                job_id, source_message_id, media_kind, plan_selected, action_used,
                probe_input_json, retries_json, verify_json, result, error_class,
                error_reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                job_id,
                source_message_id,
                media_kind,
                data.get("plan_selected", ""),
                data.get("action_used", ""),
                json.dumps(data.get("probe_input", {}), ensure_ascii=False),
                json.dumps(data.get("retries", []), ensure_ascii=False),
                json.dumps(data.get("verify", {}), ensure_ascii=False),
                data.get("result", ""),
                data.get("error_class", ""),
                data.get("error_reason", ""),
                int(time.time()),
            ),
        )
        conn.commit()


def record_job_log(job_id: int, level: str, phase: str, message: str, source_message_id: int = 0):
    with closing(get_db_connection()) as conn:
        conn.execute(
            """INSERT INTO backup_job_logs (job_id, level, phase, message, source_message_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (job_id, level[:16], phase[:32], (message or "")[:500], int(source_message_id or 0), int(time.time())),
        )
        conn.commit()


def parse_source_link(link: str):
    raw = (link or "").strip()
    clean = raw.replace("https://", "").replace("http://", "").rstrip("/")

    m_topic = re.search(r"t\.me/c/(\d+)/(\d+)/(\d+)$", clean)
    if m_topic:
        return str(int("-100" + m_topic.group(1))), int(m_topic.group(2)), int(m_topic.group(3))

    m_chat_msg = re.search(r"t\.me/c/(\d+)/(\d+)$", clean)
    if m_chat_msg:
        return str(int("-100" + m_chat_msg.group(1))), None, int(m_chat_msg.group(2))

    m_user_msg = re.search(r"t\.me/([A-Za-z0-9_]{5,})/(\d+)$", clean)
    if m_user_msg:
        return m_user_msg.group(1), None, int(m_user_msg.group(2))

    return None, None, 0


def parse_target(link: str):
    raw = (link or "").strip()
    clean = raw.replace("https://", "").replace("http://", "").rstrip("/")

    m_topic_msg = re.search(r"t\.me/c/(\d+)/(\d+)/\d+$", clean)
    if m_topic_msg:
        return str(int("-100" + m_topic_msg.group(1))), int(m_topic_msg.group(2))

    m_topic = re.search(r"t\.me/c/(\d+)/(\d+)$", clean)
    if m_topic:
        return str(int("-100" + m_topic.group(1))), int(m_topic.group(2))

    m_c = re.search(r"t\.me/c/(\d+)$", clean)
    if m_c:
        return str(int("-100" + m_c.group(1))), None

    m_user = re.search(r"t\.me/([A-Za-z0-9_]{5,})$", clean)
    if m_user:
        return m_user.group(1), None

    return None, None


async def get_safe_entity(client, identifier):
    if identifier is None:
        return None
    value = int(identifier) if str(identifier).lstrip("-").isdigit() else identifier
    try:
        return await client.get_entity(value)
    except Exception:
        # Warm cache dialog ở nhiều depth để lấy được access_hash cho channel private.
        try:
            await client.get_dialogs(limit=500)
            return await client.get_entity(value)
        except Exception:
            pass
        async for _ in client.iter_dialogs(limit=None):
            try:
                return await client.get_entity(value)
            except Exception:
                continue
        raise RuntimeError(
            f"entity_not_found:{value}. Hãy kiểm tra đúng account session đã join source/target channel."
        )


def media_kind(msg):
    if getattr(msg, "video", None):
        return "video"
    if getattr(msg, "photo", None):
        return "photo"
    if getattr(msg, "document", None):
        return "document"
    if getattr(msg, "media", None):
        return "media"
    return "none"


def include_media(msg, media_filter: str):
    kind = media_kind(msg)
    if kind == "none":
        return False
    if media_filter == "video":
        return kind == "video"
    if media_filter == "photo":
        return kind == "photo"
    if media_filter == "document":
        return kind == "document"
    return True


def caption_for(job, msg):
    mode = (job.get("caption_mode") or "source").lower()
    if mode == "none":
        return ""
    if mode == "custom":
        return job.get("caption") or ""
    if mode == "both":
        custom = job.get("caption") or ""
        source = msg.text or ""
        if custom and source:
            return f"{custom}\n\n{source}"
        return custom or source
    return msg.text or ""


def path_for(job_id: int, msg_id: int, suffix: str):
    return str(TMP_DIR / f"backup_{job_id}_{msg_id}_{int(time.time()*1000)}_{suffix}")


async def run_media_tool(*args, timeout=900):
    process = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise RuntimeError(f"media_timeout:{args[0]}")
    if process.returncode != 0:
        err = stderr.decode("utf-8", "ignore").strip()
        raise RuntimeError(err[-600:] or f"media_failed:{args[0]}")
    return stdout


async def ffprobe_json(path: str):
    out = await run_media_tool(
        FFPROBE_BIN,
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        path,
        timeout=120,
    )
    return json.loads(out.decode("utf-8", "ignore") or "{}")


def first_stream(probe, typ):
    for stream in probe.get("streams", []):
        if stream.get("codec_type") == typ:
            return stream
    return None


def rotate_value(stream):
    tags = stream.get("tags") or {}
    rotate = tags.get("rotate")
    if rotate is not None:
        try:
            return int(float(rotate)) % 360
        except Exception:
            return 0
    for side in stream.get("side_data_list") or []:
        if side.get("rotation") is not None:
            try:
                return int(float(side.get("rotation"))) % 360
            except Exception:
                return 0
    return 0


def duration_of(probe, stream=None):
    values = []
    if stream:
        values.append(stream.get("duration"))
    values.append((probe.get("format") or {}).get("duration"))
    for value in values:
        try:
            x = float(value)
            if x > 0:
                return x
        except Exception:
            pass
    return 0.0


def infer_dimensions(stream):
    w = int(stream.get("width") or 0)
    h = int(stream.get("height") or 0)
    if rotate_value(stream) in (90, 270):
        return h, w
    return w, h


def video_plan():
    return [
        VideoPlanAttempt(name="tier_1_remux", out_suffix="remux.mp4", mode="remux"),
        VideoPlanAttempt(name="tier_2_transcode_safe", out_suffix="safe.mp4", mode="transcode"),
        VideoPlanAttempt(name="tier_3_transcode_low", out_suffix="low.mp4", mode="transcode_low"),
    ]


async def remux_clean(src: str, dst: str):
    await run_media_tool(
        FFMPEG_BIN,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-fflags",
        "+genpts",
        "-i",
        src,
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c",
        "copy",
        "-movflags",
        "+faststart",
        "-avoid_negative_ts",
        "make_zero",
        "-map_metadata",
        "-1",
        dst,
        timeout=1800,
    )


async def transcode_safe(src: str, dst: str, low: bool = False):
    crf = "24" if low else "20"
    preset = "faster" if low else "veryfast"
    audio_bitrate = "96k" if low else "128k"
    maxrate = "2500k" if low else "5000k"
    bufsize = "5000k" if low else "10000k"

    await run_media_tool(
        FFMPEG_BIN,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-fflags",
        "+genpts",
        "-i",
        src,
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-vf",
        "scale=trunc(iw/2)*2:trunc(ih/2)*2,setsar=1,fps=30",
        "-vsync",
        "cfr",
        "-c:v",
        "libx264",
        "-preset",
        preset,
        "-crf",
        crf,
        "-maxrate",
        maxrate,
        "-bufsize",
        bufsize,
        "-pix_fmt",
        "yuv420p",
        "-profile:v",
        "high",
        "-level",
        "4.1",
        "-c:a",
        "aac",
        "-b:a",
        audio_bitrate,
        "-ar",
        "48000",
        "-ac",
        "2",
        "-movflags",
        "+faststart",
        "-avoid_negative_ts",
        "make_zero",
        "-map_metadata",
        "-1",
        dst,
        timeout=7200,
    )


async def build_thumbnail(src: str, dst: str, duration: float):
    probes = [max(1.0, duration * 0.12), max(1.0, duration * 0.3), max(1.0, duration * 0.55), max(1.0, duration * 0.78)]
    seen = set()
    for t in probes:
        ts = min(t, max(0.7, duration - 0.7)) if duration > 1 else 0.1
        key = round(ts, 1)
        if key in seen:
            continue
        seen.add(key)
        try:
            await run_media_tool(
                FFMPEG_BIN,
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                f"{ts:.3f}",
                "-i",
                src,
                "-frames:v",
                "1",
                "-vf",
                "scale='min(640,iw)':-2",
                "-q:v",
                "3",
                dst,
                timeout=120,
            )
            if os.path.exists(dst) and os.path.getsize(dst) > 0:
                return dst
        except Exception:
            pass
    return None


def make_video_attributes(probe):
    stream = first_stream(probe, "video")
    if not stream:
        raise RuntimeError("video_no_stream")
    duration = duration_of(probe, stream)
    width, height = infer_dimensions(stream)
    if duration <= 0:
        raise RuntimeError("video_invalid_duration")
    if width <= 0 or height <= 0:
        raise RuntimeError("video_invalid_dimension")
    if width % 2:
        width -= 1
    if height % 2:
        height -= 1
    if width <= 0 or height <= 0:
        raise RuntimeError("video_invalid_dimension_after_even_fix")
    attrs = [DocumentAttributeVideo(duration=int(round(duration)), w=width, h=height, supports_streaming=True)]
    return attrs, duration, width, height


async def prepare_video(src_path: str, job_id: int, msg_id: int):
    input_probe = await ffprobe_json(src_path)
    attempts = []
    failures = []

    for plan in video_plan():
        out_path = path_for(job_id, msg_id, plan.out_suffix)
        thumb_path = path_for(job_id, msg_id, "thumb.jpg")
        try:
            if plan.mode == "remux":
                await remux_clean(src_path, out_path)
            elif plan.mode == "transcode":
                await transcode_safe(src_path, out_path, low=False)
            else:
                await transcode_safe(src_path, out_path, low=True)

            out_probe = await ffprobe_json(out_path)
            attrs, duration, width, height = make_video_attributes(out_probe)
            thumb = await build_thumbnail(out_path, thumb_path, duration)
            return {
                "ok": True,
                "action": plan.mode,
                "plan": plan.name,
                "file": out_path,
                "thumb": thumb,
                "attributes": attrs,
                "duration": duration,
                "width": width,
                "height": height,
                "probe_input": input_probe,
                "probe_output": out_probe,
                "failures": failures,
            }
        except Exception as e:
            failures.append({"plan": plan.name, "error": normalize_short_error(e)})
            attempts.append(out_path)
            if os.path.exists(out_path):
                try:
                    os.remove(out_path)
                except Exception:
                    pass
            if os.path.exists(thumb_path):
                try:
                    os.remove(thumb_path)
                except Exception:
                    pass

    return {"ok": False, "probe_input": input_probe, "failures": failures}


def verify_uploaded_video(sent_message):
    result = {
        "has_document": False,
        "has_video_attr": False,
        "supports_streaming": False,
        "duration": 0,
        "w": 0,
        "h": 0,
    }
    doc = getattr(sent_message, "document", None)
    if not doc:
        return result
    result["has_document"] = True
    for attr in doc.attributes or []:
        if isinstance(attr, DocumentAttributeVideo):
            result["has_video_attr"] = True
            result["supports_streaming"] = bool(getattr(attr, "supports_streaming", False))
            result["duration"] = int(getattr(attr, "duration", 0) or 0)
            result["w"] = int(getattr(attr, "w", 0) or 0)
            result["h"] = int(getattr(attr, "h", 0) or 0)
            break
    return result


def verify_ok(v):
    return bool(
        v.get("has_document")
        and v.get("has_video_attr")
        and v.get("supports_streaming")
        and v.get("duration", 0) > 0
        and v.get("w", 0) > 0
        and v.get("h", 0) > 0
    )


async def sleep_with_pause_check(job_id: int, seconds: float):
    remaining = max(0.0, seconds)
    while remaining > 0:
        if not backup_flags.get(job_id):
            raise asyncio.CancelledError("paused")
        step = min(1.0, remaining)
        await asyncio.sleep(step)
        remaining -= step


async def upload_with_retry(job_id: int, client, target, send_params: dict, retry_max: int):
    retries = []
    last_exc = None
    progress_msg_id = int(send_params.pop("_progress_msg_id", 0) or 0)
    for idx in range(1, retry_max + 1):
        if not backup_flags.get(job_id):
            raise asyncio.CancelledError("paused")
        try:
            start_ts = time.time()
            kwargs = dict(send_params)

            async def upload_progress(cur, total):
                if not backup_flags.get(job_id):
                    raise asyncio.CancelledError("paused")
                pct = round((cur / total) * 100, 1) if total else 0
                backup_runtime[job_id]["upload_pct"] = pct
                if progress_msg_id:
                    runtime_update_speed(job_id, progress_msg_id, "upload", int(cur or 0), start_ts)

            kwargs["progress_callback"] = upload_progress
            sent = await asyncio.wait_for(client.send_file(target, **kwargs), timeout=DEFAULT_UPLOAD_TIMEOUT)
            retries.append({"attempt": idx, "result": "ok"})
            if progress_msg_id:
                runtime_clear_speed(job_id, progress_msg_id, "upload")
            return sent, retries
        except FloodWaitError as e:
            wait_sec = int(getattr(e, "seconds", 0) or 0) + FLOOD_BUFFER_SECONDS
            retries.append({"attempt": idx, "result": "flood_wait", "sleep": wait_sec})
            await sleep_with_pause_check(job_id, wait_sec)
            last_exc = e
        except (TimeoutError, asyncio.TimeoutError, OSError) as e:
            delay = min(30, 2 ** (idx - 1))
            retries.append({"attempt": idx, "result": "network_retry", "sleep": delay, "error": normalize_short_error(e)})
            await sleep_with_pause_check(job_id, delay)
            last_exc = e
        except RPCError as e:
            delay = min(20, idx * 2)
            retries.append({"attempt": idx, "result": "rpc_retry", "sleep": delay, "error": normalize_short_error(e)})
            await sleep_with_pause_check(job_id, delay)
            last_exc = e
        except Exception as e:
            retries.append({"attempt": idx, "result": "fatal", "error": normalize_short_error(e)})
            last_exc = e
            break
    raise RuntimeError(f"upload_retry_exhausted:{normalize_short_error(last_exc)}")


def validate_downloaded_file(path: str, expected_bytes: int = 0):
    if not path or not os.path.exists(path):
        raise RuntimeError("download_missing_file")
    actual = int(os.path.getsize(path) or 0)
    if actual <= 0:
        raise RuntimeError("download_empty_file")
    if expected_bytes > 0 and actual < int(expected_bytes * 0.98):
        raise RuntimeError(f"download_size_mismatch:expected={expected_bytes}:actual={actual}")


async def download_media_with_fallback(job_id: int, client, msg, dst: str, fast_download: bool):
    msg_id = int(getattr(msg, "id", 0) or 0)
    total_bytes = int(getattr(getattr(msg, "file", None), "size", 0) or 0)
    errors = []

    async def progress(cur, total, start_ts):
        if not backup_flags.get(job_id):
            raise asyncio.CancelledError("paused")
        pct = round((cur / total) * 100, 1) if total else 0
        backup_runtime[job_id]["download_pct"] = pct
        if msg_id:
            runtime_update_speed(job_id, msg_id, "download", int(cur or 0), start_ts)

    async def clear_partial():
        try:
            if dst and os.path.exists(dst):
                os.remove(dst)
        except Exception:
            pass

    try:
        for attempt in range(1, max(1, DEFAULT_DOWNLOAD_RETRIES) + 1):
            methods = ["fast", "standard"] if fast_download else ["standard"]
            for method in methods:
                await clear_partial()
                start_ts = time.time()
                try:
                    if method == "fast":
                        async def iter_fast_download():
                            downloaded = 0
                            req_size = FAST_ITER_REQUEST_MB * 1024 * 1024
                            with open(dst, "wb") as fh:
                                async for chunk in client.iter_download(msg.media, request_size=req_size):
                                    if not backup_flags.get(job_id):
                                        raise asyncio.CancelledError("paused")
                                    if not chunk:
                                        continue
                                    fh.write(chunk)
                                    downloaded += len(chunk)
                                    await progress(downloaded, total_bytes, start_ts)

                        await asyncio.wait_for(iter_fast_download(), timeout=DEFAULT_DOWNLOAD_TIMEOUT)
                        validate_downloaded_file(dst, total_bytes)
                        return dst

                    async def standard_progress(cur, total):
                        await progress(cur, total, start_ts)

                    result_path = await asyncio.wait_for(
                        client.download_media(
                            msg.media,
                            file=dst,
                            progress_callback=standard_progress,
                        ),
                        timeout=DEFAULT_DOWNLOAD_TIMEOUT,
                    )
                    final_path = result_path or dst
                    validate_downloaded_file(final_path, total_bytes)
                    if final_path != dst and os.path.exists(final_path):
                        return str(final_path)
                    return dst
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    errors.append(f"{method}#{attempt}:{normalize_short_error(e)}")
                    record_job_log(job_id, "warn", "download", errors[-1], msg_id)
                    await clear_partial()
                    if method == "fast":
                        continue
                    if attempt < max(1, DEFAULT_DOWNLOAD_RETRIES):
                        await sleep_with_pause_check(job_id, min(12, 2 ** (attempt - 1)))

        raise RuntimeError(f"download_failed:{' | '.join(errors[-6:])}")
    finally:
        if msg_id:
            runtime_clear_speed(job_id, msg_id, "download")


async def iterate_job_messages(client, job, from_last_processed: bool = True):
    src = await get_safe_entity(client, job["source_chat"])
    start_id = int(job["start_id"])
    end_id = int(job["end_id"])
    is_forward = start_id <= end_id

    iter_kwargs = {"entity": src}
    if is_forward:
        min_id = int(job["last_processed_id"]) if from_last_processed else start_id - 1
        iter_kwargs.update({"reverse": True, "min_id": min_id})
    else:
        max_id = int(job["last_processed_id"]) if from_last_processed else start_id + 1
        iter_kwargs.update({"reverse": False, "max_id": max_id})

    async for msg in client.iter_messages(**iter_kwargs):
        if job.get("source_topic_id"):
            top_id = getattr(msg.reply_to, "reply_to_top_id", None) or getattr(msg.reply_to, "reply_to_msg_id", None)
            if top_id != job["source_topic_id"] and msg.id != job["source_topic_id"]:
                continue

        if is_forward and msg.id > end_id:
            break
        if not is_forward and msg.id < end_id:
            break
        yield msg


def queue_clear(job_id: int):
    with closing(get_db_connection()) as conn:
        conn.execute("DELETE FROM backup_job_queue WHERE job_id=?", (job_id,))
        conn.commit()


def queue_stats(job_id: int) -> dict[str, int]:
    with closing(get_db_connection()) as conn:
        row = conn.execute(
            """SELECT
                SUM(CASE WHEN state='pending' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN state='done' THEN 1 ELSE 0 END) AS done,
                SUM(CASE WHEN state='error' THEN 1 ELSE 0 END) AS err,
                COUNT(*) AS total
            FROM backup_job_queue WHERE job_id=?""",
            (job_id,),
        ).fetchone()
        return {
            "pending": int((row[0] or 0) if row else 0),
            "done": int((row[1] or 0) if row else 0),
            "error": int((row[2] or 0) if row else 0),
            "total": int((row[3] or 0) if row else 0),
        }


def queue_next_pending(job_id: int):
    with closing(get_db_connection()) as conn:
        conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        return conn.execute(
            "SELECT * FROM backup_job_queue WHERE job_id=? AND state='pending' ORDER BY seq ASC LIMIT 1",
            (job_id,),
        ).fetchone()


def queue_mark(job_id: int, message_id: int, state: str, last_error: str = ""):
    with closing(get_db_connection()) as conn:
        if state == "error":
            conn.execute(
                """UPDATE backup_job_queue
                   SET state=?,
                       last_error=?,
                       attempt_count=attempt_count+1,
                       updated_at=?
                   WHERE job_id=? AND source_message_id=?""",
                (state, last_error[:MAX_LAST_ERROR], int(time.time()), job_id, message_id),
            )
        else:
            conn.execute(
                """UPDATE backup_job_queue
                   SET state=?, last_error=?, updated_at=?
                   WHERE job_id=? AND source_message_id=?""",
                (state, last_error[:MAX_LAST_ERROR], int(time.time()), job_id, message_id),
            )
        conn.commit()


def queue_sub_progress(job_id: int, message_id: int, sub_state: str, sub_progress: int):
    with closing(get_db_connection()) as conn:
        conn.execute(
            """UPDATE backup_job_queue
               SET sub_state=?,
                   sub_progress=?,
                   phase_started_at=CASE WHEN sub_state<>? THEN ? ELSE phase_started_at END,
                   last_progress_at=?,
                   updated_at=?
               WHERE job_id=? AND source_message_id=?""",
            (
                sub_state[:32],
                int(max(0, min(100, sub_progress))),
                sub_state[:32],
                int(time.time()),
                int(time.time()),
                int(time.time()),
                job_id,
                message_id,
            ),
        )
        conn.commit()


def queue_attempt_inc(job_id: int, message_id: int):
    with closing(get_db_connection()) as conn:
        conn.execute(
            """UPDATE backup_job_queue
               SET attempt_count=attempt_count+1, updated_at=?
               WHERE job_id=? AND source_message_id=?""",
            (int(time.time()), job_id, message_id),
        )
        conn.commit()


def queue_recover_in_progress(job_id: int):
    with closing(get_db_connection()) as conn:
        conn.execute(
            """UPDATE backup_job_queue
               SET state='pending', updated_at=?
               WHERE job_id=? AND state='in_progress'""",
            (int(time.time()), job_id),
        )
        conn.commit()


def is_retryable_queue_error(last_error: str) -> bool:
    text = (last_error or "").lower()
    keys = (
        "disk_space_wait_timeout",
        "timeout",
        "timed out",
        "network",
        "flood",
        "rpc",
        "connection",
        "temporarily",
        "429",
    )
    return any(k in text for k in keys)


def queue_requeue_errors(job_id: int, retryable_only: bool = True, max_attempt_count: int = 3) -> int:
    now_ts = int(time.time())
    with closing(get_db_connection()) as conn:
        rows = conn.execute(
            "SELECT source_message_id, attempt_count, last_error FROM backup_job_queue WHERE job_id=? AND state='error'",
            (job_id,),
        ).fetchall()
        selected = []
        for row in rows:
            msg_id = int(row[0])
            attempts = int(row[1] or 0)
            last_error = str(row[2] or "")
            if attempts >= max_attempt_count:
                continue
            if retryable_only and not is_retryable_queue_error(last_error):
                continue
            selected.append(msg_id)
        if selected:
            conn.executemany(
                """UPDATE backup_job_queue
                   SET state='pending',
                       sub_state='retry_pending',
                       sub_progress=0,
                       last_error='',
                       updated_at=?
                   WHERE job_id=? AND source_message_id=?""",
                [(now_ts, job_id, mid) for mid in selected],
            )
            # These items were already counted as processed+error. Requeueing them
            # should make the job counters represent the current queue state again.
            conn.execute(
                """UPDATE backup_jobs
                   SET processed_count=MAX(0, processed_count-?),
                       error_count=MAX(0, error_count-?),
                       status='Sẵn sàng retry',
                       last_error=''
                   WHERE id=?""",
                (len(selected), len(selected), job_id),
            )
            conn.commit()
        return len(selected)


async def scan_job_queue(job_id: int, job: dict[str, Any], client):
    queue_clear(job_id)
    update_job(job_id, scan_complete=0, scanned_total=0, status="Đang scan nguồn 🔎", last_error="")
    update_runtime(job_id, state="Đang scan", action="scan", scan_count=0, scan_pct=0)
    record_job_log(job_id, "info", "scan", "Bắt đầu scan toàn bộ nguồn")

    insert_rows = []
    seq = 0
    async for msg in iterate_job_messages(client, job, from_last_processed=False):
        if not backup_flags.get(job_id):
            raise asyncio.CancelledError("paused")
        if not include_media(msg, job.get("media_filter") or "all"):
            continue
        seq += 1
        grouped = int(getattr(msg, "grouped_id", 0) or 0)
        now_ts = int(time.time())
        insert_rows.append((job_id, seq, msg.id, media_kind(msg), grouped, "pending", "pending", 0, now_ts, now_ts, 0, "", now_ts, now_ts))
        if seq % 100 == 0:
            update_runtime(job_id, scan_count=seq)
            record_job_log(job_id, "info", "scan", f"Đã scan {seq} item")

    with closing(get_db_connection()) as conn:
        conn.executemany(
            """INSERT OR IGNORE INTO backup_job_queue
               (job_id, seq, source_message_id, media_kind, grouped_id, state, sub_state, sub_progress, phase_started_at, last_progress_at, attempt_count, last_error, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            insert_rows,
        )
        conn.commit()

    update_job(job_id, scan_complete=1, scanned_total=seq, status=f"Scan xong {seq} item")
    update_runtime(job_id, scan_count=seq, scan_pct=100, state=f"Scan xong {seq} item", action="scan_done")
    record_job_log(job_id, "info", "scan", f"Scan hoàn tất, tổng {seq} item")
    return seq


def classify_error(err: Exception) -> tuple[str, str]:
    raw = normalize_short_error(err)
    cls_name = err.__class__.__name__ if err else "UnknownError"
    text = raw if raw else cls_name
    if raw and cls_name.lower() not in raw.lower():
        text = f"{cls_name}: {raw}"[:MAX_LAST_ERROR]
    lower = text.lower()
    if "ffmpeg" in lower or "ffprobe" in lower or "video_" in lower or "media_" in lower:
        return "pipeline", text
    if "flood" in lower or "rpc" in lower or "telegram" in lower or "upload_retry_exhausted" in lower:
        return "telegram", text
    if "download" in lower or "network" in lower or "timed out" in lower or "timeout" in lower:
        return "source", text
    return "unknown", text


def update_runtime(job_id: int, **fields):
    runtime = backup_runtime.setdefault(job_id, {})
    runtime.update(fields)


def runtime_track_item(job_id: int, msg_id: int, phase: str, add: bool):
    runtime = backup_runtime.setdefault(job_id, {})
    key = "active_download_ids" if phase == "download" else "active_upload_ids"
    cur = runtime.get(key) or []
    cur_set = {int(x) for x in cur}
    if add:
        cur_set.add(int(msg_id))
    else:
        cur_set.discard(int(msg_id))
    runtime[key] = sorted(cur_set)


def runtime_update_speed(job_id: int, msg_id: int, phase: str, cur: int, start_ts: float):
    runtime = backup_runtime.setdefault(job_id, {})
    elapsed = max(time.time() - float(start_ts or time.time()), 0.001)
    mbps = round((float(cur or 0) / elapsed) / (1024 * 1024), 2)
    if phase == "download":
        map_key, out_key, bytes_key = "_dl_speed_map", "download_speed_mbps", "download_bytes"
    else:
        map_key, out_key, bytes_key = "_ul_speed_map", "upload_speed_mbps", "upload_bytes"
    speed_map = runtime.get(map_key) or {}
    speed_map[str(int(msg_id or 0))] = mbps
    runtime[map_key] = speed_map
    runtime[out_key] = round(sum(float(v or 0) for v in speed_map.values()), 2)
    runtime[bytes_key] = int(cur or 0)


def runtime_clear_speed(job_id: int, msg_id: int, phase: str):
    runtime = backup_runtime.setdefault(job_id, {})
    if phase == "download":
        map_key, out_key = "_dl_speed_map", "download_speed_mbps"
    else:
        map_key, out_key = "_ul_speed_map", "upload_speed_mbps"
    speed_map = runtime.get(map_key) or {}
    speed_map.pop(str(int(msg_id or 0)), None)
    runtime[map_key] = speed_map
    runtime[out_key] = round(sum(float(v or 0) for v in speed_map.values()), 2)


async def run_backup(job_id: int):
    backup_flags[job_id] = True
    backup_runtime[job_id] = {
        "state": "Khởi động",
        "current_message_id": 0,
        "download_pct": 0,
        "upload_pct": 0,
        "scan_pct": 0,
        "scan_count": 0,
        "active_download_ids": [],
        "active_upload_ids": [],
        "download_speed_mbps": 0.0,
        "upload_speed_mbps": 0.0,
        "download_bytes": 0,
        "upload_bytes": 0,
        "action": "idle",
    }
    update_job(job_id, status="Đang chạy", last_error="")
    record_job_log(job_id, "info", "run", "Job bắt đầu chạy")
    stale_removed, stale_freed = cleanup_stale_tmp_files()
    free_mb_start = round(tmp_dir_free_bytes() / (1024 * 1024), 1)
    if stale_removed > 0:
        record_job_log(
            job_id,
            "info",
            "cleanup",
            f"Tự dọn tmp trước khi chạy: removed={stale_removed} freed_mb={round(stale_freed/(1024*1024),2)}",
        )
    record_job_log(job_id, "info", "disk", f"Dung lượng trống lúc start: free_mb={free_mb_start}")

    try:
        job = fetch_job(job_id)
        if not job:
            return

        client = await get_tg_client()
        source = await get_safe_entity(client, job["source_chat"])
        target_id, target_topic = parse_target(job["target_link"])
        target = await get_safe_entity(client, target_id)
        record_job_log(job_id, "info", "resolve", f"Resolve entity OK source={job['source_chat']} target={target_id}")

        batch_limit = clamp(job.get("batch_limit"), 50, 1, 1000)
        delay_min = clamp(job.get("delay_min"), 1.0, 0.0, 600.0)
        delay_max = clamp(job.get("delay_max"), 3.0, 0.0, 600.0)
        if delay_max < delay_min:
            delay_max = delay_min

        fast_download = bool(job.get("fast_download", 1))
        upload_retry_max = clamp(job.get("upload_retry_max"), DEFAULT_UPLOAD_RETRIES, 1, 8)

        processed_count = int(job.get("processed_count") or 0)
        uploaded_count = int(job.get("uploaded_count") or 0)
        skipped_count = int(job.get("skipped_count") or 0)
        error_count = int(job.get("error_count") or 0)

        if not int(job.get("scan_complete") or 0):
            await scan_job_queue(job_id, job, client)
        else:
            record_job_log(job_id, "info", "scan", f"Dùng queue đã scan sẵn total={int(job.get('scanned_total') or 0)}")

        queue_recover_in_progress(job_id)
        auto_retried = queue_requeue_errors(job_id, retryable_only=True, max_attempt_count=3)
        if auto_retried > 0:
            record_job_log(job_id, "info", "retry", f"Auto retry queue_error -> pending: {auto_retried} item")
        dl_workers, up_workers, auto_meta = auto_tune_workers(job)
        record_job_log(
            job_id,
            "info",
            "workers",
            "Auto workers "
            f"free_mb={auto_meta['free_mb']} net_mbps={auto_meta['net_mbps']} "
            f"cfg_dl={auto_meta['configured_dl']} cfg_ul={auto_meta['configured_ul']} "
            f"effective_dl={dl_workers} effective_ul={up_workers}",
        )
        download_sem = asyncio.Semaphore(dl_workers)
        upload_sem = asyncio.Semaphore(up_workers)
        state_lock = asyncio.Lock()
        stop_event = asyncio.Event()

        with closing(get_db_connection()) as conn:
            rows = conn.execute(
                "SELECT source_message_id, grouped_id, seq FROM backup_job_queue WHERE job_id=? AND state='pending' ORDER BY seq ASC LIMIT ?",
                (job_id, batch_limit),
            ).fetchall()
        pending_rows = [{"msg_id": int(r[0]), "grouped_id": int(r[1] or 0), "seq": int(r[2])} for r in rows]
        if not pending_rows:
            record_job_log(job_id, "info", "run", "Không còn pending item trong queue")
        else:
            record_job_log(job_id, "info", "run", f"Bắt đầu process {len(pending_rows)} item pending")

        group_map = {}
        for r in pending_rows:
            gid = r["grouped_id"]
            if gid > 0:
                group_map.setdefault(gid, []).append(r)
        units = []
        consumed = set()
        for r in pending_rows:
            mid = r["msg_id"]
            if mid in consumed:
                continue
            gid = r["grouped_id"]
            if gid > 0 and len(group_map.get(gid, [])) > 1:
                unit_ids = [x["msg_id"] for x in sorted(group_map[gid], key=lambda a: a["seq"])]
                for x in unit_ids:
                    consumed.add(x)
                units.append(unit_ids)
            else:
                consumed.add(mid)
                units.append([mid])

        async def process_one(msg_id: int):
            nonlocal processed_count, uploaded_count, skipped_count, error_count
            if stop_event.is_set() or not backup_flags.get(job_id):
                return
            queue_mark(job_id, msg_id, "in_progress", "")
            queue_sub_progress(job_id, msg_id, "fetch", 2)
            record_job_log(job_id, "info", "item", "Bắt đầu xử lý item", msg_id)
            msg = await asyncio.wait_for(client.get_messages(source, ids=msg_id), timeout=DEFAULT_FETCH_TIMEOUT)
            if not msg:
                async with state_lock:
                    processed_count += 1
                    error_count += 1
                queue_mark(job_id, msg_id, "error", "message_not_found")
                record_job_log(job_id, "error", "fetch", "Không lấy được message từ source", msg_id)
                return

            kind = media_kind(msg)
            downloaded = ""
            tmp_files = []
            retries_log = []
            verify_data = {}
            plan_used = ""
            action_used = ""
            probe_input = {}
            try:
                async with download_sem:
                    if stop_event.is_set() or not backup_flags.get(job_id):
                        raise asyncio.CancelledError("paused")
                    runtime_track_item(job_id, msg_id, "download", True)
                    queue_sub_progress(job_id, msg_id, "download", 8)
                    update_runtime(job_id, state=f"Đang tải #{msg_id}", current_message_id=msg_id, action="download")
                    await ensure_disk_budget(job_id, msg_id, estimate_tmp_need_bytes(msg, kind))
                    ext = (getattr(getattr(msg, "file", None), "ext", None) or "bin").lstrip(".")
                    downloaded = path_for(job_id, msg_id, f"src.{ext}")
                    tmp_files.append(downloaded)
                    downloaded = await download_media_with_fallback(job_id, client, msg, downloaded, fast_download)
                    if not downloaded or not os.path.exists(downloaded) or os.path.getsize(downloaded) == 0:
                        raise RuntimeError("download_empty_or_failed")
                    queue_sub_progress(job_id, msg_id, "download", 42)

                    send_file_path = downloaded
                    send_kwargs = {"force_document": False}
                    if kind == "video":
                        queue_sub_progress(job_id, msg_id, "prepare_video", 56)
                        prepared = await prepare_video(downloaded, job_id, msg_id)
                        probe_input = prepared.get("probe_input", {})
                        if not prepared.get("ok"):
                            raise RuntimeError(f"video_plan_all_failed:{prepared.get('failures')}")
                        send_file_path = prepared["file"]
                        tmp_files.append(send_file_path)
                        if prepared.get("thumb"):
                            tmp_files.append(prepared["thumb"])
                        plan_used = prepared["plan"]
                        action_used = prepared["action"]
                        send_kwargs = {
                            "force_document": False,
                            "supports_streaming": True,
                            "thumb": prepared.get("thumb"),
                            "attributes": prepared["attributes"],
                        }
                    queue_sub_progress(job_id, msg_id, "ready_upload", 64)
                    runtime_track_item(job_id, msg_id, "download", False)

                async with upload_sem:
                    if stop_event.is_set() or not backup_flags.get(job_id):
                        raise asyncio.CancelledError("paused")
                    runtime_track_item(job_id, msg_id, "upload", True)
                    queue_sub_progress(job_id, msg_id, "upload", 76)
                    queue_attempt_inc(job_id, msg_id)
                    update_runtime(job_id, state=f"Đang upload #{msg_id}", current_message_id=msg_id, action="upload")
                    send_params = {
                        "file": send_file_path,
                        "caption": caption_for(job, msg),
                        "reply_to": target_topic,
                        "_progress_msg_id": msg_id,
                        **send_kwargs,
                    }
                    sent_message, retries_log = await upload_with_retry(job_id, client, target, send_params, upload_retry_max)
                    update_runtime(job_id, upload_pct=100)
                    queue_sub_progress(job_id, msg_id, "verify", 90)
                    if kind == "video":
                        verify_data = verify_uploaded_video(sent_message)
                        if not verify_ok(verify_data):
                            raise RuntimeError(f"verify_failed:{verify_data}")

                async with state_lock:
                    processed_count += 1
                    uploaded_count += 1
                    queue_mark(job_id, msg_id, "done", "")
                    queue_sub_progress(job_id, msg_id, "done", 100)
                    update_job(
                        job_id,
                        last_processed_id=msg_id,
                        processed_count=processed_count,
                        uploaded_count=uploaded_count,
                        skipped_count=skipped_count,
                        error_count=error_count,
                        status=f"Đang chạy song song {processed_count}/{batch_limit}",
                        last_error="",
                    )
                record_telemetry(
                    job_id, msg_id, kind, plan_selected=plan_used, action_used=action_used,
                    probe_input=probe_input, retries=retries_log, verify=verify_data, result="ok"
                )
            except asyncio.CancelledError:
                queue_mark(job_id, msg_id, "pending", "paused")
                queue_sub_progress(job_id, msg_id, "pending", 0)
                raise
            except Exception as e:
                error_class, reason = classify_error(e)
                tb_line = normalize_short_error(traceback.format_exc().splitlines()[-1] if traceback.format_exc() else "")
                detail = f"{error_class}: {reason} | {tb_line}" if tb_line else f"{error_class}: {reason}"
                async with state_lock:
                    processed_count += 1
                    error_count += 1
                queue_mark(job_id, msg_id, "error", f"{error_class}: {reason}")
                queue_sub_progress(job_id, msg_id, "error", 100)
                record_telemetry(
                    job_id, msg_id, kind, plan_selected=plan_used, action_used=action_used,
                    probe_input=probe_input, retries=retries_log, verify=verify_data, result="failed",
                    error_class=error_class, error_reason=reason
                )
                record_job_log(job_id, "error", "item", detail, msg_id)
                if bool(job.get("stop_on_error")):
                    stop_event.set()
            finally:
                for p in tmp_files:
                    if p and os.path.exists(p):
                        try:
                            os.remove(p)
                        except Exception:
                            pass
                runtime_track_item(job_id, msg_id, "download", False)
                runtime_track_item(job_id, msg_id, "upload", False)
                if delay_max > 0:
                    await asyncio.sleep(random.uniform(delay_min, delay_max))

        async def process_group(msg_ids: list[int]):
            nonlocal processed_count, uploaded_count, error_count
            if len(msg_ids) == 1:
                await process_one(msg_ids[0])
                return
            # Process album as one upload unit to preserve grouped media & captions.
            msgs = []
            fetch_failed_ids: list[int] = []
            for mid in msg_ids:
                queue_mark(job_id, mid, "in_progress", "")
                queue_sub_progress(job_id, mid, "fetch", 2)
                try:
                    m = await asyncio.wait_for(client.get_messages(source, ids=mid), timeout=DEFAULT_FETCH_TIMEOUT)
                    if not m:
                        raise RuntimeError("message_not_found")
                    msgs.append(m)
                except Exception as e:
                    fetch_failed_ids.append(mid)
                    error_class, reason = classify_error(e)
                    queue_mark(job_id, mid, "error", f"{error_class}: {reason}")
                    queue_sub_progress(job_id, mid, "error", 100)
                    record_telemetry(
                        job_id, mid, "unknown", result="failed", error_class=error_class, error_reason=reason
                    )
                    record_job_log(job_id, "error", "fetch", f"{error_class}: {reason}", mid)
                    async with state_lock:
                        processed_count += 1
                        error_count += 1
            if len(msgs) < 2:
                for m in msgs:
                    await process_one(m.id)
                return
            # Safety-first for Telegram video playback quality:
            # group send may drop per-item video attrs/thumb/streaming metadata.
            # For any album containing video, process each item individually.
            if any(media_kind(m) == "video" for m in msgs):
                record_job_log(
                    job_id,
                    "info",
                    "group",
                    f"group_contains_video_fallback_single count={len(msgs)}",
                )
                for m in msgs:
                    await process_one(int(m.id))
                return

            tmp_files = []
            upload_files = []
            captions = []
            per_meta = []
            any_video = False
            try:
                for m in msgs:
                    mid = int(m.id)
                    kind = media_kind(m)
                    any_video = any_video or kind == "video"
                    runtime_track_item(job_id, mid, "download", True)
                    queue_sub_progress(job_id, mid, "download", 8)
                    await ensure_disk_budget(job_id, mid, estimate_tmp_need_bytes(m, kind))
                    ext = (getattr(getattr(m, "file", None), "ext", None) or "bin").lstrip(".")
                    src_path = path_for(job_id, mid, f"src.{ext}")
                    tmp_files.append(src_path)
                    downloaded = await download_media_with_fallback(job_id, client, m, src_path, fast_download)
                    if not downloaded or not os.path.exists(downloaded) or os.path.getsize(downloaded) == 0:
                        raise RuntimeError(f"download_empty_or_failed:{mid}")
                    queue_sub_progress(job_id, mid, "download", 42)

                    send_file_path = downloaded
                    if kind == "video":
                        queue_sub_progress(job_id, mid, "prepare_video", 56)
                        prepared = await prepare_video(downloaded, job_id, mid)
                        if not prepared.get("ok"):
                            raise RuntimeError(f"video_plan_all_failed:{mid}")
                        send_file_path = prepared["file"]
                        tmp_files.append(send_file_path)
                        if prepared.get("thumb"):
                            tmp_files.append(prepared["thumb"])
                    queue_sub_progress(job_id, mid, "ready_upload", 64)
                    runtime_track_item(job_id, mid, "download", False)
                    upload_files.append(send_file_path)
                    captions.append(caption_for(job, m))
                    per_meta.append((mid, kind))

                async with upload_sem:
                    for mid, _ in per_meta:
                        runtime_track_item(job_id, mid, "upload", True)
                        queue_sub_progress(job_id, mid, "upload", 76)
                        queue_attempt_inc(job_id, mid)
                    album_caption = captions if any(captions) else None
                    send_params = {
                        "file": upload_files,
                        "caption": album_caption,
                        "reply_to": target_topic,
                        "force_document": False,
                        "supports_streaming": any_video,
                        "_progress_msg_id": int(per_meta[0][0]) if per_meta else 0,
                    }
                    await upload_with_retry(job_id, client, target, send_params, upload_retry_max)
                    for mid, kind in per_meta:
                        queue_sub_progress(job_id, mid, "verify", 90 if kind == "video" else 100)

                async with state_lock:
                    for mid, kind in per_meta:
                        processed_count += 1
                        uploaded_count += 1
                        queue_mark(job_id, mid, "done", "")
                        queue_sub_progress(job_id, mid, "done", 100)
                        record_telemetry(job_id, mid, kind, result="ok")
                    update_job(
                        job_id,
                        last_processed_id=max(msg_ids),
                        processed_count=processed_count,
                        uploaded_count=uploaded_count,
                        skipped_count=skipped_count,
                        error_count=error_count,
                        status=f"Đang chạy song song {processed_count}/{batch_limit}",
                        last_error="",
                    )
            except Exception as e:
                error_class, reason = classify_error(e)
                tb_line = normalize_short_error(traceback.format_exc().splitlines()[-1] if traceback.format_exc() else "")
                failed_ids = [int(x.id) for x in msgs]
                async with state_lock:
                    for mid, kind in per_meta:
                        processed_count += 1
                        error_count += 1
                        queue_mark(job_id, mid, "error", f"{error_class}: {reason}")
                        queue_sub_progress(job_id, mid, "error", 100)
                        record_telemetry(job_id, mid, kind, result="failed", error_class=error_class, error_reason=reason)
                        record_job_log(job_id, "error", "group_item", f"{error_class}: {reason} | {tb_line}", mid)
                if not per_meta:
                    record_job_log(job_id, "error", "group", f"group_failed_no_meta: {error_class}: {reason} | {tb_line}")
                # Fallback: avoid losing whole album on a transient group-send failure.
                # Reprocess remaining items individually with full pipeline.
                fallback_ids = [mid for mid in failed_ids if mid not in fetch_failed_ids]
                if fallback_ids:
                    record_job_log(
                        job_id,
                        "warn",
                        "group",
                        f"group_failed_fallback_single count={len(fallback_ids)} reason={error_class}:{reason}",
                    )
                    for mid in fallback_ids:
                        # Skip items already marked by per_meta error path.
                        if any(x[0] == mid for x in per_meta):
                            continue
                        queue_mark(job_id, mid, "pending", "group_fallback_single")
                        queue_sub_progress(job_id, mid, "pending", 0)
                        await process_one(mid)
                if bool(job.get("stop_on_error")):
                    stop_event.set()
            finally:
                for mid in msg_ids:
                    runtime_track_item(job_id, mid, "download", False)
                    runtime_track_item(job_id, mid, "upload", False)
                for p in tmp_files:
                    if p and os.path.exists(p):
                        try:
                            os.remove(p)
                        except Exception:
                            pass

        tasks = [asyncio.create_task(process_group(unit)) for unit in units]
        if tasks:
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                update_job(job_id, status="Đã tạm dừng")
                record_job_log(job_id, "warn", "run", "Job bị pause bởi user")
                return

        stats = queue_stats(job_id)
        if stats["pending"] == 0:
            final_status = f"Hoàn thành done={stats['done']} error={stats['error']}"
        else:
            final_status = f"Hoàn thành lượt pending={stats['pending']}"
        update_job(job_id, status=final_status)
        record_job_log(job_id, "info", "run", final_status)
        update_runtime(job_id, state="Chờ lệnh", action="idle", download_pct=0, upload_pct=0)
    except Exception as e:
        print(traceback.format_exc())
        update_job(job_id, status="Lỗi runtime", last_error=normalize_short_error(e))
        update_runtime(job_id, state="Lỗi", action="failed")
        record_job_log(job_id, "error", "runtime", normalize_short_error(e))
    finally:
        backup_flags[job_id] = False
        stale_removed, stale_freed = cleanup_stale_tmp_files()
        if stale_removed > 0:
            record_job_log(
                job_id,
                "info",
                "cleanup",
                f"Tự dọn tmp sau khi chạy: removed={stale_removed} freed_mb={round(stale_freed/(1024*1024),2)}",
            )
        record_job_log(job_id, "info", "run", "Job kết thúc")


async def collect_preview(job, limit=30):
    client = await get_tg_client()
    items = []
    async for msg in iterate_job_messages(client, job, from_last_processed=False):
        if include_media(msg, job.get("media_filter") or "all"):
            items.append({
                "id": msg.id,
                "kind": media_kind(msg),
                "caption": (msg.text or "")[:120],
            })
        if len(items) >= limit:
            break
    return items


def is_any_job_running() -> bool:
    return any(bool(v) for v in backup_flags.values())


async def run_job_queue():
    global backup_queue_running, backup_queue_stop, backup_queue_current
    backup_queue_running = True
    backup_queue_stop = False
    backup_queue_current = None
    try:
        while backup_queue and not backup_queue_stop:
            job_id = backup_queue.pop(0)
            job = fetch_job(job_id)
            if not job:
                continue
            backup_queue_current = job_id
            await run_backup(job_id)
            await asyncio.sleep(0.2)
    finally:
        backup_queue_running = False
        backup_queue_current = None
        backup_queue_stop = False


@router.get("/jobs")
async def get_jobs():
    with closing(get_db_connection()) as conn:
        conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        return conn.execute("SELECT * FROM backup_jobs ORDER BY id DESC").fetchall()


@router.get("/runtime")
async def get_runtime():
    return backup_runtime


@router.get("/debug/whoami")
async def debug_whoami():
    client = await get_tg_client()
    me = await client.get_me()
    return {
        "id": me.id,
        "username": me.username,
        "phone": me.phone,
        "first_name": me.first_name,
        "last_name": me.last_name,
    }


@router.get("/debug/check_access")
async def debug_check_access(link: str):
    raw = (link or "").strip()
    if not raw:
        return {"ok": False, "error_class": "bad_input", "error": "Thiếu query param link"}

    client = await get_tg_client()
    me = await client.get_me()

    src_chat, src_topic, src_msg_id = parse_source_link(raw)
    tgt_chat, tgt_topic = parse_target(raw)

    probe = {
        "link": raw,
        "account": {
            "id": me.id,
            "username": me.username,
            "phone": me.phone,
        },
        "parsed": {
            "source_chat": src_chat,
            "source_topic_id": src_topic,
            "source_message_id": src_msg_id,
            "target_chat": tgt_chat,
            "target_topic_id": tgt_topic,
        },
    }

    entity_id = src_chat or tgt_chat
    if not entity_id:
        return {
            "ok": False,
            "error_class": "bad_link",
            "error": "Không parse được link Telegram hợp lệ",
            **probe,
        }

    try:
        entity = await get_safe_entity(client, entity_id)
    except Exception as e:
        msg = normalize_short_error(e)
        return {
            "ok": False,
            "error_class": "entity_access_denied",
            "error": msg,
            **probe,
        }

    result = {
        "ok": True,
        "entity": {
            "id": getattr(entity, "id", None),
            "title": getattr(entity, "title", None),
            "username": getattr(entity, "username", None),
        },
        "message_check": None,
        **probe,
    }

    if src_msg_id:
        try:
            msg = await client.get_messages(entity, ids=src_msg_id)
            if not msg:
                return {
                    "ok": False,
                    "error_class": "message_not_found",
                    "error": f"Không lấy được message id={src_msg_id}",
                    **result,
                }

            top_id = getattr(msg.reply_to, "reply_to_top_id", None) or getattr(msg.reply_to, "reply_to_msg_id", None)
            topic_ok = True
            if src_topic:
                topic_ok = (top_id == src_topic) or (msg.id == src_topic)

            result["message_check"] = {
                "id": msg.id,
                "has_media": bool(getattr(msg, "media", None)),
                "topic_id_expected": src_topic,
                "topic_id_actual": top_id,
                "topic_match": topic_ok,
            }
            if not topic_ok:
                return {
                    "ok": False,
                    "error_class": "topic_mismatch",
                    "error": f"Message {msg.id} không thuộc topic {src_topic}",
                    **result,
                }
        except Exception as e:
            return {
                "ok": False,
                "error_class": "message_access_denied",
                "error": normalize_short_error(e),
                **result,
            }

    return result


@router.get("/queue")
async def get_queue_state():
    return {
        "running": backup_queue_running,
        "current_job_id": backup_queue_current,
        "queued_job_ids": backup_queue,
        "queued_count": len(backup_queue),
    }


@router.post("/add")
async def add_job(req: BackupReq):
    src_chat, src_topic, start_id = parse_source_link(req.start_link)
    if not src_chat or not start_id:
        return {"error": "Link bắt đầu sai định dạng"}

    end_chat, end_topic, end_id = parse_source_link(req.end_link) if req.end_link else (src_chat, src_topic, start_id)
    if not end_id:
        end_id = start_id
    if end_chat != src_chat:
        return {"error": "Start/End phải cùng source chat"}
    if src_topic and end_topic and src_topic != end_topic:
        return {"error": "Start/End phải cùng topic"}

    target_chat, target_topic = parse_target(req.target_link)
    if not target_chat:
        return {"error": "Link đích sai định dạng"}

    is_forward = start_id <= end_id
    last_processed_id = start_id - 1 if is_forward else start_id + 1

    with closing(get_db_connection()) as conn:
        conn.execute(
            '''INSERT INTO backup_jobs (
                name, source_chat, source_topic_id, target_link, target_topic_id,
                start_id, end_id, last_processed_id, media_filter, caption_mode, caption,
                batch_limit, delay_min, delay_max, stop_on_error, fast_download, upload_retry_max, download_workers, upload_workers
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                req.name,
                str(src_chat),
                src_topic,
                req.target_link,
                target_topic,
                start_id,
                end_id,
                last_processed_id,
                req.media_filter,
                req.caption_mode,
                req.caption,
                clamp(req.batch_limit, 50, 1, 1000),
                clamp(req.delay_min, 1.0, 0.0, 600.0),
                clamp(req.delay_max, 3.0, 0.0, 600.0),
                int(bool(req.stop_on_error)),
                int(bool(req.fast_download)),
                clamp(req.upload_retry_max, DEFAULT_UPLOAD_RETRIES, 1, 8),
                clamp(req.download_workers, 4, 1, 8),
                clamp(req.upload_workers, 1, 1, 2),
            ),
        )
        conn.commit()
    return {"ok": True}


@router.get("/preview/{job_id}")
async def preview_job(job_id: int):
    job = fetch_job(job_id)
    if not job:
        return {"error": "Không tìm thấy job"}
    items = await collect_preview(job)
    return {"items": items, "count": len(items)}


@router.post("/run/{job_id}")
async def run_job(job_id: int, bg: BackgroundTasks):
    if backup_queue_running:
        return {"error": "Queue đang chạy, không thể chạy lẻ song song"}
    if is_any_job_running():
        return {"error": "Đang có job khác chạy, hãy pause hoặc dùng queue"}
    if backup_flags.get(job_id):
        return {"error": "Job đang chạy"}
    bg.add_task(run_backup, job_id)
    return {"ok": True}


@router.post("/resume/{job_id}")
async def resume_job(job_id: int, bg: BackgroundTasks):
    if backup_queue_running:
        return {"error": "Queue đang chạy, không thể resume lẻ song song"}
    if is_any_job_running():
        return {"error": "Đang có job khác chạy, hãy pause hoặc dùng queue"}
    if backup_flags.get(job_id):
        return {"error": "Job đang chạy"}
    bg.add_task(run_backup, job_id)
    return {"ok": True}


class QueueReq(BaseModel):
    job_ids: list[int]


class JobSettingsReq(BaseModel):
    batch_limit: int | None = None
    delay_min: float | None = None
    delay_max: float | None = None
    stop_on_error: bool | None = None
    fast_download: bool | None = None
    upload_retry_max: int | None = None
    download_workers: int | None = None
    upload_workers: int | None = None


@router.post("/queue/start")
async def start_queue(req: QueueReq, bg: BackgroundTasks):
    global backup_queue_running, backup_queue_stop
    if backup_queue_running:
        return {"error": "Queue đang chạy"}
    if is_any_job_running():
        return {"error": "Đang có job chạy thủ công, hãy pause trước"}

    deduped = []
    seen = set()
    for x in req.job_ids or []:
        try:
            jid = int(x)
        except Exception:
            continue
        if jid in seen:
            continue
        seen.add(jid)
        if fetch_job(jid):
            deduped.append(jid)
    if not deduped:
        return {"error": "Không có job hợp lệ để chạy queue"}

    backup_queue.clear()
    backup_queue.extend(deduped)
    backup_queue_stop = False
    bg.add_task(run_job_queue)
    return {"ok": True, "queued_count": len(backup_queue)}


@router.post("/queue/stop")
async def stop_queue():
    global backup_queue_stop
    backup_queue_stop = True
    if backup_queue_current:
        backup_flags[backup_queue_current] = False
    backup_queue.clear()
    return {"ok": True}


@router.post("/pause/{job_id}")
async def pause_job(job_id: int):
    backup_flags[job_id] = False
    update_job(job_id, status="Đã tạm dừng")
    return {"ok": True}


@router.post("/retry_errors/{job_id}")
async def retry_errors(job_id: int, retryable_only: bool = True):
    job = fetch_job(job_id)
    if not job:
        return {"error": "Không tìm thấy job"}
    if backup_flags.get(job_id):
        return {"error": "Job đang chạy, hãy pause trước khi retry lỗi"}
    retried = queue_requeue_errors(job_id, retryable_only=retryable_only, max_attempt_count=5)
    mode = "retryable" if retryable_only else "all"
    record_job_log(job_id, "info", "retry", f"Manual retry errors mode={mode} count={retried}")
    return {"ok": True, "retried": retried, "mode": mode}


@router.post("/update/{job_id}")
async def update_job_settings(job_id: int, req: JobSettingsReq):
    job = fetch_job(job_id)
    if not job:
        return {"error": "Không tìm thấy job"}
    if backup_flags.get(job_id):
        return {"error": "Job đang chạy, hãy pause trước khi đổi settings"}

    fields = {}
    if req.batch_limit is not None:
        fields["batch_limit"] = clamp(req.batch_limit, 50, 1, 1000)
    if req.delay_min is not None:
        fields["delay_min"] = clamp(req.delay_min, 1.0, 0.0, 600.0)
    if req.delay_max is not None:
        fields["delay_max"] = clamp(req.delay_max, 3.0, 0.0, 600.0)
    if "delay_min" in fields and "delay_max" in fields and fields["delay_max"] < fields["delay_min"]:
        fields["delay_max"] = fields["delay_min"]
    if req.stop_on_error is not None:
        fields["stop_on_error"] = int(bool(req.stop_on_error))
    if req.fast_download is not None:
        fields["fast_download"] = int(bool(req.fast_download))
    if req.upload_retry_max is not None:
        fields["upload_retry_max"] = clamp(req.upload_retry_max, DEFAULT_UPLOAD_RETRIES, 1, 8)
    if req.download_workers is not None:
        fields["download_workers"] = clamp(req.download_workers, 4, 1, 8)
    if req.upload_workers is not None:
        fields["upload_workers"] = clamp(req.upload_workers, 1, 1, 2)

    if not fields:
        return {"error": "Không có field nào để cập nhật"}
    update_job(job_id, **fields)
    record_job_log(job_id, "info", "settings", f"Cập nhật settings: {','.join(fields.keys())}")
    return {"ok": True}


@router.post("/reset/{job_id}")
async def reset_job(job_id: int):
    with closing(get_db_connection()) as conn:
        row = conn.execute("SELECT start_id, end_id FROM backup_jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            return {"error": "Không tìm thấy job"}
        start_id, end_id = row
        last_processed_id = start_id - 1 if start_id <= end_id else start_id + 1
        conn.execute(
            """UPDATE backup_jobs
               SET last_processed_id=?, processed_count=0, uploaded_count=0, skipped_count=0,
                   error_count=0, scan_complete=0, scanned_total=0, status='Sẵn sàng', last_error=''
               WHERE id=?""",
            (last_processed_id, job_id),
        )
        conn.execute("DELETE FROM backup_file_telemetry WHERE job_id=?", (job_id,))
        conn.execute("DELETE FROM backup_job_queue WHERE job_id=?", (job_id,))
        conn.execute("DELETE FROM backup_job_logs WHERE job_id=?", (job_id,))
        conn.commit()
    return {"ok": True}


@router.delete("/delete/{job_id}")
async def delete_job(job_id: int):
    backup_flags[job_id] = False
    with closing(get_db_connection()) as conn:
        conn.execute("DELETE FROM backup_jobs WHERE id=?", (job_id,))
        conn.execute("DELETE FROM backup_file_telemetry WHERE job_id=?", (job_id,))
        conn.execute("DELETE FROM backup_job_queue WHERE job_id=?", (job_id,))
        conn.execute("DELETE FROM backup_job_logs WHERE job_id=?", (job_id,))
        conn.commit()
    return {"ok": True}


@router.post("/cleanup")
async def cleanup_tmp():
    removed = 0
    freed_bytes = 0
    for name in os.listdir(TMP_DIR):
        p = TMP_DIR / name
        if p.is_file():
            try:
                freed_bytes += int(p.stat().st_size or 0)
                p.unlink()
                removed += 1
            except Exception:
                pass
    return {"ok": True, "removed": removed, "freed_mb": round(freed_bytes / (1024 * 1024), 2)}


@router.post("/cleanup/downloads")
async def cleanup_downloads():
    tmp_removed, tmp_freed = cleanup_stale_tmp_files(max_age_sec=0)
    junk_removed, junk_freed = cleanup_download_junk()
    return {
        "ok": True,
        "removed": int(tmp_removed + junk_removed),
        "removed_tmp": int(tmp_removed),
        "removed_junk": int(junk_removed),
        "freed_mb": round((tmp_freed + junk_freed) / (1024 * 1024), 2),
    }


@router.get("/telemetry/{job_id}")
async def telemetry(job_id: int, limit: int = 100):
    lim = clamp(limit, 100, 1, 1000)
    with closing(get_db_connection()) as conn:
        conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        return conn.execute(
            "SELECT * FROM backup_file_telemetry WHERE job_id=? ORDER BY id DESC LIMIT ?",
            (job_id, lim),
        ).fetchall()


@router.get("/logs/{job_id}")
async def logs(job_id: int, limit: int = 200):
    lim = clamp(limit, 200, 1, 1000)
    with closing(get_db_connection()) as conn:
        conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        return conn.execute(
            "SELECT * FROM backup_job_logs WHERE job_id=? ORDER BY id DESC LIMIT ?",
            (job_id, lim),
        ).fetchall()


@router.get("/diagnostics/{job_id}")
async def diagnostics(job_id: int, limit: int = 300):
    lim = clamp(limit, 300, 1, 2000)
    with closing(get_db_connection()) as conn:
        conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        rows = conn.execute(
            """SELECT error_class, error_reason, COUNT(*) AS cnt
               FROM backup_file_telemetry
               WHERE job_id=? AND result='failed'
               GROUP BY error_class, error_reason
               ORDER BY cnt DESC
               LIMIT ?""",
            (job_id, lim),
        ).fetchall()

    total_failed = sum(int(r.get("cnt") or 0) for r in rows)
    by_class = {}
    for r in rows:
        cls = (r.get("error_class") or "unknown").strip() or "unknown"
        by_class[cls] = by_class.get(cls, 0) + int(r.get("cnt") or 0)

    job = fetch_job(job_id) or {}
    suggestions = []
    for cls, cnt in sorted(by_class.items(), key=lambda x: x[1], reverse=True):
        if cls == "source":
            suggestions.append({
                "error_class": cls,
                "priority": 1,
                "title": "Nguồn không ổn định hoặc thiếu quyền",
                "action": f"Kiểm tra account đúng channel, giảm download_workers ({int(job.get('download_workers') or 4)}->2), giữ fast_download ON nhưng tăng retry.",
                "count": cnt,
            })
        elif cls == "telegram":
            suggestions.append({
                "error_class": cls,
                "priority": 1,
                "title": "Telegram throttle / FloodWait",
                "action": f"Giảm upload_workers ({int(job.get('upload_workers') or 1)}->1), tăng delay min/max, chạy queue ít job đồng thời.",
                "count": cnt,
            })
        elif cls == "pipeline":
            suggestions.append({
                "error_class": cls,
                "priority": 1,
                "title": "Lỗi chuẩn hóa video",
                "action": "Giữ 3-tier fallback, kiểm tra ffmpeg/ffprobe trong server, ưu tiên transcode safe cho nguồn lỗi cao.",
                "count": cnt,
            })
        else:
            suggestions.append({
                "error_class": cls,
                "priority": 2,
                "title": "Lỗi chưa phân loại",
                "action": "Mở Events để lấy stack gần nhất, retry với batch nhỏ hơn để khoanh vùng.",
                "count": cnt,
            })

    health = "stable"
    if total_failed > 0:
        health = "degraded"
    if by_class.get("telegram", 0) >= 5 or by_class.get("source", 0) >= 5:
        health = "blocked"

    return {
        "job_id": job_id,
        "health": health,
        "total_failed": total_failed,
        "by_class": by_class,
        "top_reasons": rows,
        "suggestions": suggestions[:6],
    }


@router.get("/metrics/{job_id}")
async def metrics(job_id: int):
    now_ts = int(time.time())
    from_ts = now_ts - 5 * 60
    with closing(get_db_connection()) as conn:
        rows_done = conn.execute(
            """SELECT updated_at
               FROM backup_job_queue
               WHERE job_id=? AND state='done' AND updated_at>=?
               ORDER BY updated_at ASC""",
            (job_id, from_ts),
        ).fetchall()
        row_pending = conn.execute(
            "SELECT COUNT(*) FROM backup_job_queue WHERE job_id=? AND state='pending'",
            (job_id,),
        ).fetchone()
        row_stuck = conn.execute(
            """SELECT COUNT(*) FROM backup_job_queue
               WHERE job_id=? AND state='in_progress' AND (
                    (? - COALESCE(last_progress_at,0)) >
                    CASE LOWER(COALESCE(sub_state,''))
                        WHEN 'fetch' THEN 90
                        WHEN 'download' THEN 120
                        WHEN 'prepare_video' THEN 180
                        WHEN 'ready_upload' THEN 60
                        WHEN 'upload' THEN 90
                        WHEN 'verify' THEN 45
                        ELSE 120
                    END
               )""",
            (job_id, now_ts),
        ).fetchone()

    buckets = []
    done_by_minute = {}
    for r in rows_done:
        ts = int(r[0] or 0)
        if ts <= 0:
            continue
        minute = ts - (ts % 60)
        done_by_minute[minute] = done_by_minute.get(minute, 0) + 1

    for i in range(5):
        start = from_ts + i * 60
        minute = start - (start % 60)
        buckets.append({"minute": minute, "done": int(done_by_minute.get(minute, 0))})

    total_done_5m = sum(x["done"] for x in buckets)
    throughput_per_min = round(total_done_5m / 5.0, 2)
    pending = int((row_pending[0] or 0) if row_pending else 0)
    eta_min = None
    if throughput_per_min > 0:
        eta_min = round(pending / throughput_per_min, 1)
    stuck_count = int((row_stuck[0] or 0) if row_stuck else 0)

    return {
        "job_id": job_id,
        "throughput_per_min": throughput_per_min,
        "pending": pending,
        "eta_min": eta_min,
        "stuck_count": stuck_count,
        "buckets": buckets,
    }


@router.get("/queue_items/{job_id}")
async def queue_items(job_id: int, limit: int = 300):
    lim = clamp(limit, 300, 20, 2000)
    with closing(get_db_connection()) as conn:
        conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        stats_row = conn.execute(
            """SELECT
                SUM(CASE WHEN state='pending' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN state='in_progress' THEN 1 ELSE 0 END) AS in_progress,
                SUM(CASE WHEN state='done' THEN 1 ELSE 0 END) AS done,
                SUM(CASE WHEN state='error' THEN 1 ELSE 0 END) AS error,
                COUNT(*) AS total
            FROM backup_job_queue
            WHERE job_id=?""",
            (job_id,),
        ).fetchone() or {}

        items = conn.execute(
            """SELECT seq, source_message_id, media_kind, grouped_id, state, sub_state, sub_progress, attempt_count, phase_started_at, last_progress_at, last_error, updated_at
               FROM backup_job_queue
               WHERE job_id=?
               ORDER BY
                 CASE state
                    WHEN 'in_progress' THEN 0
                    WHEN 'error' THEN 1
                    WHEN 'pending' THEN 2
                    ELSE 3
                 END,
                 seq ASC
               LIMIT ?""",
            (job_id, lim),
        ).fetchall()

    stats = {
        "pending": int(stats_row.get("pending") or 0),
        "in_progress": int(stats_row.get("in_progress") or 0),
        "done": int(stats_row.get("done") or 0),
        "error": int(stats_row.get("error") or 0),
        "total": int(stats_row.get("total") or 0),
    }
    return {
        "job_id": job_id,
        "stats": stats,
        "items": items,
    }
