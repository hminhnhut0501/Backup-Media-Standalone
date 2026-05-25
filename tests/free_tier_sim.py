import asyncio
import importlib
import os
import sys
import tempfile
import unittest
from collections import namedtuple
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class FreeTierSimulationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["TELEVAULT_DB_PATH"] = str(Path(self.tmp.name) / "sim.db")
        os.environ["BACKUP_DISK_WAIT_TIMEOUT_SEC"] = "1"
        os.environ["BACKUP_DISK_WAIT_INTERVAL_SEC"] = "1"

        import database
        import mod_backup

        self.database = importlib.reload(database)
        self.mod = importlib.reload(mod_backup)
        with self.database.get_db_connection() as conn:
            self.mod.init_db(conn)
            conn.execute(
                """INSERT INTO backup_jobs (
                    name, source_chat, target_link, start_id, end_id, last_processed_id,
                    media_filter, caption_mode, batch_limit, delay_min, delay_max,
                    upload_retry_max, download_workers, upload_workers,
                    processed_count, error_count, scan_complete, scanned_total
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "free-tier-sim",
                    "src",
                    "target",
                    1,
                    10,
                    0,
                    "all",
                    "source",
                    10,
                    1,
                    3,
                    3,
                    4,
                    1,
                    3,
                    3,
                    1,
                    3,
                ),
            )
            self.job_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            now = 1779700000
            rows = [
                (self.job_id, 1, 6309, "video", 0, "error", "error", 100, now, now, 0, "source: RuntimeError: disk_space_wait_timeout:need_mb=6920.7:free_mb=5735.5", now, now),
                (self.job_id, 2, 6313, "video", 0, "error", "error", 100, now, now, 2, "source: TimeoutError", now, now),
                (self.job_id, 3, 6320, "video", 0, "error", "error", 100, now, now, 5, "source: TimeoutError", now, now),
                (self.job_id, 4, 6400, "video", 0, "done", "done", 100, now, now, 1, "", now, now),
            ]
            conn.executemany(
                """INSERT INTO backup_job_queue (
                    job_id, seq, source_message_id, media_kind, grouped_id, state,
                    sub_state, sub_progress, phase_started_at, last_progress_at,
                    attempt_count, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            conn.commit()

    def tearDown(self):
        self.tmp.cleanup()

    def test_retry_requeues_retryable_errors_and_fixes_counters(self):
        retried = self.mod.queue_requeue_errors(self.job_id, retryable_only=True, max_attempt_count=3)
        self.assertEqual(retried, 2)

        with self.database.get_db_connection() as conn:
            states = conn.execute(
                "SELECT source_message_id, state, sub_state FROM backup_job_queue ORDER BY seq"
            ).fetchall()
            job = conn.execute(
                "SELECT processed_count, error_count, status FROM backup_jobs WHERE id=?",
                (self.job_id,),
            ).fetchone()

        self.assertEqual(states[0], (6309, "pending", "retry_pending"))
        self.assertEqual(states[1], (6313, "pending", "retry_pending"))
        self.assertEqual(states[2][1], "error")
        self.assertEqual(job[0], 1)
        self.assertEqual(job[1], 1)
        self.assertEqual(job[2], "Sẵn sàng retry")

    def test_error_mark_increments_attempt_for_pre_upload_failures(self):
        self.mod.queue_mark(self.job_id, 6309, "error", "source: RuntimeError: disk_space_wait_timeout")
        with self.database.get_db_connection() as conn:
            attempt = conn.execute(
                "SELECT attempt_count FROM backup_job_queue WHERE job_id=? AND source_message_id=?",
                (self.job_id, 6309),
            ).fetchone()[0]
        self.assertEqual(attempt, 1)

    def test_disk_budget_allows_one_to_two_gb_files_on_30gb_free_tier(self):
        Usage = namedtuple("usage", "total used free")
        total = 30 * 1024 * 1024 * 1024
        free = int(5.8 * 1024 * 1024 * 1024)
        required = int(2.0 * 1024 * 1024 * 1024 * 1.7)

        async def run_case():
            await self.mod.ensure_disk_budget(self.job_id, 6309, required)

        with patch.object(self.mod.shutil, "disk_usage", return_value=Usage(total, total - free, free)):
            with patch.object(self.mod, "tmp_dir_free_bytes", return_value=free):
                with patch.object(self.mod, "cleanup_stale_tmp_files", return_value=(0, 0)):
                    with patch.object(self.mod, "cleanup_download_junk", return_value=(0, 0)):
                        asyncio.run(run_case())

    def test_worker_autotune_caps_for_low_disk_google_free_tier(self):
        Usage = namedtuple("usage", "total used free")
        total = 30 * 1024 * 1024 * 1024
        free = int(5.5 * 1024 * 1024 * 1024)
        job = {"download_workers": 4, "upload_workers": 2}

        with patch.object(self.mod.shutil, "disk_usage", return_value=Usage(total, total - free, free)):
            with patch.object(self.mod, "tmp_dir_free_bytes", return_value=free):
                with patch.object(self.mod, "probe_network_mbps", return_value=35.0):
                    dl, ul, meta = self.mod.auto_tune_workers(job)

        self.assertEqual(dl, 2)
        self.assertEqual(ul, 1)
        self.assertEqual(meta["effective_dl"], 2)
        self.assertEqual(meta["effective_ul"], 1)


if __name__ == "__main__":
    unittest.main()
