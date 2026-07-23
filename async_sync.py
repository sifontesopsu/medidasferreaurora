from __future__ import annotations

import base64
import io
import json
import os
import shutil
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from PIL import Image, ImageOps


def post_json(
    endpoint: str,
    payload: Dict[str, Any],
    timeout: int = 180,
) -> Dict[str, Any]:
    """Send one plain JSON request to the Apps Script API."""
    endpoint = str(endpoint or "").strip()
    if not endpoint:
        raise RuntimeError("Falta APPS_SCRIPT_URL")
    response = requests.post(
        endpoint,
        json=payload,
        timeout=max(20, int(timeout)),
    )
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(str(data.get("error", "Error desconocido en API")))
    return data


class SyncQueueError(RuntimeError):
    """Raised when an asynchronous sync job cannot be queued."""


class DurableSyncQueue:
    """Durable SQLite-backed queue with a single background delivery worker.

    The UI only writes a small local record and returns. The worker performs the
    slow HTTP request and photo processing outside Streamlit's request/rerun.
    """

    def __init__(
        self,
        endpoint: str,
        db_path: str,
        spool_dir: str,
        poll_seconds: float = 0.8,
        request_timeout: int = 180,
        max_attempts: int = 8,
    ) -> None:
        self.endpoint = str(endpoint or "").strip()
        self.db_path = Path(db_path).expanduser().resolve()
        self.spool_dir = Path(spool_dir).expanduser().resolve()
        self.poll_seconds = max(0.25, float(poll_seconds))
        self.request_timeout = max(20, int(request_timeout))
        self.max_attempts = max(1, int(max_attempts))
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._init_storage()
        self._recover_interrupted_jobs()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_storage(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sync_jobs (
                    job_id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    attachments_json TEXT NOT NULL DEFAULT '[]',
                    entity_key TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 8,
                    available_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    claimed_at REAL,
                    completed_at REAL,
                    last_error TEXT NOT NULL DEFAULT '',
                    result_json TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_sync_jobs_ready
                    ON sync_jobs(status, available_at, created_at);
                CREATE INDEX IF NOT EXISTS idx_sync_jobs_entity
                    ON sync_jobs(entity_key, status);
                CREATE TABLE IF NOT EXISTS sync_meta (
                    meta_key TEXT PRIMARY KEY,
                    meta_value TEXT NOT NULL
                );
                INSERT OR IGNORE INTO sync_meta(meta_key, meta_value)
                    VALUES ('revision', '0');
                """
            )

    def _recover_interrupted_jobs(self) -> None:
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE sync_jobs
                   SET status='pending', claimed_at=NULL, available_at=?,
                       updated_at=?, last_error=CASE
                           WHEN last_error='' THEN 'Worker interrumpido; reintento automático'
                           ELSE last_error END
                 WHERE status='processing'
                """,
                (now, now),
            )

    def start(self) -> "DurableSyncQueue":
        if self._worker and self._worker.is_alive():
            return self
        self._stop_event.clear()
        self._worker = threading.Thread(
            target=self._worker_loop,
            name="aurora-sync-worker",
            daemon=True,
        )
        self._worker.start()
        return self

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()

    @property
    def worker_alive(self) -> bool:
        return bool(self._worker and self._worker.is_alive())

    def enqueue(
        self,
        action: str,
        payload: Dict[str, Any],
        *,
        entity_key: str = "",
        attachments: Optional[List[Dict[str, Any]]] = None,
        job_id: Optional[str] = None,
    ) -> str:
        if not self.endpoint:
            raise SyncQueueError("Falta APPS_SCRIPT_URL")
        now = time.time()
        resolved_id = str(job_id or uuid.uuid4())
        body = dict(payload)
        body["action"] = str(action)
        body["request_id"] = resolved_id
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO sync_jobs(
                    job_id, action, payload_json, attachments_json, entity_key,
                    status, attempts, max_attempts, available_at, created_at,
                    updated_at, last_error, result_json
                ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?, '', '')
                """,
                (
                    resolved_id,
                    str(action),
                    json.dumps(body, ensure_ascii=False, separators=(",", ":")),
                    json.dumps(attachments or [], ensure_ascii=False, separators=(",", ":")),
                    str(entity_key or ""),
                    self.max_attempts,
                    now,
                    now,
                    now,
                ),
            )
            conn.execute("COMMIT")
        self._wake_event.set()
        return resolved_id

    def enqueue_with_uploaded_files(
        self,
        action: str,
        payload: Dict[str, Any],
        uploaded_files: Iterable[tuple[str, Any]],
        *,
        entity_key: str = "",
        max_source_mb: float = 15.0,
    ) -> str:
        job_id = str(uuid.uuid4())
        job_dir = self.spool_dir / job_id
        job_dir.mkdir(parents=True, exist_ok=False)
        attachments: List[Dict[str, Any]] = []
        try:
            for photo_type, uploaded_file in uploaded_files:
                if uploaded_file is None:
                    raise SyncQueueError(f"Falta foto {photo_type}")
                raw = uploaded_file.getvalue()
                if not raw:
                    raise SyncQueueError(f"La foto {photo_type} está vacía")
                if len(raw) > int(max_source_mb * 1024 * 1024):
                    raise SyncQueueError(
                        f"La foto {photo_type} supera {max_source_mb:.0f} MB"
                    )
                # PIL.verify reads the structure without decoding the full image.
                with Image.open(io.BytesIO(raw)) as image:
                    image.verify()
                source_name = Path(str(getattr(uploaded_file, "name", photo_type))).name
                suffix = Path(source_name).suffix.lower()
                if suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
                    suffix = ".img"
                destination = job_dir / f"{photo_type}{suffix}"
                destination.write_bytes(raw)
                attachments.append(
                    {
                        "tipo": str(photo_type),
                        "path": str(destination),
                        "source_name": source_name,
                    }
                )
            return self.enqueue(
                action,
                payload,
                entity_key=entity_key,
                attachments=attachments,
                job_id=job_id,
            )
        except Exception:
            shutil.rmtree(job_dir, ignore_errors=True)
            raise

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM sync_jobs WHERE job_id=?", (str(job_id),)
            ).fetchone()
        return dict(row) if row else None

    def stats(self) -> Dict[str, int]:
        out = {"pending": 0, "processing": 0, "done": 0, "error": 0}
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS n FROM sync_jobs GROUP BY status"
            ).fetchall()
        for row in rows:
            out[str(row["status"])] = int(row["n"])
        return out

    def recent_failures(self, limit: int = 10) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT job_id, action, entity_key, attempts, updated_at, last_error
                  FROM sync_jobs
                 WHERE status='error'
                 ORDER BY updated_at DESC
                 LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def retry_failed(self, job_ids: Optional[Iterable[str]] = None) -> int:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if job_ids:
                ids = [str(x) for x in job_ids]
                placeholders = ",".join("?" for _ in ids)
                cursor = conn.execute(
                    f"""
                    UPDATE sync_jobs
                       SET status='pending', attempts=0, available_at=?, updated_at=?,
                           claimed_at=NULL, last_error=''
                     WHERE status='error' AND job_id IN ({placeholders})
                    """,
                    [now, now, *ids],
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE sync_jobs
                       SET status='pending', attempts=0, available_at=?, updated_at=?,
                           claimed_at=NULL, last_error=''
                     WHERE status='error'
                    """,
                    (now, now),
                )
            count = int(cursor.rowcount or 0)
            conn.execute("COMMIT")
        if count:
            self._wake_event.set()
        return count

    def active_entity_keys(self, prefix: str = "") -> set[str]:
        query = "SELECT DISTINCT entity_key FROM sync_jobs WHERE status IN ('pending','processing') AND entity_key<>''"
        params: List[Any] = []
        if prefix:
            query += " AND entity_key LIKE ?"
            params.append(f"{prefix}%")
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return {str(row["entity_key"]) for row in rows}

    def revision(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT meta_value FROM sync_meta WHERE meta_key='revision'"
            ).fetchone()
        try:
            return int(row["meta_value"]) if row else 0
        except (TypeError, ValueError):
            return 0

    def cleanup(self, done_older_than_days: int = 7) -> int:
        cutoff = time.time() - max(1, int(done_older_than_days)) * 86400
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM sync_jobs WHERE status='done' AND completed_at<?",
                (cutoff,),
            )
        return int(cursor.rowcount or 0)

    def _claim_next(self) -> Optional[Dict[str, Any]]:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT * FROM sync_jobs
                 WHERE status='pending' AND available_at<=?
                 ORDER BY created_at ASC
                 LIMIT 1
                """,
                (now,),
            ).fetchone()
            if not row:
                conn.execute("COMMIT")
                return None
            updated = conn.execute(
                """
                UPDATE sync_jobs
                   SET status='processing', claimed_at=?, updated_at=?
                 WHERE job_id=? AND status='pending'
                """,
                (now, now, row["job_id"]),
            )
            conn.execute("COMMIT")
            if int(updated.rowcount or 0) != 1:
                return None
        return dict(row)

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            job = self._claim_next()
            if not job:
                self._wake_event.wait(self.poll_seconds)
                self._wake_event.clear()
                continue
            self._process_job(job)

    def _process_job(self, job: Dict[str, Any]) -> None:
        job_id = str(job["job_id"])
        attempts = int(job.get("attempts", 0)) + 1
        try:
            payload = json.loads(str(job["payload_json"]))
            attachments = json.loads(str(job.get("attachments_json") or "[]"))
            if attachments:
                payload["photos"] = [self._compress_attachment(x) for x in attachments]
            result = self._post_json(payload)
            self._mark_done(job_id, attempts, result)
            self._cleanup_job_files(attachments)
        except Exception as exc:  # worker must never die because of one job
            self._mark_retry_or_error(job_id, attempts, str(exc))

    def _compress_attachment(
        self,
        attachment: Dict[str, Any],
        max_size: int = 960,
        quality: int = 55,
        target_size_kb: int = 450,
        min_quality: int = 42,
        min_size: int = 720,
    ) -> Dict[str, Any]:
        source = Path(str(attachment["path"]))
        raw = source.read_bytes()
        source_size_kb = round(len(raw) / 1024, 1)
        with Image.open(io.BytesIO(raw)) as image:
            image = ImageOps.exif_transpose(image).convert("RGB")
            current_max = max_size
            current_quality = quality
            optimized = b""
            while True:
                working = image.copy()
                working.thumbnail((current_max, current_max))
                buffer = io.BytesIO()
                working.save(
                    buffer,
                    format="JPEG",
                    quality=current_quality,
                    optimize=True,
                    progressive=True,
                )
                optimized = buffer.getvalue()
                if len(optimized) / 1024 <= target_size_kb:
                    break
                if current_quality > min_quality:
                    current_quality = max(min_quality, current_quality - 5)
                    continue
                if current_max > min_size:
                    current_max = max(min_size, current_max - 120)
                    continue
                break
        return {
            "tipo": str(attachment["tipo"]),
            "file_base64": base64.b64encode(optimized).decode("utf-8"),
            "mime_type": "image/jpeg",
            "file_name": f"{Path(str(attachment.get('source_name') or attachment['tipo'])).stem}.jpg",
            "size_kb": round(len(optimized) / 1024, 1),
            "source_size_kb": source_size_kb,
        }

    def _post_json(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return post_json(
                self.endpoint,
                payload,
                timeout=self.request_timeout,
            )
        except RuntimeError as exc:
            message = str(exc)
            # Validation/authorization errors will not heal by retrying.
            permanent_markers = (
                "no autorizado", "rol no permitido", "falta " ,
                "estado no permitido", "transición", "credenciales",
                "usuario no encontrado", "pin inválido",
            )
            if any(marker in message.lower() for marker in permanent_markers):
                raise PermanentSyncError(message) from exc
            raise

    def _mark_done(self, job_id: str, attempts: int, result: Dict[str, Any]) -> None:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE sync_jobs
                   SET status='done', attempts=?, updated_at=?, completed_at=?,
                       claimed_at=NULL, last_error='', result_json=?
                 WHERE job_id=?
                """,
                (
                    attempts,
                    now,
                    now,
                    json.dumps(result, ensure_ascii=False, separators=(",", ":")),
                    job_id,
                ),
            )
            conn.execute(
                """
                UPDATE sync_meta
                   SET meta_value=CAST(CAST(meta_value AS INTEGER)+1 AS TEXT)
                 WHERE meta_key='revision'
                """
            )
            conn.execute("COMMIT")

    def _mark_retry_or_error(self, job_id: str, attempts: int, error: str) -> None:
        now = time.time()
        permanent = error.startswith("PERMANENT: ")
        with self._connect() as conn:
            row = conn.execute(
                "SELECT max_attempts FROM sync_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            max_attempts = int(row["max_attempts"]) if row else self.max_attempts
            if permanent or attempts >= max_attempts:
                status = "error"
                available_at = now
            else:
                status = "pending"
                # 2, 4, 8... seconds, capped at 5 minutes.
                available_at = now + min(300, 2 ** min(attempts, 8))
            conn.execute(
                """
                UPDATE sync_jobs
                   SET status=?, attempts=?, available_at=?, updated_at=?,
                       claimed_at=NULL, last_error=?
                 WHERE job_id=?
                """,
                (status, attempts, available_at, now, error[:2000], job_id),
            )
        if status == "pending":
            self._wake_event.set()

    def _cleanup_job_files(self, attachments: List[Dict[str, Any]]) -> None:
        parents: set[Path] = set()
        for attachment in attachments:
            path = Path(str(attachment.get("path", "")))
            if path:
                parents.add(path.parent)
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
        for parent in parents:
            try:
                parent.rmdir()
            except OSError:
                pass


class PermanentSyncError(RuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(f"PERMANENT: {message}")
