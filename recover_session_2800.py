"""
One-time data recovery script for:
1. Fix session 2800 (earlysalty March 14 stream) from category stats
2. Reconstruct twitch_stats_tracked entries from category data for that session
3. Close all remaining orphaned sessions (ended_at IS NULL, samples=0)

Run once from the project root:
    python recover_session_2800.py
"""
from __future__ import annotations

import sys
import os

# Make sure bot package is importable
sys.path.insert(0, os.path.dirname(__file__))

from bot import storage  # noqa: E402

SESSION_ID = 2800
STREAMER = "earlysalty"
STREAM_START = "2026-03-14 21:53:00"
STREAM_END_UPPER = "2026-03-15 02:00:00"


def fix_session_2800(c) -> None:
    """Reconstruct session 2800 from category stats data."""
    print(f"[1/3] Fixing session {SESSION_ID} for {STREAMER}...")

    row = c.execute(
        """
        SELECT
            COUNT(*)               AS samples,
            AVG(viewer_count)      AS avg_viewers,
            MAX(viewer_count)      AS peak_viewers,
            MIN(viewer_count)      AS min_viewers,
            MIN(ts_utc)            AS first_seen,
            MAX(ts_utc)            AS last_seen
        FROM twitch_stats_category
        WHERE LOWER(streamer) = ?
          AND ts_utc >= ?
          AND ts_utc <= ?
        """,
        (STREAMER, STREAM_START, STREAM_END_UPPER),
    ).fetchone()

    if not row:
        print("  ERROR: No category stats found for this period.")
        return

    def rv(r, key, idx, default=None):
        if hasattr(r, "keys"):
            try:
                return r[key]
            except Exception:
                return default
        try:
            return r[idx]
        except Exception:
            return default

    samples = int(rv(row, "samples", 0, 0) or 0)
    avg_viewers = float(rv(row, "avg_viewers", 1, 0.0) or 0.0)
    peak_viewers = int(rv(row, "peak_viewers", 2, 0) or 0)
    min_viewers = int(rv(row, "min_viewers", 3, 0) or 0)
    first_seen = rv(row, "first_seen", 4, None)
    last_seen = rv(row, "last_seen", 5, None)

    print(f"  Found {samples} samples, peak={peak_viewers}, avg={avg_viewers:.0f}")
    print(f"  Period: {first_seen} -> {last_seen}")

    if samples == 0:
        print("  WARNING: 0 samples found, skipping session fix.")
        return

    # Get the viewer count at the first sample for start_viewers
    first_row = c.execute(
        """
        SELECT viewer_count FROM twitch_stats_category
        WHERE LOWER(streamer) = ?
          AND ts_utc >= ?
          AND ts_utc <= ?
        ORDER BY ts_utc ASC
        LIMIT 1
        """,
        (STREAMER, STREAM_START, STREAM_END_UPPER),
    ).fetchone()
    start_viewers = int(rv(first_row, "viewer_count", 0, min_viewers) or min_viewers) if first_row else min_viewers

    last_row = c.execute(
        """
        SELECT viewer_count FROM twitch_stats_category
        WHERE LOWER(streamer) = ?
          AND ts_utc >= ?
          AND ts_utc <= ?
        ORDER BY ts_utc DESC
        LIMIT 1
        """,
        (STREAMER, STREAM_START, STREAM_END_UPPER),
    ).fetchone()
    end_viewers = int(rv(last_row, "viewer_count", 0, 0) or 0) if last_row else 0

    # Get session started_at to compute duration
    session_row = c.execute(
        "SELECT started_at FROM twitch_stream_sessions WHERE id = ?",
        (SESSION_ID,),
    ).fetchone()
    started_at = rv(session_row, "started_at", 0, None) if session_row else None

    c.execute(
        """
        UPDATE twitch_stream_sessions
        SET samples            = ?,
            avg_viewers        = ?,
            peak_viewers       = ?,
            end_viewers        = ?,
            start_viewers      = ?,
            had_deadlock_in_session = TRUE,
            ended_at           = ?,
            duration_seconds   = EXTRACT(EPOCH FROM (CAST(? AS TIMESTAMPTZ) - CAST(started_at AS TIMESTAMPTZ)))::INTEGER,
            notes              = 'recovered from category stats'
        WHERE id = ?
        """,
        (
            samples,
            avg_viewers,
            peak_viewers,
            end_viewers,
            start_viewers,
            str(last_seen),
            str(last_seen),
            SESSION_ID,
        ),
    )
    print(f"  Session {SESSION_ID} updated OK.")


def reconstruct_tracked_stats(c) -> None:
    """Copy category entries into twitch_stats_tracked for earlysalty's session."""
    print(f"[2/3] Copying category stats into twitch_stats_tracked for {STREAMER}...")

    cur = c.execute(
        """
        INSERT INTO twitch_stats_tracked (ts_utc, streamer, viewer_count, is_partner, game_name, stream_title, tags)
        SELECT
            tsc.ts_utc,
            tsc.streamer,
            tsc.viewer_count,
            TRUE,
            tsc.game_name,
            tsc.stream_title,
            tsc.tags
        FROM twitch_stats_category tsc
        WHERE LOWER(tsc.streamer) = ?
          AND tsc.ts_utc >= ?
          AND tsc.ts_utc <= ?
          AND NOT EXISTS (
              SELECT 1 FROM twitch_stats_tracked t
              WHERE t.ts_utc = tsc.ts_utc
                AND LOWER(t.streamer) = ?
          )
        """,
        (STREAMER, STREAM_START, STREAM_END_UPPER, STREAMER),
    )
    inserted = cur.rowcount if cur.rowcount is not None else -1
    print(f"  Inserted {inserted} rows into twitch_stats_tracked.")


def close_orphaned_sessions(c) -> None:
    """Close all orphaned sessions (ended_at IS NULL, samples=0)."""
    print("[3/3] Closing orphaned sessions...")

    # First: for sessions with category data, try to reconstruct end time
    orphan_rows = c.execute(
        """
        SELECT id, streamer_login, started_at
        FROM twitch_stream_sessions
        WHERE ended_at IS NULL
          AND samples = 0
        ORDER BY id
        """
    ).fetchall()

    def rv(r, key, idx, default=None):
        if hasattr(r, "keys"):
            try:
                return r[key]
            except Exception:
                return default
        try:
            return r[idx]
        except Exception:
            return default

    print(f"  Found {len(orphan_rows)} orphaned sessions.")

    recovered = 0
    closed_no_data = 0

    for orphan in orphan_rows:
        sid = int(rv(orphan, "id", 0))
        login = str(rv(orphan, "streamer_login", 1, "") or "").lower()
        started_at = rv(orphan, "started_at", 2, None)

        # Skip the one we already fixed
        if sid == SESSION_ID:
            continue

        # Check if there's any category data for this streamer around the session start
        cat_row = c.execute(
            """
            SELECT
                COUNT(*)          AS samples,
                MAX(viewer_count) AS peak_viewers,
                AVG(viewer_count) AS avg_viewers,
                MIN(viewer_count) AS min_viewers,
                MIN(ts_utc)       AS first_seen,
                MAX(ts_utc)       AS last_seen
            FROM twitch_stats_category
            WHERE LOWER(streamer) = ?
              AND ts_utc >= COALESCE(CAST(? AS TIMESTAMPTZ), NOW() - INTERVAL '7 days')
              AND ts_utc <= COALESCE(CAST(? AS TIMESTAMPTZ), NOW()) + INTERVAL '24 hours'
            """,
            (login, str(started_at) if started_at else None, str(started_at) if started_at else None),
        ).fetchone()

        cat_samples = int(rv(cat_row, "samples", 0, 0) or 0)

        if cat_samples > 0:
            cat_last_seen = rv(cat_row, "last_seen", 5, None)
            cat_peak = int(rv(cat_row, "peak_viewers", 1, 0) or 0)
            cat_avg = float(rv(cat_row, "avg_viewers", 2, 0.0) or 0.0)
            cat_min = int(rv(cat_row, "min_viewers", 3, 0) or 0)
            cat_first = rv(cat_row, "first_seen", 4, None)

            first_viewer_row = c.execute(
                """
                SELECT viewer_count FROM twitch_stats_category
                WHERE LOWER(streamer) = ?
                  AND ts_utc = ?
                LIMIT 1
                """,
                (login, str(cat_first) if cat_first else ""),
            ).fetchone()
            start_v = int(rv(first_viewer_row, "viewer_count", 0, cat_min) or cat_min) if first_viewer_row else cat_min

            last_viewer_row = c.execute(
                """
                SELECT viewer_count FROM twitch_stats_category
                WHERE LOWER(streamer) = ?
                  AND ts_utc = ?
                LIMIT 1
                """,
                (login, str(cat_last_seen) if cat_last_seen else ""),
            ).fetchone()
            end_v = int(rv(last_viewer_row, "viewer_count", 0, 0) or 0) if last_viewer_row else 0

            c.execute(
                """
                UPDATE twitch_stream_sessions
                SET samples            = ?,
                    avg_viewers        = ?,
                    peak_viewers       = ?,
                    end_viewers        = ?,
                    start_viewers      = ?,
                    had_deadlock_in_session = TRUE,
                    ended_at           = ?,
                    duration_seconds   = GREATEST(0, EXTRACT(EPOCH FROM (CAST(? AS TIMESTAMPTZ) - CAST(started_at AS TIMESTAMPTZ)))::INTEGER),
                    notes              = 'recovered from category stats (orphan cleanup)'
                WHERE id = ?
                """,
                (cat_samples, cat_avg, cat_peak, end_v, start_v, str(cat_last_seen), str(cat_last_seen), sid),
            )
            recovered += 1
        else:
            # No data available — close with started_at as ended_at
            c.execute(
                """
                UPDATE twitch_stream_sessions
                SET ended_at         = COALESCE(started_at, NOW()),
                    duration_seconds = 0,
                    notes            = 'auto-closed: orphaned (no samples, no category data)'
                WHERE id = ?
                """,
                (sid,),
            )
            closed_no_data += 1

    print(f"  Recovered with category data: {recovered}")
    print(f"  Closed without data:          {closed_no_data}")


def main() -> None:
    print("=== Session Recovery Script ===")
    print()

    with storage.get_conn() as c:
        fix_session_2800(c)
        print()
        reconstruct_tracked_stats(c)
        print()
        close_orphaned_sessions(c)
        print()
        print("All changes committed.")

    print()
    print("=== Recovery complete ===")


if __name__ == "__main__":
    main()
