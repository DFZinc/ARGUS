"""
argus_cache.py
Simple SQLite cache for ARGUS findings + narratives
"""

import sqlite3
import json
import os
from datetime import datetime
import logging

log = logging.getLogger(__name__)

DB_FILE = "argus_findings.db"


class ArgusCache:
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self._create_tables()

    def _create_tables(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS findings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    address TEXT,
                    symbol TEXT,
                    score REAL,
                    narrative_fit REAL,
                    virality REAL,
                    found_narratives TEXT,
                    raw_data TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS narratives (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    keyword TEXT,
                    velocity_score REAL,
                    mention_count INTEGER,
                    source TEXT
                )
            """)

    def save_findings(self, ranked_launches: list[dict], current_narratives: list[dict]):
        now = datetime.utcnow().isoformat()

        with self.conn:
            # Save narratives
            for n in current_narratives:
                self.conn.execute(
                    "INSERT INTO narratives (timestamp, keyword, velocity_score, mention_count, source) VALUES (?, ?, ?, ?, ?)",
                    (now, n["keyword"], n.get("velocity_score", 0), n.get("mention_count", 0), n.get("source", "unknown"))
                )

            # Save high-scoring launches
            for launch in ranked_launches:
                self.conn.execute(
                    """
                    INSERT INTO findings 
                    (timestamp, address, symbol, score, narrative_fit, virality, found_narratives, raw_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        now,
                        launch["address"],
                        launch.get("symbol"),
                        launch["score"],
                        launch.get("narrative_fit", 0),
                        launch.get("virality", 0),
                        json.dumps(launch.get("found_narratives", [])),
                        json.dumps(launch)
                    )
                )

        log.info(f"💾 Saved {len(ranked_launches)} findings and {len(current_narratives)} narratives to DB")

    def get_latest_findings(self, limit: int = 50):
        with self.conn:
            cursor = self.conn.execute("""
                SELECT timestamp, symbol, score, narrative_fit, virality, found_narratives 
                FROM findings 
                ORDER BY timestamp DESC LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            return [
                {
                    "timestamp": row[0],
                    "symbol": row[1],
                    "score": row[2],
                    "narrative_fit": row[3],
                    "virality": row[4],
                    "found_narratives": json.loads(row[5]) if row[5] else []
                }
                for row in rows
            ]

    def get_latest_narratives(self, limit: int = 20):
        with self.conn:
            cursor = self.conn.execute("""
                SELECT timestamp, keyword, velocity_score, mention_count, source 
                FROM narratives 
                ORDER BY timestamp DESC LIMIT ?
            """, (limit,))
            return [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]