"""
argus_cache.py
SQLite cache for ARGUS findings with connection pooling and TTL
"""

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from contextlib import contextmanager

log = logging.getLogger(__name__)

DB_FILE = "argus_findings.db"
CACHE_TTL_HOURS = 24  # Keep findings for 24 hours


class ArgusCache:
    def __init__(self, db_file: str = DB_FILE):
        self.db_file = db_file
        self._init_database()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection"""
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _get_cursor(self):
        """Context manager for database operations"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        finally:
            conn.close()

    def _init_database(self):
        """Initialize database tables"""
        with self._get_cursor() as cursor:
            # Findings table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS findings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    address TEXT,
                    symbol TEXT,
                    name TEXT,
                    score REAL,
                    narrative_fit REAL,
                    narrative_count INTEGER,
                    virality REAL,
                    txns_score REAL,
                    found_narratives TEXT,
                    volume_usd_1h REAL,
                    txns_1h INTEGER,
                    raw_data TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Narratives table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS narratives (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    keyword TEXT,
                    velocity_score REAL,
                    mention_count INTEGER,
                    engagement INTEGER,
                    source TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_findings_timestamp ON findings(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_findings_score ON findings(score)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_narratives_timestamp ON narratives(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_narratives_keyword ON narratives(keyword)")

    def save_findings(self, ranked_launches: List[Dict], current_narratives: List[Dict]):
        """Save findings and narratives to database"""
        now = datetime.utcnow().isoformat()
        
        with self._get_cursor() as cursor:
            # Save narratives
            for n in current_narratives:
                cursor.execute("""
                    INSERT INTO narratives 
                    (timestamp, keyword, velocity_score, mention_count, engagement, source)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    now,
                    n.get("keyword", ""),
                    n.get("velocity_score", 0),
                    n.get("mention_count", 0),
                    n.get("engagement", 0),
                    n.get("source", "unknown")
                ))

            # Save high-scoring launches
            for launch in ranked_launches:
                cursor.execute("""
                    INSERT INTO findings 
                    (timestamp, address, symbol, name, score, narrative_fit, 
                     narrative_count, virality, txns_score, found_narratives, 
                     volume_usd_1h, txns_1h, raw_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    now,
                    launch.get("address", ""),
                    launch.get("symbol", ""),
                    launch.get("name", ""),
                    launch.get("score", 0),
                    launch.get("narrative_fit", 0),
                    launch.get("narrative_count", 0),
                    launch.get("virality", 0),
                    launch.get("txns_score", 0),
                    json.dumps(launch.get("found_narratives", [])),
                    launch.get("volume_usd_1h", 0),
                    launch.get("txns_1h", 0),
                    json.dumps(launch)
                ))

            # Clean old data
            cutoff = datetime.utcnow() - timedelta(hours=CACHE_TTL_HOURS)
            cutoff_str = cutoff.isoformat()
            
            cursor.execute("DELETE FROM findings WHERE timestamp < ?", (cutoff_str,))
            cursor.execute("DELETE FROM narratives WHERE timestamp < ?", (cutoff_str,))
            
            deleted_findings = cursor.rowcount
            if deleted_findings:
                log.info(f"Cleaned {deleted_findings} old findings")

        log.info(f"Saved {len(ranked_launches)} findings and {len(current_narratives)} narratives")

    def get_latest_findings(self, limit: int = 50) -> List[Dict]:
        """Get latest findings"""
        with self._get_cursor() as cursor:
            cursor.execute("""
                SELECT timestamp, symbol, name, score, narrative_fit, narrative_count,
                       virality, txns_score, found_narratives, volume_usd_1h, txns_1h
                FROM findings 
                ORDER BY timestamp DESC LIMIT ?
            """, (limit,))
            
            rows = cursor.fetchall()
            return [
                {
                    "timestamp": row["timestamp"],
                    "symbol": row["symbol"],
                    "name": row["name"],
                    "score": row["score"],
                    "narrative_fit": row["narrative_fit"],
                    "narrative_count": row["narrative_count"],
                    "virality": row["virality"],
                    "txns_score": row["txns_score"],
                    "found_narratives": json.loads(row["found_narratives"]) if row["found_narratives"] else [],
                    "volume_usd_1h": row["volume_usd_1h"],
                    "txns_1h": row["txns_1h"]
                }
                for row in rows
            ]

    def get_latest_narratives(self, limit: int = 20) -> List[Dict]:
        """Get latest narratives"""
        with self._get_cursor() as cursor:
            cursor.execute("""
                SELECT timestamp, keyword, velocity_score, mention_count, engagement, source 
                FROM narratives 
                ORDER BY timestamp DESC LIMIT ?
            """, (limit,))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_top_narratives(self, hours: int = 24, limit: int = 10) -> List[Dict]:
        """Get top narratives by velocity score in last N hours"""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        cutoff_str = cutoff.isoformat()
        
        with self._get_cursor() as cursor:
            cursor.execute("""
                SELECT keyword, AVG(velocity_score) as avg_velocity, 
                       SUM(mention_count) as total_mentions, 
                       COUNT(*) as appearances
                FROM narratives 
                WHERE timestamp > ?
                GROUP BY keyword 
                ORDER BY avg_velocity DESC LIMIT ?
            """, (cutoff_str, limit))
            
            return [dict(row) for row in cursor.fetchall()]

    def get_stats(self) -> Dict:
        """Get cache statistics"""
        with self._get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as total FROM findings")
            findings_count = cursor.fetchone()["total"]
            
            cursor.execute("SELECT COUNT(*) as total FROM narratives")
            narratives_count = cursor.fetchone()["total"]
            
            cursor.execute("""
                SELECT AVG(score) as avg_score, MAX(score) as max_score 
                FROM findings 
                WHERE timestamp > datetime('now', '-1 day')
            """)
            stats = dict(cursor.fetchone())
            
            return {
                "total_findings": findings_count,
                "total_narratives": narratives_count,
                "avg_score_24h": round(stats.get("avg_score", 0), 2),
                "max_score_24h": round(stats.get("max_score", 0), 2)
            }