import dataclasses
import sqlite3
from typing import Dict, Any, List


from attendance.sitting_day_attendee import SittingDayAttendee


class StoreSqlite:
    sqlite_db_file = "data.sqlite"

    def add_items(self, items: List[SittingDayAttendee]):
        db_conn = None
        added = 0
        total = 0
        try:
            db_conn = self.get_connection()
            self.create_tables(db_conn)

            for item in items:
                item_dict = dataclasses.asdict(item)

                if self.add_entry(db_conn, item_dict):
                    added += 1
                total += 1

                db_conn.commit()

        finally:
            if db_conn:
                db_conn.close()

        return total, added

    def add_entry(self, db_conn, row: Dict[str, Any]) -> bool:
        c = db_conn.execute(
            "SELECT COUNT(*) "
            "FROM attendance "
            "WHERE assembly = ? AND sitting_date = ? AND person_name = ?",
            (row["assembly"], row["sitting_date"], row["person_name"]),
        )
        count = c.fetchone()[0]
        c.close()

        if count < 1:
            c = db_conn.execute(
                "INSERT INTO attendance "
                "(url, assembly, sitting_date, person_name, is_leave, retrieved_date) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    row["url"],
                    row["assembly"],
                    row["sitting_date"],
                    row["person_name"],
                    row["is_leave"],
                    row["retrieved_date"],
                ),
            )
            c.close()
            return True
        return False

    def get_connection(self):
        conn = sqlite3.connect(self.sqlite_db_file)
        return conn

    def create_tables(self, db_conn):
        db_conn.execute(
            "CREATE TABLE IF NOT EXISTS attendance ("
            "id INTEGER PRIMARY KEY,"
            "url TEXT,"
            "assembly TEXT,"
            "sitting_date TEXT,"
            "person_name TEXT,"
            "is_leave INTEGER,"
            "retrieved_date TEXT,"
            "CONSTRAINT unique_assembly_date_person UNIQUE (assembly, sitting_date, person_name)"
            ")"
        )
