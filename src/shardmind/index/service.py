"""SQLite-backed typed-object index."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from shardmind.models import Note, ObjectRecord, PaperCard, SearchResult
from shardmind.paper_cards import PAPER_CARD_SECTION_LABELS
from shardmind.vault.ids import slugify


class IndexService:
    def __init__(self, sqlite_path: Path):
        self.sqlite_path = sqlite_path
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection: sqlite3.Connection | None = self._connect()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.sqlite_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def close(self) -> None:
        if self.connection is None:
            return
        self.connection.close()
        self.connection = None

    def _initialize(self) -> None:
        connection = self._require_connection()
        with connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    path TEXT NOT NULL,
                    tags TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    normalized_title TEXT NOT NULL DEFAULT '',
                    url TEXT NOT NULL DEFAULT '',
                    citekey TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id TEXT NOT NULL,
                    section_name TEXT NOT NULL,
                    content TEXT NOT NULL,
                    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS tags (
                    document_id TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS embeddings (
                    document_id TEXT NOT NULL,
                    section_name TEXT NOT NULL,
                    vector BLOB,
                    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
                );

                CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
                    document_id UNINDEXED,
                    section_name,
                    content
                );
                """
            )
            self._ensure_document_columns()

    def _ensure_document_columns(self) -> None:
        connection = self._require_connection()
        columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(documents)").fetchall()
        }
        missing = {
            "normalized_title": (
                "ALTER TABLE documents ADD COLUMN normalized_title TEXT NOT NULL DEFAULT ''"
            ),
            "url": "ALTER TABLE documents ADD COLUMN url TEXT NOT NULL DEFAULT ''",
            "citekey": "ALTER TABLE documents ADD COLUMN citekey TEXT NOT NULL DEFAULT ''",
        }
        for column, statement in missing.items():
            if column not in columns:
                connection.execute(statement)

    def reindex_note(self, note: Note, path: str) -> None:
        self.reindex_object(note, path)

    def reindex_object(self, record: ObjectRecord, path: str) -> None:
        tags = list(record.tags)
        chunks = self._chunks_for_object(record)
        metadata = self._document_metadata(record)
        connection = self._require_connection()
        with connection:
            self._upsert_object(connection, record, path, tags, chunks, metadata)

    def rebuild(self, records: list[tuple[ObjectRecord, str]]) -> None:
        connection = self._require_connection()
        with connection:
            connection.execute("DELETE FROM chunks_fts")
            connection.execute("DELETE FROM documents")
            for record, path in records:
                tags = list(record.tags)
                chunks = self._chunks_for_object(record)
                metadata = self._document_metadata(record)
                self._upsert_object(connection, record, path, tags, chunks, metadata)

    def remove_object(self, document_id: str) -> None:
        connection = self._require_connection()
        with connection:
            self._delete_object_rows(connection, document_id)
            connection.execute("DELETE FROM documents WHERE id = ?", (document_id,))

    def get_path(self, document_id: str) -> str | None:
        row = (
            self._require_connection()
            .execute(
                "SELECT path FROM documents WHERE id = ?",
                (document_id,),
            )
            .fetchone()
        )
        if row is None:
            return None
        return str(row["path"])

    def existing_paper_card_ids(self) -> set[str]:
        rows = (
            self._require_connection()
            .execute(
                "SELECT id FROM documents WHERE type = 'paper-card'",
            )
            .fetchall()
        )
        return {str(row["id"]) for row in rows}

    def find_duplicate_paper_card(
        self,
        *,
        normalized_title: str = "",
        url: str = "",
        citekey: str = "",
    ) -> str | None:
        clauses: list[str] = []
        params: list[object] = []
        if normalized_title:
            clauses.append("normalized_title = ?")
            params.append(normalized_title)
        if url:
            clauses.append("url = ?")
            params.append(url)
        if citekey:
            clauses.append("citekey = ?")
            params.append(citekey)
        if not clauses:
            return None
        row = (
            self._require_connection()
            .execute(
                f"""
            SELECT id
            FROM documents
            WHERE type = 'paper-card' AND ({" OR ".join(clauses)})
            ORDER BY updated_at DESC
            LIMIT 1
            """,
                params,
            )
            .fetchone()
        )
        if row is None:
            return None
        return str(row["id"])

    def list_objects(
        self,
        object_type: str | None = None,
        path_scope: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, object]]:
        clauses = []
        params: list[object] = []
        if object_type:
            clauses.append("type = ?")
            params.append(object_type)
        if path_scope:
            clauses.append("path LIKE ?")
            params.append(f"{path_scope}%")
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"""
            SELECT id, type, title, path, updated_at
            FROM documents
            {where_clause}
            ORDER BY updated_at DESC
            LIMIT ?
        """
        params.append(limit)
        rows = self._require_connection().execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def search(
        self,
        query: str,
        object_types: list[str] | None = None,
        path_scope: str | None = None,
        top_k: int = 10,
        tags: list[str] | None = None,
    ) -> list[SearchResult]:
        filters = []
        params: list[object] = [query]
        if object_types:
            placeholders = ", ".join(["?"] * len(object_types))
            filters.append(f"d.type IN ({placeholders})")
            params.extend(object_types)
        if path_scope:
            filters.append("d.path LIKE ?")
            params.append(f"{path_scope}%")
        for index, tag in enumerate(tags or []):
            alias = f"t{index}"
            filters.append(
                "EXISTS ("
                f"SELECT 1 FROM tags {alias} "
                f"WHERE {alias}.document_id = d.id AND {alias}.tag = ?"
                ")"
            )
            params.append(tag)
        where_clause = f"AND {' AND '.join(filters)}" if filters else ""
        sql = f"""
            SELECT
                d.id,
                d.type,
                d.title,
                d.path,
                d.tags,
                chunks_fts.section_name AS section_name,
                chunks_fts.content AS content,
                snippet(chunks_fts, 2, '', '', '...', 24) AS snippet,
                bm25(chunks_fts) AS rank
            FROM chunks_fts
            JOIN documents d ON d.id = chunks_fts.document_id
            WHERE chunks_fts MATCH ?
            {where_clause}
            ORDER BY rank
            LIMIT ?
        """
        params.append(max(top_k * 10, 25))
        rows = self._require_connection().execute(sql, params).fetchall()
        return self._collapse_results(rows, top_k)

    def _delete_object_rows(self, connection: sqlite3.Connection, document_id: str) -> None:
        connection.execute("DELETE FROM chunks WHERE document_id = ?", (document_id,))
        connection.execute("DELETE FROM chunks_fts WHERE document_id = ?", (document_id,))
        connection.execute("DELETE FROM tags WHERE document_id = ?", (document_id,))
        connection.execute("DELETE FROM embeddings WHERE document_id = ?", (document_id,))

    def _upsert_object(
        self,
        connection: sqlite3.Connection,
        record: ObjectRecord,
        path: str,
        tags: list[str],
        chunks: list[tuple[str, str]],
        metadata: dict[str, str],
    ) -> None:
        self._delete_object_rows(connection, record.id)
        connection.execute(
            """
            INSERT INTO documents(
                id, type, title, path, tags, updated_at, normalized_title, url, citekey
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                type = excluded.type,
                title = excluded.title,
                path = excluded.path,
                tags = excluded.tags,
                updated_at = excluded.updated_at,
                normalized_title = excluded.normalized_title,
                url = excluded.url,
                citekey = excluded.citekey
            """,
            (
                record.id,
                record.type,
                record.title,
                path,
                self._encode_tags(tags),
                record.updated_at,
                metadata["normalized_title"],
                metadata["url"],
                metadata["citekey"],
            ),
        )
        for tag in tags:
            connection.execute(
                "INSERT INTO tags(document_id, tag) VALUES (?, ?)",
                (record.id, tag),
            )
        for section_name, content in chunks:
            connection.execute(
                "INSERT INTO chunks(document_id, section_name, content) VALUES (?, ?, ?)",
                (record.id, section_name, content),
            )
            connection.execute(
                "INSERT INTO chunks_fts(document_id, section_name, content) VALUES (?, ?, ?)",
                (record.id, section_name, content),
            )

    def _require_connection(self) -> sqlite3.Connection:
        if self.connection is None:
            raise RuntimeError("IndexService connection is closed.")
        return self.connection

    def _document_metadata(self, record: ObjectRecord) -> dict[str, str]:
        if isinstance(record, PaperCard):
            return {
                "normalized_title": slugify(record.title),
                "url": record.url,
                "citekey": record.citekey,
            }
        return {"normalized_title": "", "url": "", "citekey": ""}

    def _chunks_for_object(self, record: ObjectRecord) -> list[tuple[str, str]]:
        chunks: list[tuple[str, str]] = [("Title", record.title)]
        if isinstance(record, Note):
            if record.sections.content.strip():
                chunks.append(("Content", record.sections.content.strip()))
            return chunks
        if record.authors:
            chunks.append(("Authors", ", ".join(record.authors)))
        if record.year is not None:
            chunks.append(("Year", str(record.year)))
        if record.source.strip():
            chunks.append(("Source", record.source.strip()))
        if record.url.strip():
            chunks.append(("URL", record.url.strip()))
        for field_name, section_label in PAPER_CARD_SECTION_LABELS.items():
            content = getattr(record.sections, field_name).strip()
            if content:
                chunks.append((section_label, content))
        return chunks

    def _collapse_results(self, rows: list[sqlite3.Row], top_k: int) -> list[SearchResult]:
        collapsed: dict[str, SearchResult] = {}
        best_ranks: dict[str, float] = {}
        for row in rows:
            document_id = row["id"]
            rank = float(row["rank"])
            snippet = row["snippet"] or row["content"][:200]
            if document_id not in collapsed:
                collapsed[document_id] = SearchResult(
                    id=document_id,
                    type=row["type"],
                    title=row["title"],
                    path=row["path"],
                    score=self._score(rank),
                    matched_sections=[row["section_name"]],
                    snippet=snippet,
                    tags=self._decode_tags(row["tags"]),
                )
                best_ranks[document_id] = rank
                continue
            result = collapsed[document_id]
            if row["section_name"] not in result.matched_sections:
                result.matched_sections.append(row["section_name"])
            if rank < best_ranks[document_id]:
                result.score = self._score(rank)
                result.snippet = snippet
                best_ranks[document_id] = rank
        ordered_ids = sorted(best_ranks, key=best_ranks.get)
        return [collapsed[document_id] for document_id in ordered_ids[:top_k]]

    def _encode_tags(self, tags: list[str]) -> str:
        if not tags:
            return ""
        return "|" + "|".join(tags) + "|"

    def _decode_tags(self, encoded: str) -> list[str]:
        return [tag for tag in encoded.strip("|").split("|") if tag]

    def _score(self, rank: float) -> float:
        return 1.0 / (1.0 + abs(rank))
