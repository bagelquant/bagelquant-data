"""Stable catalog identity and independent Raw/DataItem category trees."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.management.sources import SourceManager
from bagelquant_data.storage.data_meta import DataMetaStore


class CategoryTree:
    """A single-parent tree whose paths do not determine file locations."""

    def __init__(self, store: DataMetaStore, kind: str, source: str = "") -> None:
        self._data_meta = store
        self.kind = kind
        self.source = source

    def list(self) -> list[dict[str, Any]]:
        return self._data_meta._rows(
            "select id,name,parent_id from category_nodes where kind=? and source=? order by name,id",
            (self.kind, self.source),
        )

    def get(self, category_id: str) -> dict[str, Any]:
        rows = self._data_meta._rows(
            "select id,name,parent_id from category_nodes where id=? and kind=? and source=?",
            (category_id, self.kind, self.source),
        )
        if not rows:
            raise ConfigurationError(f"Unknown {self.kind} category: {category_id}")
        return rows[0]

    def create(self, name: str, *, parent_id: str | None = None) -> dict[str, Any]:
        self._data_meta.ensure_writable()
        self._check_name(name)
        if parent_id is not None:
            self.get(parent_id)
        category_id = uuid4().hex
        with self._data_meta.connect() as db:
            db.execute("begin immediate")
            self._check_sibling(db, name, parent_id)
            db.execute(
                "insert into category_nodes values(?,?,?,?,?)",
                (category_id, self.kind, self.source, name, parent_id),
            )
        return self.get(category_id)

    def rename(self, category_id: str, name: str) -> dict[str, Any]:
        self._data_meta.ensure_writable()
        self._check_name(name)
        node = self.get(category_id)
        with self._data_meta.connect() as db:
            db.execute("begin immediate")
            self._check_sibling(db, name, node["parent_id"], category_id)
            db.execute(
                "update category_nodes set name=? where id=?", (name, category_id)
            )
        return self.get(category_id)

    def move(self, category_id: str, *, parent_id: str | None = None) -> dict[str, Any]:
        self._data_meta.ensure_writable()
        node = self.get(category_id)
        with self._data_meta.connect() as db:
            db.execute("begin immediate")
            parent = parent_id
            while parent is not None:
                if parent == category_id:
                    raise ConfigurationError("Category move would create a cycle")
                row = db.execute(
                    "select parent_id from category_nodes where id=? and kind=? and source=?",
                    (parent, self.kind, self.source),
                ).fetchone()
                if row is None:
                    raise ConfigurationError(f"Unknown parent category: {parent}")
                parent = row[0]
            self._check_sibling(db, node["name"], parent_id, category_id)
            db.execute(
                "update category_nodes set parent_id=? where id=?",
                (parent_id, category_id),
            )
        return self.get(category_id)

    def remove(self, category_id: str) -> None:
        self._data_meta.ensure_writable()
        self.get(category_id)
        with self._data_meta.connect() as db:
            db.execute("begin immediate")
            if (
                db.execute(
                    "select 1 from category_nodes where parent_id=?", (category_id,)
                ).fetchone()
                or db.execute(
                    "select 1 from catalog_assignments where category_id=?",
                    (category_id,),
                ).fetchone()
            ):
                raise ConfigurationError("Cannot remove a nonempty category")
            db.execute("delete from category_nodes where id=?", (category_id,))

    def assign(self, object_key: str, category_id: str | None) -> None:
        self._data_meta.ensure_writable()
        if category_id is not None:
            self.get(category_id)
        table = "datasets" if self.kind == "raw" else "item_definitions"
        with self._data_meta.connect() as db:
            db.execute("begin immediate")
            if self.kind == "raw":
                exists = db.execute(
                    f"select 1 from {table} where source=? and name=? and active=1",
                    (self.source, object_key),
                ).fetchone()
            else:
                exists = db.execute(
                    f"select 1 from {table} where name=? and active=1", (object_key,)
                ).fetchone()
            if exists is None:
                raise ConfigurationError(f"Unknown {self.kind} object: {object_key}")
            if category_id is None:
                db.execute(
                    "delete from catalog_assignments where kind=? and source=? and object_key=?",
                    (self.kind, self.source, object_key),
                )
            else:
                db.execute(
                    "insert into catalog_assignments values(?,?,?,?) on conflict(kind,source,object_key) do update set category_id=excluded.category_id",
                    (self.kind, self.source, object_key, category_id),
                )

    def members(self, category_id: str) -> list[str]:
        self.get(category_id)
        return [
            row["object_key"]
            for row in self._data_meta._rows(
                "select object_key from catalog_assignments where category_id=? order by object_key",
                (category_id,),
            )
        ]

    @staticmethod
    def _check_name(name: str) -> None:
        if not isinstance(name, str) or not name.strip() or name != name.strip():
            raise ConfigurationError("Category name must be a nonempty trimmed string")

    def _check_sibling(
        self, db, name: str, parent_id: str | None, excluded: str = ""
    ) -> None:
        if db.execute(
            "select 1 from category_nodes where kind=? and source=? and name=? and parent_id is ? and id<>?",
            (self.kind, self.source, name, parent_id, excluded),
        ).fetchone():
            raise ConfigurationError(f"Duplicate sibling category: {name}")


class LakeCatalog:
    """Sources and two independent category namespaces."""

    def __init__(self, store: DataMetaStore, sources: SourceManager) -> None:
        self._data_meta = store
        self.sources = sources
        if not store.read_only:
            with store.connect() as db:
                db.executescript("""
                    create table if not exists category_nodes(
                        id text primary key, kind text not null, source text not null,
                        name text not null, parent_id text references category_nodes(id));
                    create table if not exists catalog_assignments(
                        kind text not null, source text not null, object_key text not null,
                        category_id text not null references category_nodes(id),
                        primary key(kind,source,object_key));
                """)
        self.item_categories = CategoryTree(store, "item")

    def raw_categories(self, source: str) -> CategoryTree:
        return CategoryTree(self._data_meta, "raw", source)
