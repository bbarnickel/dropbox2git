from contextlib import closing
import sqlite3
import logging
from datetime import datetime

from dropbox.files import (
    FileMetadata, DeletedMetadata, ListRevisionsMode)
from dropbox.exceptions import (ApiError)


class RegistryError(Exception):
    pass


class FileRevision:
    STATUS_UNKNOWN = 0
    STATUS_META_ONLY = 1
    STATUS_FETCHED = 2
    STATUS_ARCHIVED = 3

    def __init__(self, id, rev):
        self.id = id
        self.rev = rev
        self.timestamp = None
        self.hash = None
        self.path_lower = None
        self.path_display = None
        self.status = FileRevision.STATUS_UNKNOWN
        self.deleted = False

    def __repr__(self):
        return self.rev + ": " + self.path + " at " + repr(self.timestamp)

    def is_equal(self, other):
        return all([i == j for i, j in [
            (self.id, other.id),
            (self.rev, other.rev),
            (self.timestamp, other.timestamp),
            (self.hash, other.hash),
            (self.path_lower, other.path_lower),
            (self.path_display, other.path_display),
            (self.status, other.status),
            (self.deleted, other.deleted)
        ]])


class SqliteRegistry:
    def __init__(self, db_path):
        self.connection = sqlite3.connect(
            db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.ensure_structure()

    def ensure_structure(self):
        with closing(self.connection.cursor()) as c:
            c.execute("""SELECT name FROM sqlite_master
                         WHERE type='table' AND name='revisions'""")
            if c.fetchone() is not None:
                return
            with self.connection:
                c.execute("""CREATE TABLE revisions (
                                id TEXT,
                                rev TEXT,
                                timestamp TIMESTAMP,
                                hash TEXT,
                                path_lower TEXT,
                                path_display TEXT,
                                status INTEGER,
                                deleted BOOLEAN,
                                PRIMARY KEY(id, rev)
                            )""")

    def get_file_ids(self):
        with closing(self.connection.cursor()) as c:
            query = """SELECT r.id FROM revisions r
                       WHERE r.timestamp = (SELECT MAX(timestamp)
                            FROM revisions rr WHERE rr.id = r.id)"""
            c.execute(query)
            for r in c:
                yield r[0]

    def get_unsafed_revisions(self):
        with closing(self.connection.cursor()) as c:
            query = """SELECT
                    id, rev, timestamp, hash, path_lower, path_display,
                    status, deleted
                FROM revisions
                WHERE status = 1"""
            c.execute(query)
            for row in c:
                yield self._create_revision_from_row(row)

    def has_revision(self, id, rev):
        with closing(self.connection.cursor()) as c:
            c.execute("""SELECT COUNT(*) FROM revisions
                         WHERE id=? AND rev=?""", (id, rev))
            count = int(c.fetchone()[0])
            if count == 0:
                return False
            elif count == 1:
                return True
            else:
                raise RegistryError(
                    "registry has more than one entry for revision "
                    "({0}, {1})".format(id, rev))

    def read_revision(self, id, rev):
        with closing(self.connection.cursor()) as c:
            c.execute("""SELECT id, rev, timestamp,
                         hash, path_lower, path_display, status, deleted
                         FROM revisions
                         WHERE id=? AND rev=?""", (id, rev))
            rows = c.fetchmany(2)
            if len(rows) == 0:
                return None
            elif len(rows) > 1:
                raise RegistryError(
                    "registry has more than one entry for "
                    "revision ({0}, {1})".format(id, rev))

            return self._create_revision_from_row(rows[0])

    def ensure_revision(self, file_revision):
        existing = self.read_revision(file_revision.id, file_revision.rev)
        if existing:
            if file_revision.deleted and existing.deleted:
                return False
            elif file_revision.is_equal(existing):
                return False
            else:
                print(file_revision.__dict__)
                print(existing.__dict__)
                raise RegistryError(
                    "Registry already contains revision ({0}, {1}) "
                    "but attributes differ!".format(
                        file_revision.id,
                        file_revision.rev))

        self._save_revision(file_revision)
        return True

    def _save_revision(self, file_revision):
        with closing(self.connection.cursor()) as c:
            with self.connection:
                c.execute(
                    """INSERT INTO revisions(
                            id,
                            rev,
                            timestamp,
                            hash,
                            path_lower,
                            path_display,
                            status,
                            deleted)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        file_revision.id,
                        file_revision.rev,
                        file_revision.timestamp,
                        file_revision.hash,
                        file_revision.path_lower,
                        file_revision.path_display,
                        file_revision.status,
                        file_revision.deleted)
                )
                return True

    def _create_revision_from_row(self, row):
        result = FileRevision(row[0], row[1])
        result.timestamp = row[2]
        result.hash = row[3]
        result.path_lower = row[4]
        result.path_display = row[5]
        result.status = row[6]
        result.deleted = row[7] == 1
        return result


class RegistryUpdater:
    """Updates the given registry from dropbox metadata"""

    def __init__(self, registry, dbx):
        self.registry = registry
        self.dbx = dbx

    def update_folder(self, folder):
        already_updated_ids = []
        update_count = 0
        print("Enumerate folder " + folder + "...")
        dbx_items = list(self.get_current_metadata(folder))
        print("Found {0} items.".format(len(dbx_items)))

        for i, dbx_metadata in enumerate(dbx_items):
            id = dbx_metadata.id
            rev = dbx_metadata.rev

            print("({}/{}) Getting data for {} ({})".format(
                i+1, len(dbx_items), dbx_metadata.path_display, id))

            if not self.registry.has_revision(id, rev):
                update_count += self.update_item(dbx_metadata, id)

            already_updated_ids.append(id)

        # Jetzt noch alle Ids in der self.map updaten,
        # die nicht geupdated worden sind.
        print("Trying to update ids which were not found in the "
              "directory enumeration...")
        update_count += self.update_items(already_updated_ids)
        print("Updated {0} files.".format(update_count))

        return update_count

    def get_current_metadata(self, folder):
        dbx_files_list = self.dbx.files_list_folder(
            folder, recursive=True, include_deleted=False)

        while True:
            for dbx_entry in dbx_files_list.entries:
                if (isinstance(dbx_entry, FileMetadata) or
                        isinstance(dbx_entry, DeletedMetadata)):
                    yield dbx_entry
            if not dbx_files_list.has_more:
                break
            dbx_files_list = self.dbx.files_list_folder_continue(
                dbx_files_list.cursor)

    def update_items(self, already_updated_ids):
        update_count = 0
        for id in self.registry.get_file_ids():
            if id not in already_updated_ids:
                dbx_metadata = self.dbx.files_get_metadata(
                    id, include_deleted=True)
                update_count += self.update_item(dbx_metadata, id)
        return update_count

    def update_item(self, dbx_metadata, id):
        is_deleted = isinstance(dbx_metadata, DeletedMetadata)
        if is_deleted:
            deleted_revision = self._get_deleted_file_revision(
                dbx_metadata, id)
            if self.registry.ensure_revision(deleted_revision):
                return 1
            else:
                return 0

        if self.registry.has_revision(id, dbx_metadata.rev):
            return 0

        revision = self._get_file_revision(dbx_metadata)

        updates = [self.registry.ensure_revision(revision)] + \
                  [
                    self.registry.ensure_revision(r) for r in
                    self.get_revisions_by_id(dbx_metadata.id)]
        return updates.count(True)

    def get_revisions_by_id(self, id):
        return self._get_revisions(id, True)

    def get_revisions_by_path(self, path):
        return self._get_revisions(path, False)

    def _get_revisions(self, key, is_id):
        try:
            if is_id:
                mode = ListRevisionsMode('id', None)
            else:
                mode = ListRevisionsMode('path', None)
            dbx_revisions_result = self.dbx.files_list_revisions(
                    key, mode=mode, limit=100)

            return map(self._get_file_revision, dbx_revisions_result.entries)
        except ApiError:
            logging.warning(
                "Error retrieving history of {}. Skipping.".format(key))
            return []

    def _get_file_revision(self, dbx_metadata):
        result = FileRevision(dbx_metadata.id, dbx_metadata.rev)
        result.timestamp = dbx_metadata.server_modified
        result.hash = dbx_metadata.content_hash
        result.path_lower = dbx_metadata.path_lower
        result.path_display = dbx_metadata.path_display
        result.deleted = isinstance(dbx_metadata, DeletedMetadata)
        return result

    def _get_deleted_file_revision(self, dbx_metadata, id):
        result = FileRevision(id, "deleted")
        # TODO: Improve following. Unfortunately the DeletedMetadata instance
        # does not offer much information
        result.timestamp = datetime.now()
        result.hash = ""
        result.path_lower = dbx_metadata.path_lower
        result.path_display = dbx_metadata.path_display
        result.deleted = True
        return result
