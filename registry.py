import pickle
from datetime import datetime
from contextlib import closing
import sqlite3

from dropbox.files import (
    FileMetadata, DeletedMetadata, ListRevisionsMode)
from dropbox.exceptions import (ApiError)


class File:
    def __init__(self, dbid):
        self.id = dbid
        self.revisions = []

    def contains_revision(self, rev_id):
        return any(map(lambda r: r.rev == rev_id, self.revisions))

    def is_deleted(self):
        if not any(self.revisions):
            return False

        newest = sorted(
                self.revisions, key=lambda i: i.timestamp, reverse=True)[0]
        return isinstance(newest, DeletedRevision)

    def add_deleted(self):
        self.revisions.append(DeletedRevision(self.id))


class DeletedRevision:
    def __init__(self, dbid):
        self.id = dbid
        self.archived = False
        self.timestamp = datetime.now()

    def __repr__(self):
        return "Deleted id " + self.id + " at " + repr(self.timestamp)


class FileRevision:
    STATUS_UNKNOWN = 0
    STATUS_META_ONLY = 1
    STATUS_FETCHED = 2
    STATUS_ARCHIVED = 3

#    def __init__(self, dbxRev):
#        self.id = dbxRev.id
#        self.path = dbxRev.path_lower
#        self.rev = dbxRev.rev
#        self.timestamp = dbxRev.server_modified
#        self.hash = dbxRev.content_hash
#        self.archived = False

    def __init__(self, id, rev):
        self.id = id
        self.rev = rev
        self.timestamp = None
        self.hash = None
        self.path = None
        self.status = FileRevision.STATUS_UNKNOWN

    def __repr__(self):
        return self.rev + ": " + self.path + " at " + repr(self.timestamp)


class SqliteRegistry:
    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path)
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
                                timestamp DATETIME,
                                hash TEXT,
                                path TEXT,
                                status INTEGER,
                                PRIMARY KEY(id, rev)
                            )""")
    def query(self, query, *params):

    def get_all_revisions(self, file_id):




class Registry:
    def __init__(self, dbx, dbx_folders):
        self.dbx = dbx
        self.folders = dbx_folders
        self.map = {}

    def load_from_json(self, path_to_json):
        try:
            with open(path_to_json, 'rb') as fin:
                self.map = pickle.load(fin)
                # self.map = json.load(fin)
        except:
            print("Warning: Could not properly restore from " + path_to_json)
            self.map = {}

    def store_to_json(self, path_to_json):
        with open(path_to_json, 'wb') as fout:
            # json.dump(self.map, fout)
            pickle.dump(self.map, fout)

    def update_from_dropbox(self):
        items = list(self.get_dbx_current_metadata())[:20]
        already_updated_ids = []
        for i, metadata in enumerate(items):
            mid = metadata.id

            print("({}/{}) Getting data for {} ({})".format(
                i, len(items), metadata.path_display, mid))

            # Sorgt dafür, dass die Datei in self.map mit der
            # id als Schlüssel kommt, wenn nicht schon
            # geschehen.
            dbx_file = self.map.setdefault(mid, File(mid))

            # File-Objekt wird nun mit den Daten der obersten
            # Revision gefüttert.
            self.update_file_from_metadata(dbx_file, metadata)

            already_updated_ids.append(mid)

        # Jetzt noch alle Ids in der self.map updaten,
        # die nicht geupdated worden sind.
        self.update_files(already_updated_ids)

    def get_dbx_current_metadata(self):
        for folder in self.folders:
            files_list = self.dbx.files_list_folder(
                folder, recursive=True, include_deleted=False)

            while True:
                for entry in files_list.entries:
                    if (isinstance(entry, FileMetadata) or
                            isinstance(entry, DeletedMetadata)):
                        yield entry
                if not files_list.has_more:
                    break
                files_list = self.dbx.files_list_folder_continue(
                    files_list.cursor)

    def update_file_from_metadata(self, dbx_file, metadata):
        if not dbx_file.contains_revision(metadata.rev):
            print("  Getting revisions...")
            revision = FileRevision(metadata)
            dbx_file.revisions.append(revision)
            self.update_revisions(dbx_file)

    def update_files(self, already_updated_ids):
        for id, dbx_file in self.map.items():
            if id not in already_updated_ids:
                print("Found existing entry not updated.")
                self.update_file(self, dbx_file)

    def update_file(self, dbx_file):
        metadata = self.dbx.files_get_metadata(
            dbx_file.id, include_deleted=True)

        if isinstance(metadata, DeletedMetadata):
            if not dbx_file.is_deleted():
                dbx_file.add_deleted()
        else:
            self.update_file_from_metadata(dbx_file, metadata)

    def update_revisions(self, dbx_file):
        try:
            revisionsResult = self.dbx.files_list_revisions(
                    dbx_file.id,
                    mode=ListRevisionsMode('id', None),
                    limit=100)

            for entry in revisionsResult.entries:
                revision = FileRevision(entry)
                if not dbx_file.contains_revision(revision.rev):
                    dbx_file.revisions.append(revision)
        except ApiError as e:
            print("Error retrieving history of {}. Skipping.".format(
                dbx_file.id))
