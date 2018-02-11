import json
from datetime import datetime

from dropbox.files import (
    FileMetadata, DeletedMetadata, ListRevisionsMode)


class File:
    def __init__(self, dbid):
        self.id = dbid
        print(self.id)
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
        print(self.id)
        self.archived = False
        self.timestamp = datetime.now()


class FileRevision:
    def __init__(self, dbxRev):
        self.id = dbxRev.id
        self.path = dbxRev.path_lower
        self.rev = dbxRev.rev
        self.timestamp = dbxRev.server_modified
        self.hash = dbxRev.content_hash
        self.archived = False


class Registry:
    def __init__(self, dbx, dbx_folders):
        self.dbx = dbx
        self.folders = dbx_folders
        self.map = {}

    def load_from_json(self, path_to_json):
        with open(path_to_json, 'r') as fin:
            self.map = json.load(fin)

    def store_to_json(self, path_to_json):
        with open(path_to_json, 'w') as fout:
            json.dump(fout, self.map)

    def update_from_dropbox(self):
        items = list(self.get_dbx_current_metadata())
        already_updated_ids = []
        for metadata in items:
            if id not in self.map:
                self.map[id] = File(id)
            dbx_file = self.map[id]
            self.update_file_from_metadata(dbx_file, metadata)
            already_updated_ids.append(metadata.id)

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
            revision = FileRevision(metadata)
            dbx_file.revisions.append(revision)
            self.update_revisions(dbx_file)

    def update_files(self, already_updated_ids):
        for id, dbx_file in self.map.items():
            if id not in already_updated_ids:
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
        print(dbx_file)
        print(dbx_file.id)
        revisionsResult = self.dbx.files_list_revisions(
                dbx_file.id,
                mode=ListRevisionsMode('id', None),
                limit=100)
        for entry in revisionsResult.entries:
            revision = FileRevision(entry)
            if not dbx_file.contains_revision(revision):
                dbx_file.revisions.append(revision)
