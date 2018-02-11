import sys

import yaml
import dropbox
from dropbox.exceptions import AuthError
from dropbox.files import (
    FileMetadata, FolderMetadata, DeletedMetadata, ListRevisionsMode)

from registry import Registry


def read_config(path='config.yaml'):
    with open(path, 'r') as fin:
        return yaml.load(fin)


def handle_file(dbx, md_file):
    deleted = isinstance(md_file, DeletedMetadata)
    print("File ", md_file.name, "[DELETED]" if deleted else "")
    print("------")
    print("Revisions:")
    revisionsResult = dbx.files_list_revisions(
            md_file.id,
            mode=ListRevisionsMode('id', None),
            limit=50)
    # revs = dbx.files_list_revisions(md_file.path_lower).entries
    for rev in revisionsResult.entries:
        print('id:', rev.id)
        print('path: ', rev.path_lower)
        print('sharing info:', rev.sharing_info)
        print('server modified: ', rev.server_modified)
        print('rev:', rev.rev)
        print('size (bytes):', rev.size)
        print('content hash:', rev.content_hash)
        print("")


def handle_folder(dbx, md_folder):
    deleted = isinstance(md_folder, DeletedMetadata)
    print("Folder ", md_folder.name, "[DELETED]" if deleted else "")
    print("------")
    print('id:', md_folder.id)
    print('path: ', md_folder.path_lower)
    print("")


def handle_metadata(dbx, metadata):
    if isinstance(metadata, FileMetadata):
        handle_file(dbx, metadata)

    if isinstance(metadata, FolderMetadata):
        handle_folder(dbx, metadata)


def list_dropbox_contents_recursively(dbx, fids):
    result = dbx.files_list_folder(
        '/test', recursive=True, include_deleted=True)

    while True:
        for entry in result.entries:
            # handle_metadata(dbx, entry)
            if isinstance(entry, DeletedMetadata):
                print(entry)
            elif entry.id in fids:
                print(entry)
        if not result.has_more:
            break
        result = dbx.files_list_folder_continue(result.cursor)

    print()


def single_file(fid, dbx):
    # fid = 'id:FioHgpMceDsAAAAAAAAdKA'
    print("============================")
    print("Metadata for ", fid)
    print("============================")
    m = dbx.files_get_metadata(
        fid, include_deleted=True)
#    handle_file(dbx, m)
    print(m)
    print("")

    revisionsResult = dbx.files_list_revisions(
            fid,
            mode=ListRevisionsMode('id', None),
            limit=50)

    print("============================")
    print("Revisions for ", fid)
    print("============================")
    for entry in revisionsResult.entries:
        print("---------------------------------------")
        print(entry)
    print("")
    print("")


def oldstuff(dbx):
    fids = ["id:FioHgpMceDsAAAAAAAAdIw", 'id:FioHgpMceDsAAAAAAAAdKA']
    list_dropbox_contents_recursively(dbx, fids)

    for fid in fids:
        single_file(fid, dbx)


def newstuff(dbx):
    registry = Registry(dbx, ['/test'])
    registry.update_from_dropbox()

    for id, obj in registry.map.items():
        print("ID: ", id, obj.id)
        for rev in obj.revisions:
            print(rev)


def main():
    config = read_config()
    cfg_dropbox = config.get('dropbox')
    if not cfg_dropbox:
        sys.exit('ERROR: config lacks dropbox section!')

    access_token = cfg_dropbox.get('access-token')
    if not access_token:
        sys.exit('ERROR: dropbox config lacks access token!')

    dbx = dropbox.dropbox.Dropbox(access_token)
    try:
        dbx.users_get_current_account()
    except AuthError:
        sys.exit('ERROR: inalid dropbox access token!')

    newstuff(dbx)


if __name__ == '__main__':
    main()
