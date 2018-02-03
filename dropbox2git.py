import sys

import yaml

import dropbox
from dropbox.exceptions import AuthError
from dropbox.files import (
    FileMetadata, FolderMetadata, DeletedMetadata)


def read_config(path='config.yaml'):
    with open(path, 'r') as fin:
        return yaml.load(fin)


def handle_file(dbx, md_file):
    deleted = isinstance(md_file, DeletedMetadata)
    print("File ", md_file.name, "[DELETED]" if deleted else "")
    print("------")
    print("Revisions:")
    revs = dbx.files_list_revisions(md_file.path_lower).entries
    for rev in revs:
        print('id:', md_file.id)
        print('path: ', md_file.path_lower)
        print('sharing info:', md_file.sharing_info)
        print('server modified: ', md_file.server_modified)
        print('rev:', md_file.rev)
        print('size (bytes):', md_file.size)
        print('content hash:', md_file.content_hash)
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


def list_dropbox_contents_recursively(dbx):
    result = dbx.files_list_folder('', recursive=True, include_deleted=False)

    while True:
        for entry in result.entries:
            handle_metadata(dbx, entry)
        if not result.has_more:
            break
        result = dbx.files_list_folder_continue(result.cursor)


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

    list_dropbox_contents_recursively(dbx)


if __name__ == '__main__':
    main()
