class Commit:
    def __init__(self, classification, username=None):
        self.username = username
        self.classification
        self.revisions = []

    def add_revision(self, revision):
        self.revisions.append(revision)


class Git:
    def __init__(self, base_directory, config=None):
        self.base_directory = base_directory

    def commit(self, commit):
        pass # TBD


def build_commits(revisions, classification_func):
    m = {}
    for revision in revisions:
        classification = classification_func(revision)
        commit = m.setdefault(classification, Commit(classification))
        commit.add_revision(revision)
    
    return m