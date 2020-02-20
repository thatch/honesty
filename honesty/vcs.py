"""
If it works right, tells you what git tag corresponds to a given release by
examinining contents.

Precaching the information about every file for every commit is very
memory-intensive, and for some large repos like tensorflow, consumes many
GB, slowly.  I intend to refactor this and document better in the future, but
this works for many smaller repos now.
"""
import os
import functools
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional

from .releases import Package

GITHUB_URL = re.compile(r"^https?://github.com/[^/]+/[^/]+")
GITLAB_URL = re.compile(r"^https?://gitlab.com/[^/]+/[^/]+")


def extract_vcs_url(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    if not s or s == "UNKNOWN":
        return None

    m = GITHUB_URL.match(s)
    if m:
        # TODO repack to make https, transform ssh to https
        return m.group(0) + "/"
    else:
        # TODO right now these go in the same cache dir as a github project of
        # the same name.
        m = GITLAB_URL.match(s)
        if m:
            return m.group(0) + "/"

    # It's a string, but not a known hosting provider
    #print(f"Unknown host {s}")
    return None


def extract2(p: Package) -> Optional[str]:
    url = extract_vcs_url(p.home_page)
    if url:
        return url
    if p.project_urls:
        for i in p.project_urls.values():
            url = extract_vcs_url(i)
            if url:
                return url
    return None


ONELINE_RE = re.compile(r"^([0-9a-f]+) (?:\((.+?)\) )?(.*)", re.M)


class CloneAnalyzer:
    def __init__(self, url: str, verbose: bool=False) -> None:
        assert url.endswith("/")
        parts = url.split("/")
        self.key = "__".join(parts[-3:-1])
        self.dir = Path("~/.cache/honesty/git").expanduser() / self.key
        if not self.dir.exists():
            subprocess.check_call(["git", "clone", url, self.dir],
            env={"GIT_TERMINAL_PROMPT": "0"})
        else:
            subprocess.check_call(["git", "fetch", "origin", "--tags"], cwd=self.dir)

        self.verbose = verbose

        #self.branch_commits = branch_commits = {}
        #self.branch_file_hash_ranges = branch_file_hash_ranges = {}
        #if self.verbose:
        #    print("Caching commits", file=sys.stderr)
        #self._tree_cache = {}

        #for branch in self._branch_names():
        #    t0 = time.time()
        #    branch_commits[branch] = []
        #    branch_file_hash_ranges[branch] = {}

        #    for (rev, tree) in self._tree_log(branch):
        #        branch_commits[branch].append(rev)

        #        for line in self._ls_tree(tree):
        #            parts = line.split(" ", 2)
        #            if parts[1] == "blob":
        #                blob_bash, filename = parts[2].split("\t", 1)

        #                # TODO we've thrown away the filename...
        #                branch_file_hash_ranges[branch].setdefault(
        #                    blob_bash, []
        #                ).append(rev)
        #    if self.verbose:
        #        print(f"Branch {branch} done in {time.time()-t0}s", file=sys.stderr)

        #self._log_cache = {}

    def _tree_log(self, ref):
        return [
            line.split()
            for line in subprocess.check_output(
                ["git", "log", "--format=%h %T", ref], cwd=self.dir, encoding="utf-8"
            ).splitlines()
            if line.strip()
        ]

    @functools.lru_cache(maxsize=None)
    def _ls_tree(self, tree):
        return subprocess.check_output(
            ["git", "ls-tree", "-r", tree], encoding="utf-8", cwd=self.dir
        ).splitlines()

    def _hash_object_path(self, path):
        return subprocess.check_output(
            ["git", "hash-object", path], encoding="utf-8").strip()

    def best_match_contents(self, filename, contents) -> Any:
        # In order for clone to pull it down, it must be reachable; so we can
        # check log of tags, and log of remote branches.  Commonly, tags are
        # part of branch history, so check those first.

        # TODO contents has to be utf-8 encodable here...
        hash = subprocess.check_output(
            ["git", "hash-object", "--stdin"], input=contents, encoding="utf-8"
        ).strip()

        rv = {}

        for branch, known_blobs in self.branch_file_hash_ranges.items():
            rv[branch] = set(known_blobs.get(hash, ()))

        # for branch in self._branch_names():
        #    #print("  ", branch)
        #    # Need to meld overall commit history with the file's so we can see
        #    # tags created with the same contents, but not on the commit that
        #    # sets those contents.
        #    branch_history = self._log(branch)[::-1]
        #    # If we just want one, probably should start with most recent...
        #    file_history = self._log(branch, filename)[::-1]

        #    matching_contents = {}
        #    for a, b, c in file_history:
        #        matching_contents[a] = self._cat(filename, a) == contents

        #    state = False
        #    tags = []
        #    commits = set()
        #    for a, b, c in branch_history:
        #        if a in matching_contents:
        #            state = matching_contents[a]
        #        if state:
        #            commits.add(a)
        #            for dec in b.split(", "):
        #                if dec.startswith("tag: "):
        #                    tags.append(dec[5:])
        #    rv[branch] = commits

        #    #if tags:
        #    #    print("    ", "tag", tags)
        #    #elif commits:
        #    #    print("    ", "commit", commits)
        #    #else:
        #    #    print("    ", "(none)")

        # git tag --contains <ref>
        return rv

    def _tag_in_branch(self, branch, commits):
        tags = []
        for a, b, c in self._log(branch):
            if a in commits:
                for dec in b.split(", "):
                    if dec.startswith("tag: "):
                        tags.append(dec[5:])
        return tags

    def _log(self, ref, filename=None):
        if filename is None and ref in self._log_cache:
            return self._log_cache[ref]

        args = ["git", "log", "--oneline", "--decorate", ref]
        if filename:
            args.extend(["--", filename])

        data = subprocess.check_output(args, cwd=self.dir, encoding="utf-8")
        # print(data)
        rv = ONELINE_RE.findall(data)
        if filename is None:
            self._log_cache[ref] = rv
        return rv

    def _branch_names(self):
        names = []
        for line in subprocess.check_output(
            ["git", "branch", "-r"], cwd=self.dir, encoding="utf-8"
        ).splitlines():
            parts = line.strip().split()
            if len(parts) == 3 and parts[1] == "->":
                # HEAD
                continue
            elif len(parts) == 1:
                names.append(parts[0])
            else:
                raise ValueError(f"Unknown branch format {line!r}")
        return names

    def _tag_names(self):
        return subprocess.check_output(["git", "tag"], cwd=self.dir,
        encoding="utf-8").splitlines()

    def _cat(self, filename, rev):
        return subprocess.check_output(
            ["git", "show", f"{rev}:{filename}"], cwd=self.dir, encoding="utf-8"
        )

    def _exists(self, hash):
        try:
            subprocess.check_call(
                ["git", "cat-file", "-e", hash], cwd=self.dir)
            return True
        except subprocess.CalledProcessError:
            return False


    def find_best_match(self, archive_root, names, version):
        known = {}
        for a, b in names:
            hash = self._hash_object_path(os.path.join(archive_root, a))
            if self._exists(hash):
                known[hash] = b
            else:
                #print(f"{b} does not exist in this repo with {hash}")
                pass

        # If there are plausible tags, only check that.
        likely_tags = [t for t in self._tag_names() if version in t]
        # right now just (float similarity, ref) but should gain a
        # type too, so that a group of tags can be returned together.
        scores = []
        print(likely_tags)
        if likely_tags:
            for tag in likely_tags:
                matching_hashes = set()
                for line in self._ls_tree(tag):
                    parts = line.split(" ", 2)
                    if parts[1] == "blob":
                        blob_hash, filename = parts[2].split("\t", 1)
                        if blob_hash in known:
                            matching_hashes.add(blob_hash)

                leftover = [k for k in known if k not in matching_hashes]
                #if not leftover:
                #    # TODO multiple identical tags should all be reported.
                #    return (1.0, f"tags/{tag}")
                #print(f"{tag} is close, missing {', '.join(known[x] for x in leftover)}")
                scores.append((1-(len(leftover) / float(len(known))), f"tags/{tag}"))

        scores.sort(reverse=True)
        scores = []
        if not scores or scores[0][0] != 1.0:
            # No perfect match, so basically walk all revs looking for one.

            rev_on_branch = {}
            revs = None
            checked = set()

            for branch in self._branch_names():
                # TODO: Index
                branch_revs = subprocess.check_output(["git", "log",
                "--pretty=%h", branch], cwd=self.dir, encoding="utf-8").split()

                a, b = 0, len(branch_revs)
                print(branch)
                if branch_revs[0] in checked:
                    print("done")
                    continue

                checked.update(branch_revs)
                bad_branch = False

                for h in known:
                    #print(f"top {a} {b}")
                    changed_revs = subprocess.check_output(["git", "log",
                        "--pretty=%h", "--find-object", h, branch],
                        cwd=self.dir,
                        encoding="utf-8").split()
                    # Because multiple files can have the same contents (thus
                    # hash), check whether the newest listed still contains such
                    # an object.  The oldest listed will always be a creation.
                    # For simplicity, we want the range that encloses the
                    # (potentially disjoint) existence of such a file.

                    # TODO: Structured output of _ls_tree
                    if len(changed_revs) == 0:
                        # It's not on this branch (but exists somewhere else);
                        # this can probably become 'break' after testing.
                        print("  bad")
                        bad_branch = True
                        break
                    elif len(changed_revs) == 1 or h in self._ls_tree(changed_revs[0]):
                        # It still has this state.
                        bh = branch_revs.index(changed_revs[-1])
                        if bh < b:
                            b = bh
                        print(f"  1: {ah} {bh}")
                    else:
                        # len(changed_revs) > 1, and it is deleted in changed_revs[0]

                        # It only had this state for a period of time, and does
                        # not any longer.
                        ah = branch_revs.index(changed_revs[0])
                        if ah > a:
                            a = ah
                        bh = branch_revs.index(changed_revs[-1])
                        if bh < b:
                            b = bh
                        print(f"  2: {ah} {bh}")

                    #print(f"bot: {a} {b} {b-a+1}")
                if bad_branch:
                    continue

                if b >= a:
                    for rev in branch_revs[a:b+1]:

                        if rev in rev_on_branch:
                            rev_on_branch[rev].add(branch)
                            continue
                        rev_on_branch[rev] = set(branch)

                        matching_hashes = set()
                        # TODO this could probably be optimized by looking at log
                        # --stat; many fewer forks.
                        for line in self._ls_tree(rev):
                            parts = line.split(" ", 2)
                            if parts[1] == "blob":
                                blob_hash, filename = parts[2].split("\t", 1)
                                if blob_hash in known:
                                    matching_hashes.add(blob_hash)
                        leftover = [k for k in known if k not in matching_hashes]
                        #if not leftover:
                        #    # TODO multiple should be reported
                        #    return (1.0, rev)
                        #print(f"{rev} is close, missing {', '.join(known[x] for x in leftover) or None}")
                        scores.append((1-(len(leftover) / float(len(known))), rev))

        scores.sort(reverse=True)
        prev = None
        last = 0
        for i in range(len(scores)):
            if prev is None:
                prev = scores[i][0]
                last = 0
            if scores[i][0] != prev:
                break
            last = i
            if i > 100:
                break

        return scores[:last+1]


def matchmerge(a, b):
    d = {}
    for k, v in a.items():
        if k in b:
            d[k] = v.intersection(b[k])
        else:
            d[k] = a[k]
    return d
