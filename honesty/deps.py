import logging
import re
from dataclasses import dataclass
from io import StringIO
from typing import List, Optional, Sequence, Tuple
from urllib.request import Request, urlopen
from zipfile import ZipFile

import pkg_resources
from pkginfo.distribution import parse as distribution_parse
from pkginfo.sdist import SDist
from pkginfo.wheel import Wheel

from honesty.cache import Cache
from honesty.releases import FileType, Package, PackageRelease, parse_index

LOG = logging.getLogger(__name__)

# These correlate roughly to the node and edge terminology used by graphviz.


@dataclass
class DepNode:
    name: str
    version: str
    deps: Sequence["DepEdge"] = ()
    has_sdist: bool = False
    dep_extras: Optional[str] = None
    # TODO has_bdist (set of version/platform)?
    done: bool = False


@dataclass
class DepEdge:
    target: DepNode
    constraints: Optional[str] = None
    markers: Optional[str] = None


class DepWalker:
    def __init__(self, starting_package: str) -> None:
        self.nodes: Dict[Tuple[str, str], DepNode] = {}
        self.queue = [(None, starting_package)]
        self.root = None

    def walk(self, include_extras: bool):
        with Cache(fresh_index=True) as cache:
            while self.queue:
                parent, item = self.queue.pop(0)
                LOG.info(f"dequeue {item!r} for {parent.name if parent else None}")

                # This call needs to be serialized on the "main thread" because
                # it will do asyncio behind the scenes.
                package, v, marker, dep_extras, parsed_constraints = self._pick_a_version(item, cache)
                LOG.debug(f"Chose {v}")
                has_sdist = any(
                    fe.file_type == FileType.SDIST for fe in package.releases[v].files
                )
                # TODO: consider canonicalizing name
                key = (package.name, v, dep_extras)

                # TODO: This can be parallelized in threads; we can't just do
                # this all as async because the partial http fetches are done
                # through zipfile calls that aren't async-friendly.
                node = self.nodes.get(key)
                if node is None:
                    # No edges to it yet
                    node = DepNode(package.name, v, [], has_sdist, dep_extras)
                    self.nodes[key] = node

                if node.done:
                    continue

                if parent is None:
                    self.root = node
                else:
                    parent.deps.append(DepEdge(
                        node,
                        ','.join(a+b for a, b in parsed_constraints),
                        marker,
                    ))

                # DO STUFF
                deps = self._fetch_single_deps(package, v, cache)
                LOG.info(f"deps {deps}")
                for d in deps:
                    _, extras, _, m = parse_constraints(d)
                    if ((m is None) or
                        ('extra' not in m) or
                        (dep_extras and 'extra' in m and dep_extras in m) or
                        (include_extras)):

                        self.queue.append((node, d))
                        LOG.info(f"enqueue {d!r} for {node!r}")
                node.done = True

        return self.root

    def _pick_a_version(self, p: str, cache: Cache) -> Tuple[Package, str, str]:
        """
        Given `attrs (==0.1.0)` returns the corresponding release.

        Supports multiple comparisons, and prefers the most recent version.
        """
        a, extras, b, c = parse_constraints(p)
        package = parse_index(a, cache, use_json=True)
        v = _find_compatible_version(package, b)

        return package, v, c, extras, b

    def _fetch_single_deps(self, package: Package, v: str, cache: Cache) -> List[str]:
        # This uses pkginfo same as poetry, but we try to be a lot more efficient at
        # only downloading what we need to.  This is not a solver.

        if package.releases[v].requires is not None:
            # This makes for convenient testing, but Honesty does not currently
            # populate it.  (The API requires a separate request for each
            # version.)
            return package.releases[v].requires

        # Different wheels can have different deps.  We're choosing one arbitrarily.
        for fe in package.releases[v].files:
            if fe.file_type == FileType.BDIST_WHEEL:
                LOG.info(f"wheel {fe.url} {fe.size}")
                if fe.size is None or fe.size > 20000000:
                    # Gigantic wheels we'll pay the remote read penalty
                    # the 'or ()' is needed for numpy
                    return read_metadata_remote_wheel(fe.url) or ()
                else:
                    local_path = cache.fetch(package.name, fe.url)
                    return read_metadata_wheel(local_path) or ()

        for fe in package.releases[v].files:
            if fe.file_type == FileType.SDIST:
                LOG.info("sdist")
                local_path = cache.fetch(pkg=package.name, url=fe.url)
                return read_metadata_sdist(local_path)

        raise ValueError(f"No whl/sdist for {package.name}")



LINE_RE = re.compile(r"^([\w.-]+)(?:\[(\w+)\])?\s*(?:\((.*?)\))?\s*(;.*)?")
OPERATOR_RE = re.compile(r"^([!<>=~]+)\s*(.*)$")


def parse_constraints(line: str) -> Tuple[str, str, List[Tuple[str, str]], Optional[str]]:
    # Surely there's a library function that already does this, and does it
    # better, and with tests.
    #
    # returns: pkgname, list of (operator, ver), optional markers
    name, extra, constraint_str, markers = LINE_RE.match(line.strip()).groups()
    constraints = ()
    if constraint_str:
        constraints = [OPERATOR_RE.match(c).groups() for c in constraint_str.split(",")]

    return name, extra, constraints, markers


def read_metadata_sdist(path):
    return SDist(str(path)).requires_dist


def read_metadata_wheel(path):
    return Wheel(str(path)).requires_dist

def read_metadata_remote_wheel(url):
    f = SeekableHttpFile(url)
    z = ZipFile(f)

    # Favors the shortest name; most wheels only have one.
    metadata_names = [name for name in z.namelist() if name.endswith("/METADATA")]
    metadata_names.sort(key=len)

    assert len(metadata_names) > 0
    # TODO: This does not go through the Wheel path from pkginfo because it
    # requires a filename on disk.
    data = z.read(metadata_names[0])
    metadata = distribution_parse(StringIO(data.decode()))
    return metadata.get_all("Requires-Dist")

    raise ValueError("No metadata")


def _find_compatible_version(
    package: Package, constraints: Sequence[Tuple[str, str]]
) -> str:
    # This is an 80% solution that works on enough of the real world to be
    # useful.  Doing this remotely "correct" involves way too much code.  The
    # next thing I would add is special casing prereleases.
    # https://github.com/python-poetry/poetry/blob/master/poetry/version/specifiers.py#L312

    possible = {k: pkg_resources.parse_version(k) for k in package.releases}

    for (operator, ver) in constraints:
        v = pkg_resources.parse_version(ver)
        # print(possible, operator, repr(v))
        if not v.is_prerelease:
            to_remove = [k for k in possible if possible[k].is_prerelease]
            for t in to_remove:
                del possible[t]

        if operator == "==":
            to_remove = [k for k in possible if not possible[k] == v]
            for t in to_remove:
                del possible[t]
        elif operator == ">=":
            to_remove = [k for k in possible if not possible[k] >= v]
            for t in to_remove:
                del possible[t]
        elif operator == "<=":
            to_remove = [k for k in possible if not possible[k] <= v]
            for t in to_remove:
                del possible[t]
        elif operator == "<":
            to_remove = [k for k in possible if not possible[k] < v]
            for t in to_remove:
                del possible[t]
        elif operator == ">":
            to_remove = [k for k in possible if not possible[k] > v]
            for t in to_remove:
                del possible[t]
        elif operator == "!=":
            to_remove = [k for k in possible if not possible[k] != v]
            for t in to_remove:
                del possible[t]
        elif operator == "~=":
            # >= x, < x+1
            pieces = ver.split(".")
            pieces[-2] = str(int(pieces[-2])+1)
            v2 = pkg_resources.parse_version('.'.join(pieces))
            #print(repr(v2))
            to_remove = [
                k for k in possible if not (possible[k] >= v and possible[k] < v2)
            ]
            for t in to_remove:
                del possible[t]
        else:
            raise NotImplementedError((operator, ver))

    if not possible:
        raise ValueError(
            f"Impossible constraint left no versions for {package.name}: {constraints}"
        )

    return sorted(possible.items(), key=lambda i: i[1])[-1][0]


class SeekableHttpFile:
    def __init__(self, url: str) -> None:
        self.url = url
        self.pos = 0
        self.length = -1
        LOG.debug("head")
        # Optimistically read the last KB, which will save a HEAD and a couple
        # of reads from doing it the obvious way.
        optimistic = 256000
        h = "bytes=-%d" % optimistic
        LOG.debug(f"Header {h!r}")
        with urlopen(
            Request(
                url,
                headers={"Range": h}
            )
        ) as resp:
            self.length = int(resp.headers["Content-Range"].split("/")[1])
            LOG.debug(resp.headers["Content-Range"])
            self.end_cache = resp.read()
            self.end_cache_start = self.length - optimistic
            print(type(self.end_cache), self.end_cache_start, url)
            assert self.end_cache_start > 0

            # TODO verify ETag/Last-Modified don't change.

    def seek(self, pos, whence=0):
        LOG.debug(f"seek {pos} {whence}")
        # TODO clamp/error
        if whence == 0:
            self.pos = pos
        elif whence == 1:
            self.pos += pos
        elif whence == 2:
            self.pos = self.length + pos
        else:
            raise ValueError(f"Invalid value for whence: {whence!r}")

    def tell(self):
        LOG.debug("tell")
        return self.pos

    def read(self, n=-1):
        LOG.debug(f"read {n} @ {self.length-self.pos}")
        if n == -1:
            n = self.length - self.pos
        if n == 0:
            return b""

        p = self.pos - self.end_cache_start
        if p >= 0:
            LOG.debug(f"  satisfied from cache @ {p}")
            self.pos += n
            return self.end_cache[p:p+n]

        with urlopen(
            Request(
                self.url,
                headers={"Range": "bytes=%d-%d" % (self.pos, self.pos + n - 1)},
            )
        ) as resp:
            data = resp.read()

        self.pos += n
        if len(data) != n:
            raise ValueError("Truncated read", len(data), n)

        return data

    def seekable(self):
        return True
