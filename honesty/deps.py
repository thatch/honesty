import logging
import operator
import os
import re
import tarfile
import zipfile
from dataclasses import asdict, dataclass, field
from io import StringIO
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.request import Request, urlopen
from zipfile import ZipFile

from packaging.markers import Marker
from packaging.specifiers import SpecifierSet
from packaging.version import Version
from pkginfo.distribution import parse as distribution_parse
from pkginfo.sdist import SDist
from pkginfo.wheel import Wheel

from honesty.cache import Cache
from honesty.releases import FileType, Package, parse_index

LOG = logging.getLogger(__name__)

# These correlate roughly to the node and edge terminology used by graphviz.


@dataclass
class DepNode:
    name: str
    version: str
    deps: List["DepEdge"] = field(default_factory=list)
    has_sdist: bool = False
    has_bdist: bool = False
    dep_extras: Optional[str] = None
    # TODO has_bdist (set of version/platform)?
    done: bool = False


@dataclass
class DepEdge:
    target: DepNode
    constraints: Optional[str] = None
    markers: Optional[str] = None


@dataclass
class EnvironmentMarkers:
    os_name: str = "posix"
    sys_platform: str = "linux"
    platform_machine: str = "x86_64"
    platform_python_implementation: str = "CPython"
    platform_release: Optional[str] = None
    platform_system: str = "Linux"
    platform_version: Optional[str] = None
    python_version: Optional[str] = None
    python_full_version: Optional[str] = None
    implementation_name: str = "cpython"
    extra: Optional[str] = None  # ??


@dataclass
class Constraint:
    name: str
    extra: str
    specifiers: SpecifierSet
    markers: Optional[str]  # If set, starts with ';'


OPERATOR_TABLE = {
    "<": operator.lt,
    ">": operator.gt,
    "<=": operator.le,
    ">=": operator.ge,
    "==": operator.eq,
    "!=": operator.ne,
}


class DepWalker:
    def __init__(self, starting_package: str, python_version: str) -> None:
        self.nodes: Dict[Tuple[str, str, str], DepNode] = {}
        self.queue: List[Tuple[Optional[DepNode], str]] = [(None, starting_package)]
        self.root: Optional[DepNode] = None
        # TODO support unusual versions.
        t = ".".join(python_version.split(".")[:2])
        self.markers = EnvironmentMarkers(
            python_version=t, python_full_version=python_version,
        )
        self.python_version = Version(python_version)

    def walk(self, include_extras: bool) -> DepNode:
        with Cache(fresh_index=True) as cache:
            while self.queue:
                parent, item = self.queue.pop(0)
                if parent is not None:
                    parent_str = parent.name
                else:
                    parent_str = "(root)"
                LOG.info(f"dequeue {item!r} for {parent_str}")

                # This call needs to be serialized on the "main thread" because
                # it will do asyncio behind the scenes.
                con = parse_constraints(item)
                # The python_version marker is by far the most widely-used.
                if con.markers and not self._do_markers_match(con.markers):
                    LOG.debug(f"Skip {con.name} {con.markers}")
                    continue

                (package, v) = self._pick_a_version(con, cache)
                LOG.debug(f"Chose {v}")

                has_sdist = any(
                    fe.file_type == FileType.SDIST for fe in package.releases[v].files
                )
                # TODO: consider eggs or bdist_dumb as valid?  Can pip still use them?
                has_bdist = any(
                    fe.file_type == FileType.BDIST_WHEEL
                    for fe in package.releases[v].files
                )

                # TODO: consider canonicalizing name
                key = (package.name, v, con.extra)

                # TODO: This can be parallelized in threads; we can't just do
                # this all as async because the partial http fetches are done
                # through zipfile calls that aren't async-friendly.
                node = self.nodes.get(key)
                if node is None:
                    # No edges to it yet
                    node = DepNode(
                        package.name,
                        v,
                        [],
                        has_sdist=has_sdist,
                        has_bdist=has_bdist,
                        dep_extras=con.extra,
                    )
                    self.nodes[key] = node

                if parent is None:
                    self.root = node
                else:
                    parent.deps.append(DepEdge(node, str(con.specifiers), con.markers,))

                if node.done:
                    continue

                # DO STUFF
                deps = self._fetch_single_deps(package, v, cache)
                LOG.info(f"deps {deps}")
                for d in deps:
                    dep_con = parse_constraints(d)

                    if (
                        (dep_con.markers is None)
                        or ("extra" not in dep_con.markers)
                        or (
                            con.extra
                            and "extra" in dep_con.markers
                            and con.extra in dep_con.markers
                        )
                        or (include_extras)
                    ):

                        self.queue.append((node, d))
                        LOG.info(f"enqueue {d!r} for {node!r}")
                node.done = True

        assert self.root is not None
        return self.root

    def _do_markers_match(self, markers: str, extras: Sequence[str] = ()) -> bool:
        env = dict(**asdict(self.markers), extras=Extras(extras))
        m = Marker(markers[1:])
        return m.evaluate(env)

    def _pick_a_version(
        self, con: Constraint, cache: Cache
    ) -> Tuple[Package, str, Constraint]:
        """
        Given `attrs (==0.1.0)` returns the corresponding release.

        Supports multiple comparisons, and prefers the most recent version.
        """
        package = parse_index(con.name, cache, use_json=True)
        # TODO allow specifying a callback to find installed versions for
        # equivalent of as-needed upgrade instead of decision in a vacuum.
        v = _find_compatible_version(package, con.specifiers, self.python_version)

        return package, v

    def _fetch_single_deps(
        self, package: Package, v: str, cache: Cache
    ) -> Sequence[str]:
        # This uses pkginfo same as poetry, but we try to be a lot more efficient at
        # only downloading what we need to.  This is not a solver.

        tmp = package.releases[v].requires
        if tmp is not None:
            # This makes for convenient testing, but Honesty does not currently
            # populate it.  (The API requires a separate request for each
            # version.)
            return tmp

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


LINE_RE = re.compile(r"^([\w.-]+)(?:\[([\w,]+)\])?\s*(?:\(?(.*?)\)?)?\s*(;.*)?$")


def parse_constraints(line: str,) -> Constraint:
    # Surely there's a library function that already does this, and does it
    # better, and with tests.
    #
    # returns: pkgname, list of (operator, ver), optional markers
    match = LINE_RE.match(line.strip())
    if match is None:
        raise ValueError(f"No operator match in {line.strip()!r}")

    # TODO: extra can have multiple -- apache-airflow==1.10.5
    name, extra, constraint_str, markers = match.groups()
    specifiers = SpecifierSet(constraint_str)

    return Constraint(name=name, extra=extra, specifiers=specifiers, markers=markers,)


def read_metadata_sdist(path: "os.PathLike[str]") -> List[str]:
    # pkginfo.sdist.SDist only parses PKG-INFO, but requirements are stored in
    # *.egg-info/requires.txt instead.  Duplicating some logic here similar to
    # pkginfo.  Avoid testdata like
    # distlib-0.3.0/tests/fake_dists/banana-0.4.egg/EGG-INFO/requires.txt

    # distutils.setup() doesn't appear to write requires.txt (PyMeeus)

    # TODO: We already have type guessing and extraction logic that improves
    # subsequent runs.  Just use that.
    ext = str(path).split(".")[-1]
    if ext == "zip":
        archive = zipfile.ZipFile(path)
        names = [
            name
            for name in archive.namelist()
            if name.endswith("/requires.txt") and name.count("/") <= 2
        ]
        if not names:
            # print(path, "no requires.txt")
            return []
        names.sort(key=len)
        data = archive.read(names[0])
    elif ext in ("gz", "bz2", "tgz"):
        archive2 = tarfile.TarFile.open(path)
        names = [
            name
            for name in archive2.getnames()
            if name.endswith("/requires.txt") and name.count("/") <= 2
        ]
        if not names:
            # print(path, "no requires.txt")
            return []
        names.sort(key=len)
        data = archive2.extractfile(names[0]).read()  # type: ignore
    else:
        raise ValueError("Unknown extension")

    return convert_sdist_requires(data.decode())


def convert_sdist_requires(data: str) -> List[str]:
    # This is reverse engineered from looking at a couple examples, but there
    # does not appear to be a formal spec.  Mentioned at
    # https://setuptools.readthedocs.io/en/latest/formats.html#requires-txt
    current_markers = None
    lst: List[str] = []
    for line in data.splitlines():
        line = line.strip()
        if not line:
            continue
        elif line[:1] == "[" and line[-1:] == "]":
            current_markers = line[1:-1]
            if ":" in current_markers:
                # absl-py==0.9.0 and requests==2.22.0 are good examples of this
                extra, markers = current_markers.split(":", 1)
                if extra:
                    current_markers = f"({markers}) and extra == {extra!r}"
                else:
                    current_markers = markers
            else:
                # this is an extras_require
                current_markers = f"extra == {current_markers!r}"
        else:
            if current_markers:
                lst.append(f"{line}; {current_markers}")
            else:
                lst.append(line)
    return lst


def read_metadata_wheel(path: "os.PathLike[str]") -> List[str]:
    tmp: List[str] = Wheel(str(path)).requires_dist
    return tmp


def read_metadata_remote_wheel(url: str) -> List[str]:
    # TODO: Convince mypy that SeekableHttpFile is an IO[Bytes]
    f = SeekableHttpFile(url)
    z = ZipFile(f)  # type: ignore

    # Favors the shortest name; most wheels only have one.
    metadata_names = [name for name in z.namelist() if name.endswith("/METADATA")]
    metadata_names.sort(key=len)

    assert len(metadata_names) > 0
    # TODO: This does not go through the Wheel path from pkginfo because it
    # requires a filename on disk.
    data = z.read(metadata_names[0])
    metadata = distribution_parse(StringIO(data.decode()))
    return metadata.get_all("Requires-Dist")  # type: ignore

    raise ValueError("No metadata")


def _find_compatible_version(
    package: Package, specifiers: SpecifierSet, python_version: Version
) -> str:
    # Luckily we can fall back on `packaging` here, because "correct" parsing is a
    # lot of code.  Legacy versions are already likely thrown away in
    # `parse_index`.

    # First filter out by requires_python; this lets us give a more descriptive
    # error when the package is completely incompatible.
    # TODO: Give a better error when there's a release with no artifacts.
    possible = []
    for k, v in package.releases.items():
        # requires_python is set on FileEntry, not PackageRelease
        # arbitrarily take the first one.
        requires_python = None
        for fe in v.files:
            if fe.requires_python:
                requires_python = SpecifierSet(fe.requires_python)
                break
        # LOG.debug(f"CHECK {package.name} {python_version} against {requires_python}: {k}")
        if not requires_python or python_version in requires_python:
            # LOG.debug("  include")
            possible.append(k)
    if not possible:
        raise ValueError(f"{package.name} incompatible with {python_version}")

    possible = list(specifiers.filter(possible))
    if not possible:
        raise ValueError(
            f"{package.name} has no {requires_python} compatible release with constraint {specifiers}"
        )

    return sorted(possible, key=Version)[-1]


CONTENT_RANGE_RE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")


class SeekableHttpFile:
    def __init__(self, url: str) -> None:
        self.url = url
        self.pos = 0
        self.length = -1
        LOG.debug("head")
        # Optimistically read the last few KB, which can satisfy both finding
        # the length and the first couple of reads (2 bytes from the end and 22
        # bytes from the end).  This value was chosen looking at scipy and saves
        # another half-second for me.
        optimistic = 256000
        h = "bytes=-%d" % optimistic
        with urlopen(Request(url, headers={"Range": h})) as resp:
            match = CONTENT_RANGE_RE.match(resp.headers["Content-Range"])
            assert match is not None
            start, end, length = match.groups()
            self.length = int(length)
            LOG.debug(resp.headers["Content-Range"])
            self.end_cache: bytes = resp.read()
            self.end_cache_start = int(start)
            # print(type(self.end_cache), self.end_cache_start, url)
            assert self.end_cache_start >= 0

            # TODO verify ETag/Last-Modified don't change.

    def seek(self, pos: int, whence: int = 0) -> None:
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

    def tell(self) -> int:
        LOG.debug("tell")
        return self.pos

    def read(self, n: int = -1) -> bytes:
        LOG.debug(f"read {n} @ {self.length-self.pos}")
        if n == -1:
            n = self.length - self.pos
        if n == 0:
            return b""

        p = self.pos - self.end_cache_start
        if p >= 0:
            LOG.debug(f"  satisfied from cache @ {p}")
            self.pos += n
            return self.end_cache[p : p + n]

        with urlopen(
            Request(
                self.url,
                headers={"Range": "bytes=%d-%d" % (self.pos, self.pos + n - 1)},
            )
        ) as resp:
            data: bytes = resp.read()

        self.pos += n
        if len(data) != n:
            raise ValueError("Truncated read", len(data), n)

        return data

    def seekable(self) -> bool:
        return True


class Extras:
    """
    This is a tiny class that lets us get 'extra == "foo"' working for
    `packaging.markers`
    """

    def __init__(self, extras):
        self.extras = extras

    def __eq__(self, other):
        return other in self.extras
