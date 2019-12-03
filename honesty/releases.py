import asyncio
import enum
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple

from .cache import Cache

# Apologies in advance, "parsing" html via regex
CHECKSUM_RE = re.compile(
    r'\A(?P<url>[^"#]+\/(?P<basename>[^#]+))#(?P<checksum>[^="]+=[a-f0-9]+)\Z'
)
NUMERIC_VERSION = re.compile(
    r"^(?P<package>.*?)-(?P<version>[0-9][^-]*?)"
    r"(?P<suffix>(?P<platform>\.macosx|\.linux|\.cygwin|\.win(?:32|xp|))?-.*)?$"
)


SDIST_EXTENSIONS = (".tar.gz", ".zip", ".tar.bz2")


# This list matches warehouse/packaging/models.py with the addition of UNKNOWN.
#
# Platform (in the case of bdist_dumb) is not currently stored anywhere but
# doesn't belong in this enum.

# Some rough popularity numbers at the time of writing (top 5k packages, with
# some double counts for the current release of each):
#
#     31 "bdist_dmg"
#     41 "bdist_rpm"
#    176 "bdist_msi"
#    309 "bdist_dumb"
#   6909 "bdist_wininst"
#  18201 "bdist_egg"
# 138984 "sdist"
# 155904 "bdist_wheel"


class FileType(enum.IntEnum):
    UNKNOWN = 0
    SDIST = 1  # .tar.gz or .zip (or for packages like Twisted, .tar.bz2)
    BDIST_DMG = 2  # .dmg
    BDIST_DUMB = 3  # -(platform).tar.gz
    BDIST_EGG = 4  # .egg
    BDIST_MSI = 5  # .msi
    BDIST_RPM = 6  # .rpm
    BDIST_WHEEL = 7  # .whl
    BDIST_WININST = 8  # .exe


class UnexpectedFilename(Exception):
    pass


def guess_file_type(filename: str) -> FileType:
    if filename.endswith(".egg"):
        return FileType.BDIST_EGG
    elif filename.endswith(".whl"):
        return FileType.BDIST_WHEEL
    elif filename.endswith(".exe"):
        return FileType.BDIST_WININST
    elif filename.endswith(".msi"):
        return FileType.BDIST_MSI
    elif filename.endswith(".rpm"):
        return FileType.BDIST_RPM
    elif filename.endswith(".dmg"):
        return FileType.BDIST_DMG
    elif filename.endswith(SDIST_EXTENSIONS):
        filename = remove_suffix(filename)
        match = NUMERIC_VERSION.match(filename)
        # Some oddly-named files are not likely to be loaded by pip either.
        if match is None:
            raise UnexpectedFilename(filename)
        # bdist_dumb can't be easily discerned
        if match.group("platform"):
            return FileType.BDIST_DUMB
        elif match.group("suffix") and match.group("suffix").startswith("-macosx"):
            return FileType.BDIST_DUMB
        return FileType.SDIST
    else:
        return FileType.UNKNOWN


@dataclass
class FileEntry:
    url: str  # https://files.pythonhosted.../foo-1.0.tgz
    basename: str  # foo-1.0.tgz
    checksum: str  # 'sha256=<foo>'
    file_type: FileType
    version: str  # TODO: better type
    requires_python: Optional[str] = None  # '>=3.6'
    python_version: Optional[str] = None  # 'py2.py3' or 'source'
    # TODO extract upload date?

    @classmethod
    def from_attrs(cls, attrs: List[Tuple[str, Optional[str]]]) -> "FileEntry":
        """
        Given the <a> element's attrs from parsing the simple html index,
        returns a new FileEntry.
        """
        d = dict(attrs)
        if d["href"] is None:  # pragma: no cover
            raise KeyError("Empty href")
        m = CHECKSUM_RE.match(d["href"])
        if m is None:
            raise UnexpectedFilename(d["href"])
        url = m.group("url")
        basename = m.group("basename")
        checksum = m.group("checksum")

        return cls(
            url=url,
            basename=basename,
            checksum=checksum,
            file_type=guess_file_type(basename),
            version=guess_version(basename)[1],
            requires_python=d.get("data-requires-python"),
        )


@dataclass
class PackageRelease:
    version: str
    files: List[FileEntry]


@dataclass
class Package:
    name: str
    releases: Dict[str, PackageRelease]


def remove_suffix(basename: str) -> str:
    suffixes = [
        ".egg",
        ".whl",
        ".zip",
        ".gz",
        ".bz2",
        ".tar",
        ".exe",
        ".msi",
        ".rpm",
        ".dmg",
    ]
    for s in suffixes:
        if basename.endswith(s):
            basename = basename[: -len(s)]
    return basename


# TODO itu-r-468-weighting-1.0.3.tar.gz
# TODO uttt-0.3-1.tar.gz
def guess_version(basename: str) -> Tuple[str, str]:
    """
    Returns (package name, version) or raises.
    """
    # This should use whatever setuptools/pip/etc use, but I spent about 10
    # minutes and couldn't find it tonight.
    basename = remove_suffix(basename)

    match = NUMERIC_VERSION.match(basename)
    if not match:
        raise UnexpectedFilename(basename)
    return match.group(1), match.group(2)


class LinkGatherer(HTMLParser):
    def __init__(self, strict: bool = False):
        super().__init__()
        self.entries: List[FileEntry] = []
        self.strict = strict

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag == "a":
            try:
                fe = FileEntry.from_attrs(attrs)
            except UnexpectedFilename:
                if not self.strict:
                    return
                raise

            self.entries.append(fe)


def parse_index(pkg: str, cache: Cache, strict: bool = False) -> Package:
    loop = asyncio.get_event_loop()
    package: Package = loop.run_until_complete(async_parse_index(pkg, cache, strict))
    return package


async def async_parse_index(pkg: str, cache: Cache, strict: bool = False) -> Package:
    package = Package(name=pkg, releases={})
    with open(await cache.async_fetch(pkg, url=None)) as f:
        gatherer = LinkGatherer(strict)
        gatherer.feed(f.read())
        for fe in gatherer.entries:
            v = fe.version
            if v not in package.releases:
                package.releases[v] = PackageRelease(version=v, files=[])
            package.releases[v].files.append(fe)

    return package
