import unittest
import urllib.error
from typing import Any
from unittest.mock import patch

from honesty.deps import (
    DepWalker,
    SeekableHttpFile,
    _find_compatible_version,
    convert_sdist_requires,
    parse_constraints,
)
from honesty.releases import Package, PackageRelease


class ConstraintsTest(unittest.TestCase):
    def test_basic(self) -> None:
        con = parse_constraints("foo")
        self.assertEqual("foo", con.name)
        self.assertEqual("", str(con.specifiers))
        self.assertEqual(None, con.markers)

    def test_operator(self) -> None:
        con = parse_constraints("foo (==1.0,<2)")
        self.assertEqual("foo", con.name)
        self.assertEqual("==1.0,<2", str(con.specifiers))
        self.assertEqual(None, con.markers)

    def test_whitespace(self) -> None:
        con = parse_constraints("foo (== 1.0) ")
        self.assertEqual("foo", con.name)
        self.assertEqual("==1.0", str(con.specifiers))
        self.assertEqual(None, con.markers)

    def test_markers(self) -> None:
        con = parse_constraints('foo ; python_version > "3"')
        self.assertEqual("foo", con.name)
        self.assertEqual("", str(con.specifiers))
        self.assertEqual('; python_version > "3"', con.markers)


class ConvertSdistRequiresTest(unittest.TestCase):
    def test_all(self) -> None:
        self.assertEqual(
            ["a"], convert_sdist_requires("a\n"),
        )
        self.assertEqual(
            ["a; python_version < '3.4'"],
            convert_sdist_requires("[:python_version < '3.4']\na\n"),
        )


FOO_PACKAGE = Package(
    name="foo",
    releases={"1.0": PackageRelease("1.0", []), "2.0": PackageRelease("2.0", []),},
)

BAR_PACKAGE = Package(
    name="bar", releases={"1.0": PackageRelease("1.0", [], requires=["foo"]),}
)


class FindCompatibleVersionTest(unittest.TestCase):
    def test_basic(self) -> None:
        v = _find_compatible_version(FOO_PACKAGE, [("==", "1.0")], None)
        self.assertEqual("1.0", v)
        v = _find_compatible_version(FOO_PACKAGE, [("==", "2.0")], None)
        self.assertEqual("2.0", v)
        v = _find_compatible_version(FOO_PACKAGE, [(">=", "2.0")], None)
        self.assertEqual("2.0", v)
        v = _find_compatible_version(FOO_PACKAGE, [("<=", "2.0")], None)
        self.assertEqual("2.0", v)
        v = _find_compatible_version(FOO_PACKAGE, [("<=", "1.0")], None)
        self.assertEqual("1.0", v)
        v = _find_compatible_version(FOO_PACKAGE, [("!=", "2.0")], None)
        self.assertEqual("1.0", v)
        with self.assertRaises(ValueError):
            _find_compatible_version(FOO_PACKAGE, [("<", "1.0")], None)
        with self.assertRaises(NotImplementedError):
            _find_compatible_version(FOO_PACKAGE, [("$", "1.0")], None)


class TestSeekableHttpFile(unittest.TestCase):
    def test_live(self) -> None:
        # This test requires internet access.
        f = SeekableHttpFile("http://timhatch.com/projects/http-tests/sequence_100.txt")
        self.assertEqual(0, f.pos)
        self.assertEqual(292, f.length)
        self.assertEqual(b"1\n", f.read(2))
        f.seek(-4, 2)
        self.assertEqual(b"100\n", f.read(4))
        f.seek(-4, 2)
        self.assertEqual(b"100\n", f.read())
        self.assertEqual(292, f.tell())
        self.assertTrue(f.seekable())

    def test_live_404(self) -> None:
        # This test requires internet access.
        with self.assertRaises(urllib.error.HTTPError):
            SeekableHttpFile(
                "http://timhatch.com/projects/http-tests/response/?code=404"
            )


A_PACKAGE = Package(
    name="a", releases={"1.0": PackageRelease("1.0", [], ["b (==1.0)"]),}
)
B_PACKAGE = Package(
    name="b",
    releases={
        "1.0": PackageRelease("1.0", [], ["c"]),
        "2.0": PackageRelease("2.0", [], []),
    },
)
C_PACKAGE = Package(name="c", releases={"1.0": PackageRelease("1.0", [], []),})


class DepWalkerTest(unittest.TestCase):
    @patch("honesty.deps.parse_index")
    def test_walk(self, parse_mock: Any) -> None:
        def parse(pkg: str, cache: Any, use_json: bool = False) -> Package:
            if pkg == "a":
                return A_PACKAGE
            elif pkg == "b":
                return B_PACKAGE
            elif pkg == "c":
                return C_PACKAGE
            else:
                raise NotImplementedError(f"Unknown package {pkg}")

        parse_mock.side_effect = parse

        d = DepWalker("a", "3.6.0")
        d.walk(include_extras=False)

        print(d.root)
        assert d.root is not None
        self.assertEqual("a", d.root.name)
        self.assertEqual("1.0", d.root.version)
        self.assertEqual(True, d.root.done)

        self.assertEqual(1, len(d.root.deps))
        self.assertEqual("b", d.root.deps[0].target.name)
        self.assertEqual(1, len(d.root.deps[0].target.deps))
        self.assertEqual("c", d.root.deps[0].target.deps[0].target.name)
        self.assertEqual(0, len(d.root.deps[0].target.deps[0].target.deps))
