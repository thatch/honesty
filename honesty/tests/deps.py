import urllib.error
import unittest
from unittest.mock import patch

from honesty.deps import parse_constraints, _find_compatible_version, SeekableHttpFile, DepWalker
from honesty.releases import Package, PackageRelease

class ConstraintsTest(unittest.TestCase):
    def test_basic(self):
        a, b, c = parse_constraints('foo')
        self.assertEqual('foo', a)
        self.assertEqual([], b)
        self.assertEqual(None, c)

    def test_operator(self):
        a, b, c = parse_constraints('foo (==1.0,<2)')
        self.assertEqual('foo', a)
        self.assertEqual([('==', '1.0'), ('<', '2')], b)
        self.assertEqual(None, c)

    def test_whitespace(self):
        a, b, c = parse_constraints('foo (== 1.0) ')
        self.assertEqual('foo', a)
        self.assertEqual([('==', '1.0')], b)
        self.assertEqual(None, c)

    def test_markers(self):
        a, b, c = parse_constraints('foo ; python_version > "3"')
        self.assertEqual('foo', a)
        self.assertEqual([], b)
        self.assertEqual('python_version > "3"', c)

FOO_PACKAGE = Package(
    name="foo",
    releases={
        "1.0": PackageRelease("1.0", []),
        "2.0": PackageRelease("2.0", []),
    }
)

BAR_PACKAGE = Package(
    name="bar",
    releases={
        "1.0": PackageRelease("1.0", [], requires=["foo"]),
    }
)

class FindCompatibleVersionTest(unittest.TestCase):
    def test_basic(self):
        v = _find_compatible_version(FOO_PACKAGE, [("==", "1.0")])
        self.assertEqual("1.0", v)
        v = _find_compatible_version(FOO_PACKAGE, [("==", "2.0")])
        self.assertEqual("2.0", v)
        v = _find_compatible_version(FOO_PACKAGE, [(">=", "2.0")])
        self.assertEqual("2.0", v)
        v = _find_compatible_version(FOO_PACKAGE, [("<=", "2.0")])
        self.assertEqual("2.0", v)
        v = _find_compatible_version(FOO_PACKAGE, [("<=", "1.0")])
        self.assertEqual("1.0", v)
        v = _find_compatible_version(FOO_PACKAGE, [("!=", "2.0")])
        self.assertEqual("1.0", v)
        with self.assertRaises(ValueError):
            _find_compatible_version(FOO_PACKAGE, [("<", "1.0")])
        with self.assertRaises(NotImplementedError):
            _find_compatible_version(FOO_PACKAGE, [("$", "1.0")])

class TestSeekableHttpFile(unittest.TestCase):
    def test_live(self):
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

    def test_live_404(self):
        # This test requires internet access.
        with self.assertRaises(urllib.error.HTTPError):
            f = SeekableHttpFile("http://timhatch.com/projects/http-tests/response/?code=404")

A_PACKAGE = Package(
    name="a",
    releases={
        "1.0": PackageRelease("1.0", [], ["b (==1.0)"]),
    }
)
B_PACKAGE = Package(
    name="b",
    releases={
        "1.0": PackageRelease("1.0", [], ["c"]),
        "2.0": PackageRelease("2.0", [], []),
    }
)
C_PACKAGE = Package(
    name="c",
    releases={
        "1.0": PackageRelease("1.0", [], []),
    }
)

class DepWalkerTest(unittest.TestCase):
    @patch("honesty.deps.parse_index")
    def test_walk(self, parse_mock):
        def parse(pkg, cache, use_json=False):
            if pkg == "a":
                return A_PACKAGE
            elif pkg == "b":
                return B_PACKAGE
            elif pkg == "c":
                return C_PACKAGE

        parse_mock.side_effect = parse

        d = DepWalker("a")
        d.walk()

        print(d.root)
        self.assertEqual("a", d.root.name)
        self.assertEqual("1.0", d.root.version)
        self.assertEqual(True, d.root.done)

        self.assertEqual(1, len(d.root.deps))
        self.assertEqual("b", d.root.deps[0].target.name)
        self.assertEqual(1, len(d.root.deps[0].target.deps))
        self.assertEqual("c", d.root.deps[0].target.deps[0].target.name)
        self.assertEqual(0, len(d.root.deps[0].target.deps[0].target.deps))
