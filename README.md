# Honesty

There's a long tail of people doing interesting/sketchy things to packages on
pypi.  Most aren't malicious, but this project gives you an easy way to check
for some of the obvious ways that packages might be tampered with.

# Usage

```
honesty list <package name>
honesty check <package name>[==version|==*] [--verbose]
honesty download <package name>[==version|==*] [--dest=some-path/]
honesty extract <package name>[==version|==*] [--dest=some-path/]
honesty license <package name>[==version|==*]

(provisional)
honesty ispep517 <package name>[==version|==*]
honesty native <package name>[==version|==*]
honesty age <package name>[==version|==*]
honesty deps <package name>[==version|==*]
honesty revs <package name>[==version|==*]
```

It will store a package cache by default under `~/.cache/honesty/pypi` but you
can change that with `HONESTY_CACHE` env var.  If you have a local bandersnatch,
specify `HONESTY_INDEX_URL` to your `/simple/` url.


# Exit Status of 'check'

These are bit flags to make sense when there are multiple problems.  If you pass
`*` for version, they are or'd together.

```
0   if only sdist or everything matches
1   if only bdist
2   (reserved for future "extraction error")
4   some .py from bdist not in sdist
8   some .py files present with same name but different hash in sdist (common
    when using versioneer or 2to3)
```


# API

The user-facing API is intended to be used to analyze metadata and download
sdists.  It is somewhat provisional, in that the exceptions raised are not
well-defined.

```
from honesty.cache import Cache
from honesty.releases import async_parse_index
from honesty.api import async_download_one
async def foo(pkgname, pkgversion):
  with Cache() as c:
      pkg = await async_parse_index(pkgname, c, use_json=True)
      path = async_download_one(pkg, pkgversion, cache=c)
```


# License

Honesty is copyright [Tim Hatch](http://timhatch.com/), and licensed under
the MIT license.  I am providing code in this repository to you under an open
source license.  This is my personal repository; the license you receive to
my code is from me and not from my employer. See the `LICENSE` file for details.
