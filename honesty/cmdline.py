import asyncio
import functools
import json
import logging
import os.path
import shutil
import sys
from datetime import datetime, timezone
from enum import Enum, IntEnum
from pathlib import Path
from typing import Any, List, Optional, Set, Tuple

import click
import pkg_resources

from honesty.__version__ import __version__
from honesty.api import async_download_many
from honesty.archive import extract_and_get_names
from honesty.cache import Cache
from honesty.checker import guess_license, has_nativemodules, is_pep517, run_checker
from honesty.deps import DepEdge, DepNode, DepWalker
from honesty.releases import FileType, Package, async_parse_index, parse_index


# TODO type
def wrap_async(coro: Any) -> Any:
    @functools.wraps(coro)
    def inner(*args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro(*args, **kwargs))

    return inner


def dataclass_default(obj: Any) -> Any:
    if hasattr(obj, "__dataclass_fields__"):
        return obj.__dict__
    elif isinstance(obj, (Enum, IntEnum)):
        return obj.name
    elif isinstance(obj, datetime):
        return str(obj)
    else:
        raise TypeError(obj)


@click.group()
@click.version_option(__version__, prog_name="honesty")
def cli() -> None:
    pass


@cli.command(help="List available archives")
@click.option("--fresh", "-f", is_flag=True, type=bool)
@click.option("--nouse_json", is_flag=True, type=bool)
@click.option("--as_json", is_flag=True, type=bool)
@click.argument("package_name")
@wrap_async
async def list(fresh: bool, nouse_json: bool, as_json: bool, package_name: str) -> None:
    async with Cache(fresh_index=fresh) as cache:
        package = await async_parse_index(package_name, cache, use_json=not nouse_json)

    if as_json:
        for k, v in package.releases.items():
            print(json.dumps(v, default=dataclass_default, sort_keys=True))
    else:
        print(f"package {package.name}")
        print("releases:")
        for k, v in package.releases.items():
            print(f"  {k}:")
            for f in v.files:
                if f.requires_python:
                    print(f"    {f.basename} (requires_python {f.requires_python})")
                else:
                    print(f"    {f.basename}")


@cli.command(help="Check for consistency among archives")
@click.option("--verbose", "-v", is_flag=True, type=bool)
@click.option("--fresh", "-f", is_flag=True, type=bool)
@click.option("--nouse_json", is_flag=True, type=bool)
@click.argument("package_name")
def check(verbose: bool, fresh: bool, nouse_json: bool, package_name: str) -> None:
    with Cache(fresh_index=fresh) as cache:
        package_name, operator, version = package_name.partition("==")
        package = parse_index(package_name, cache, use_json=not nouse_json)
        selected_versions = select_versions(package, operator, version)

        if verbose:
            click.echo(f"check {package_name} {selected_versions}")

        rc = 0
        for v in selected_versions:
            rc |= run_checker(package, v, verbose=verbose, cache=cache)

    if rc != 0:
        sys.exit(rc)


@cli.command(help="Check for presence of pep517 markers")
@click.option("--verbose", "-v", is_flag=True, type=bool)
@click.option("--fresh", "-f", is_flag=True, type=bool)
@click.option("--nouse_json", is_flag=True, type=bool)
@click.argument("package_name")
def ispep517(verbose: bool, fresh: bool, nouse_json: bool, package_name: str) -> None:
    with Cache(fresh_index=fresh) as cache:
        package_name, operator, version = package_name.partition("==")
        package = parse_index(package_name, cache, use_json=not nouse_json)
        selected_versions = select_versions(package, operator, version)

        if verbose:
            click.echo(f"check {package_name} {selected_versions}")

        rc = 0
        for v in selected_versions:
            rc |= is_pep517(package, v, verbose=verbose, cache=cache)

    if rc != 0:
        sys.exit(rc)


@cli.command(help="Check for native modules in bdist")
@click.option("--verbose", "-v", is_flag=True, type=bool)
@click.option("--fresh", "-f", is_flag=True, type=bool)
@click.option("--nouse_json", is_flag=True, type=bool)
@click.argument("package_name")
def native(verbose: bool, fresh: bool, nouse_json: bool, package_name: str) -> None:
    with Cache(fresh_index=fresh) as cache:
        package_name, operator, version = package_name.partition("==")
        package = parse_index(package_name, cache, use_json=not nouse_json)
        selected_versions = select_versions(package, operator, version)

        if verbose:
            click.echo(f"check {package_name} {selected_versions}")

        rc = 0
        for v in selected_versions:
            rc |= has_nativemodules(package, v, verbose=verbose, cache=cache)

    if rc != 0:
        sys.exit(rc)


@cli.command(help="Guess license of a package")
@click.option("--verbose", "-v", is_flag=True, type=bool)
@click.option("--fresh", "-f", is_flag=True, type=bool)
@click.option("--nouse_json", is_flag=True, type=bool)
@click.argument("package_name")
def license(verbose: bool, fresh: bool, nouse_json: bool, package_name: str) -> None:
    with Cache(fresh_index=fresh) as cache:
        package_name, operator, version = package_name.partition("==")
        package = parse_index(package_name, cache, use_json=not nouse_json)
        selected_versions = select_versions(package, operator, version)

        if verbose:
            click.echo(f"check {package_name} {selected_versions}")

        rc = 0
        for v in selected_versions:
            license = guess_license(package, v, verbose=verbose, cache=cache)
            if license is not None and not isinstance(license, str):
                license = license.shortname
            if license is None:
                rc |= 1
            print(f"{package_name}=={v}: {license or 'Unknown'}")

    if rc != 0:
        sys.exit(rc)


@cli.command(help="Download an sdist, print path on stdout")
@click.option("--verbose", "-v", is_flag=True, type=bool)
@click.option("--fresh", "-f", is_flag=True, type=bool)
@click.option("--nouse_json", is_flag=True, type=bool)
@click.option("--dest", help="Directory to store in", default="")
@click.option(
    "--index-url", help="Alternate index url (uses HONESTY_INDEX_URL or pypi by default"
)
@click.argument("package_name")
@wrap_async
async def download(
    verbose: bool,
    fresh: bool,
    nouse_json: bool,
    dest: str,
    index_url: Optional[str],
    package_name: str,
) -> None:
    dest_path: Optional[Path]
    if dest:
        dest_path = Path(dest)
        dest_path.mkdir(parents=True, exist_ok=True)
    else:
        dest_path = None

    async with Cache(fresh_index=fresh, index_url=index_url) as cache:
        package_name, operator, version = package_name.partition("==")
        package = await async_parse_index(package_name, cache, use_json=not nouse_json)
        selected_versions = select_versions(package, operator, version)

        if verbose:
            click.echo(f"check {package_name} {selected_versions}")

        rc = await async_download_many(
            package, versions=selected_versions, dest=dest_path, cache=cache
        )

    sys.exit(rc)


@cli.command(help="Download/extract an sdist, print path on stdout")
@click.option("--verbose", "-v", is_flag=True, type=bool)
@click.option("--fresh", "-f", is_flag=True, type=bool)
@click.option("--nouse_json", is_flag=True, type=bool)
@click.option("--dest", help="Directory to store in", default="")
@click.option(
    "--index-url", help="Alternate index url (uses HONESTY_INDEX_URL or pypi by default"
)
@click.argument("package_name")
@wrap_async
async def extract(
    verbose: bool,
    fresh: bool,
    nouse_json: bool,
    dest: str,
    index_url: Optional[str],
    package_name: str,
) -> None:

    async with Cache(fresh_index=fresh, index_url=index_url) as cache:
        package_name, operator, version = package_name.partition("==")
        package = await async_parse_index(package_name, cache, use_json=not nouse_json)
        selected_versions = select_versions(package, operator, version)
        if len(selected_versions) != 1:
            raise click.ClickException(f"Wrong number of versions: {selected_versions}")

        if verbose:
            click.echo(f"check {package_name} {selected_versions}")

        rel = package.releases[selected_versions[0]]
        sdists = [f for f in rel.files if f.file_type == FileType.SDIST]
        if not sdists:
            raise click.ClickException(f"{package.name} no sdists")

        lp = await cache.async_fetch(pkg=package_name, url=sdists[0].url)

        archive_root, _ = extract_and_get_names(
            lp, strip_top_level=True, patterns=("*.*",)
        )

        subdirs = tuple(Path(archive_root).iterdir())
        if dest:
            for subdir in subdirs:
                shutil.copytree(subdir, Path(dest, subdir.name))
        else:
            dest = archive_root

        # Try to be helpful in the common case that there's a top-level
        # directory by itself.  Specifying a non-empty dest makes the fallback
        # less useful.
        if len(subdirs) == 1:
            print(os.path.join(dest, subdirs[0].name))
        else:
            print(dest)


@cli.command(help="Print age in days for a given release")
@click.option("--verbose", "-v", is_flag=True, type=bool)
@click.option("--fresh", "-f", is_flag=True, type=bool)
@click.option("--base", help="yyyy-mm-dd of when to subtract from")
@click.argument("package_name")
@wrap_async
async def age(verbose: bool, fresh: bool, base: str, package_name: str,) -> None:

    if base:
        base_date = datetime.strptime(base, "%Y-%m-%d")
    else:
        base_date = datetime.utcnow()
    base_date = base_date.replace(tzinfo=timezone.utc)

    async with Cache(fresh_index=fresh) as cache:
        package_name, operator, version = package_name.partition("==")
        package = await async_parse_index(package_name, cache, use_json=True)
        selected_versions = select_versions(package, operator, version)
        for v in selected_versions:
            t = min(x.upload_time for x in package.releases[v].files)
            assert t is not None

            diff = base_date - t
            days = diff.days + (diff.seconds / 86400.0)
            print(f"{v}\t{t.strftime('%Y-%m-%d')}\t{days:.2f}")


@cli.command(help="Show dep tree")
@click.option("--include-extras", is_flag=True, help="Whether to incude *any* extras")
@click.option("--verbose", is_flag=True, help="Show verbose output")
@click.option("--python-version", default="3.7.5")
@click.argument("package_name")
def deps(
    include_extras: bool, verbose: bool, python_version: str, package_name: str
) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.WARNING)

    # TODO platform option

    seen: Set[Tuple[str, Optional[str], str]] = set()
    assert python_version.count(".") == 2
    deptree = DepWalker(package_name, python_version).walk(include_extras)
    # TODO record constraints on DepEdge, or put in lib to avoid this nonsense
    fake_root = DepNode("", version="", deps=[DepEdge(target=deptree)])
    print_deps(fake_root, seen)


def print_deps(
    deps: DepNode, seen: Set[Tuple[str, Optional[str], str]], depth: int = 0
) -> None:
    prefix = ". " * depth
    for x in deps.deps:
        # TODO display whether install or build dep, and whether pin disallows
        # current version, has compatible bdist, no sdist, etc
        key = (x.target.name, x.target.dep_extras, x.target.version)
        dep_extras = f"[{x.target.dep_extras}]" if x.target.dep_extras else ""
        if key in seen:
            print(
                f"{prefix}{x.target.name}{dep_extras} (=={x.target.version}) (already listed){x.markers or ''}"
            )
        else:
            seen.add(key)
            color = "red" if not x.target.has_sdist else "green"
            click.echo(
                prefix
                + click.style(x.target.name, fg=color,)
                + f"{dep_extras} (=={x.target.version}){' ' + x.markers if x.markers else ''} via "
                + click.style(x.constraints or "*", fg="yellow")
                + click.style(" no whl" if not x.target.has_bdist else "", fg="blue")
            )
            if x.target.deps:
                print_deps(x.target, seen, depth + 1)


def select_versions(package: Package, operator: str, selector: str) -> List[str]:
    """
    Given operator='==' and selector='*' or '2.0', return a list of the matching
    versions, in increasing order.
    """
    if not package.releases:
        raise click.ClickException(f"No releases at all for {package.name}")

    if operator not in ("", "=="):
        raise click.ClickException("Only '==' is supported")

    if selector == "":
        # latest
        version = sorted(package.releases, key=pkg_resources.parse_version)[-1]
        return [version]
    elif selector == "*":
        versions: List[str] = sorted(package.releases, key=pkg_resources.parse_version)
        return versions
    else:
        if selector not in package.releases:
            raise click.ClickException(
                f"The version {selector} does not exist for {package.name}"
            )
        return [selector]


if __name__ == "__main__":
    cli()
