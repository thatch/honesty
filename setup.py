from setuptools import setup

setup(
    use_scm_version=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Utilities",
    ],
    entry_points={"console_scripts": ["honesty = honesty.cmdline:cli"]},
    install_requires=[
        "aiohttp >= 3.6",
        "click >= 7.0",
        "dataclasses >= 0.7; python_version < '3.7'",
        "infer-license >= 0.0.6",
        "packaging >= 19.0",
        "pkginfo >= 1.5.0",
    ],
)
