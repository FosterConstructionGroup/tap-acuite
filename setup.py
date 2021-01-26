#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-acuite",
    version="1.0.0",
    description="Singer.io tap for extracting data from the Acuite API",
    author="Sam Woolerton",
    url="https://samwoolerton.com",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_acuite"],
    install_requires=["singer-python==5.9.0", "aiohttp==3.7.3", "tenacity==6.3.1"],
    extras_require={"dev": ["pylint", "ipdb", "nose",]},
    entry_points="""
          [console_scripts]
          tap-acuite=tap_acuite:main
      """,
    packages=["tap_acuite"],
    package_data={"tap_acuite": ["tap_acuite/schemas/*.json"]},
    include_package_data=True,
)
