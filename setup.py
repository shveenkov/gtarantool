# -*- coding: utf-8 -*-

import re

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def find_version():
    for line in open("gtarantool.py"):
        if line.startswith("__version__"):
            return re.match(r"""__version__\s*=\s*(['"])([^'"]+)\1""", line).group(2)

setup(
    name="gtarantool",
    py_modules=["gtarantool"],
    version=find_version(),
    author="Dmitry Shveenkov",
    author_email="shveenkov@mail.ru",
    url="https://github.com/shveenkov/gtarantool",
    classifiers=[
        "Programming Language :: Python",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database :: Front-Ends"
    ],
    install_requires=[
        "tarantool>=0.5.1",
    ],
    description="Tarantool connection driver for work with gevent framework",
    long_description=open("README.rst").read()
)
