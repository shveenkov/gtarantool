# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# Read package version without importing it
for line in open("gtarantool.py"):
    if line.startswith("__version__"):
        exec line
        break

setup(
    name="gtarantool",
    py_modules=["gtarantool"],
    version=__version__,
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
