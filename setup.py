#!/usr/bin/env python

import ast
import os

from setuptools import setup, find_packages


def this_dir():
    return os.path.dirname(os.path.abspath(__file__))


def read_version(path):
    with open(path) as fh:
        for line in fh:
            stripped = line.strip()
            if stripped == '' or stripped.startswith('#'):
                continue
            elif line.startswith('from __future__ import'):
                continue
            else:
                if not line.startswith('__version__ = '):
                    raise Exception("Can't find __version__ line in " + path)
                break
        else:
            raise Exception("Can't find __version__ line in " + path)
        _, _, quoted = line.rstrip().partition('= ')
        return ast.literal_eval(quoted)


classifiers = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: POSIX',
    'Programming Language :: Python :: 3',
]


install_requires = [
    "python-consul>=0.7.0",
]


version = read_version(os.path.join(this_dir(), 'servicecatalog/_version.py'))


setup(
    name='servicecatalog',
    url='https://github.com/madedotcom/servicecatalog',

    author='Bob Gregory',
    author_email='bob@made.com',
    classifiers=classifiers,
    description='Provides a simple dict-based interface to Consul\'s service discovery.',
    install_requires=install_requires,
    license='MIT',
    package_dir={'': '.'},
    packages=find_packages('.'),
    platforms=['any'],
    version=version,
    zip_safe=True,
)
