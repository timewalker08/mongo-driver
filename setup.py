import os
import sys
from setuptools import find_packages, setup

DESCRIPTION = (
    'Mongo Driver for IU'
)

try:
    with open('README.md') as fin:
        LONG_DESCRIPTION = fin.read()
except Exception:
    LONG_DESCRIPTION = None


def get_version(version_tuple):
    """Return the version tuple as a string, e.g. for (0, 10, 7),
    return '0.10.7'.
    """
    return '.'.join(map(str, version_tuple))


# Dirty hack to get version number from monogengine/__init__.py - we can't
# import it as it depends on PyMongo and PyMongo isn't installed until this
# file is read
init = os.path.join(os.path.dirname(__file__), 'iu_mongo', '__init__.py')
version_line = list(filter(lambda l: l.startswith('VERSION'), open(init)))[0]

VERSION = get_version(eval(version_line.split('=')[-1]))

extra_opts = {
    'packages': find_packages(exclude=['tests', 'tests.*']),
}

setup(
    name='iu_mongo',
    version=VERSION,
    author='Jiaye Zhu',
    author_email='zhujiaye@h1n1.onaliyun.com',
    maintainer="Jiaye Zhu",
    maintainer_email="zhujiaye@h1n1.onaliyun.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    platforms=['any'],
    license='MIT',
    install_requires=["pymongo>=3.7", "six", "retry"],
    **extra_opts
)
