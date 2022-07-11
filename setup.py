import pathlib

from setuptools import find_packages, setup

# Get absolute path to the decription file to avoid reading in
# something unexpected.
here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8")

# Get version number from a single source of truth
def get_version(version_path):
    with open(version_path) as infile:
        for line in infile:
            if line.startswith('__version__'):
                delim = '"' if '"' in line else "'"
                return line.split(delim)[1]
        else:
            raise RuntimeError("Unable to find version string.")

# Get non-python files under a directory recursively.
def get_data_files(directory):
    files = [
        str(p.relative_to(directory))
        for p in directory.rglob("*")
        if (not str(p).endswith(".py")) or (not str(p).endswith(".pyc"))
    ]
    return files


setup(
    name='pyTigerGraph',
    packages=find_packages(where="."),
    package_data={"pyTigerGraph.gds": get_data_files(here / "pyTigerGraph" / "gds")},
    version=get_version(here/"pyTigerGraph"/"__init__.py"),
    license='Apache 2',
    description='Library to connect to TigerGraph databases',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='TigerGraph Inc.',
    author_email='support@tigergraph.com',
    url='https://docs.tigergraph.com/home/',
    download_url='',
    keywords=['TigerGraph', 'Graph Database', 'Data Science', 'Machine Learning'],
    install_requires=[
        'pyTigerDriver',
        'validators',
        'requests'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',  # 3 - Alpha, 4 - Beta or 5 - Production/Stable
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    extras_require={
        "gds": ["pandas", "kafka-python", "numpy"],
    },
    project_urls={
        "Bug Reports": "https://github.com/tigergraph/pyTigerGraph/issues",
        "Source": "https://github.com/tigergraph/pyTigerGraph",
    },
)
