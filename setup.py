import io
import re

from setuptools import find_packages
from setuptools import setup

with io.open("src/aiomongoengine/__init__.py", "rt", encoding="utf8") as f:
    version = re.search(r'__version__ = "(.*?)"', f.read()).group(1)

setup(
    name="aiomongoengine",
    version=version,
    url="https://github.com/wangjiancn/aiomongoengine",
    project_urls={
        "Code": "https://github.com/wangjiancn/aiomongoengine",
        "Issue tracker": "https://github.com/wangjiancn/aiomongoengine/issues"
    },
    license="MIT",
    author="wangjiancn",
    author_email="wangjianchn@outlook.com",
    maintainer="smartpython",
    maintainer_email="smartpython@outlook.com",
    description="mongoengine wrapper for asyncio",
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    python_requires=">=3.6",
    install_requires=[
        "motor>=2.0.0",
        "arrow>=0.15.1",
        "tornado==5.1.1",
        "pymongo>=3.10,<4",
        "six",
        "typing-extensions",
        "easydict"
    ],
    extras_require={
        'dev': ['pytest', 'coverage', 'pytest-asyncio', 'autopep8']
    },
    keywords=['mongoengine', 'asyncio', 'mongo']
)
