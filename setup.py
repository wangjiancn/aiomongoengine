import io
import re

from setuptools import find_packages
from setuptools import setup


with io.open("src/aiomongoengine/__init__.py", "rt", encoding="utf8") as f:
    version = re.search(r'__version__ = "(.*?)"', f.read()).group(1)

setup(
    name="aiomongoengine",
    version=version,
    url="",
    project_urls={
        "Documentation": "",
        "Code": "",
        "Issue tracker": "",
    },
    license="MIT",
    author="wangjiancn",
    author_email="wangjianchn@outlook.com",
    maintainer="smartpython",
    maintainer_email="smartpython@outlook.com",
    description="mongoengine wrapper for asyncio",
    classifiers=[
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    python_requires=">=3.5, !=3.5.0, !=3.5.1",
    install_requires=[
        "motor>=2.0.0",
        "arrow>=0.15.1",
        "tornado==0.5.1",
        "pymongo==3.6",
        "six",
        "easydict"
    ],
    keywords=['mongoengine', 'asyncio', 'mongo']
)
