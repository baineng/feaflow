# -*- coding: utf-8 -*-

from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    readme = f.read()

INSTALL_REQUIRES = ["pydantic>=1.0.0"]

CLI_REQUIRES = []

AIRFLOW_REQUIRES = []

DEV_REQUIRES = [
    "flake8",
    "black==19.10b0",
    "isort>=5",
    "mypy==0.790",
    "pytest==6.0.0",
    "pytest-xdist",
    "assertpy==1.1",
]

setup(
    name="feaflow",
    version="0.1.0",
    author="Benn Ma",
    author_email="bennmsg@gmail.com",
    description="Flow Features Ingestion By DSL",
    long_description=readme,
    long_description_content_type="text/markdown",
    python_requires=">=3.7.0",
    url="https://github.com/thenetcircle/feaflow",
    project_urls={
        "Bug Tracker": "https://github.com/thenetcircle/feaflow/issues",
    },
    license='Apache License, Version 2.0',
    packages=["feaflow"],
    install_requires=INSTALL_REQUIRES,
    extras_require={
        "dev": DEV_REQUIRES,
        "cli": CLI_REQUIRES,
        "airflow": AIRFLOW_REQUIRES,
    },
    keywords=("feature featurestore feature_ingestion"),
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
)
