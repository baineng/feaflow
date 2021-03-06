# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as f:
    readme = f.read()

INSTALL_REQUIRES = [
    "Click==7.*",
    "pydantic>=1.0.0",
    "PyYAML>=5.4.*",
    "Jinja2>=2.0.0",
    "pandas>=1.0.0",
    "pendulum>=2.0.0",
    "sqlparse>=0.3.0",
]

AIRFLOW_REQUIRES = ["apache-airflow>=2.0.0", "apache-airflow-providers-docker>=2.0.0"]

SPARK_REQUIRES = ["pyspark"]

FEAST_REQUIRES = ["feast>=0.12.0"]

DEV_REQUIRES = (
    [
        "flake8",
        "black",
        "isort",
        "mypy",
        "pytest==6.2.5",
        "pytest-xdist",
        "assertpy",
        "pandas>=1.0.0",
    ]
    + AIRFLOW_REQUIRES
    + SPARK_REQUIRES
    + FEAST_REQUIRES
)

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
    license="Apache License, Version 2.0",
    packages=find_packages(include=("feaflow*",)),
    install_requires=INSTALL_REQUIRES,
    extras_require={
        "dev": DEV_REQUIRES,
        "airflow": AIRFLOW_REQUIRES,
        "spark": SPARK_REQUIRES,
        "feast": FEAST_REQUIRES,
    },
    keywords=("feature featurestore feature_ingestion"),
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    entry_points={"console_scripts": ["feaflow = feaflow.cli:cli"]},
)
