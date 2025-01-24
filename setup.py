"""
Setup configuration for the data engineering case study package.
"""

from setuptools import setup, find_packages

setup(
    name="data_pipeline",
    version="0.1.0",
    packages=find_packages(include=['src', 'src.*']),
    package_dir={'': '.'},
    python_requires='>=3.8',
    author="Sergio Ayala",
    description="Data engineering case study package for processing meter readings",
)
