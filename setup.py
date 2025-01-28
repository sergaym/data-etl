"""
Setup configuration for the data engineering case study package.
"""
from setuptools import setup, find_packages

setup(
    name="meter_readings_etl",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.11",
    install_requires=[
        "pandas>=2.2.1",
        "numpy>=1.26.4",
        "sqlalchemy>=2.0.27",
        "python-dotenv>=1.0.1",
        "psycopg2-binary>=2.9.9",
        "streamlit>=1.41.1",
        "plotly>=5.19.0",
    ],
    author="Sergio Ayala",
    description="Data engineering case study for processing meter readings",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.11",
    ],
) 