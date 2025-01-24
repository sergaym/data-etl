from setuptools import setup, find_packages

setup(
    name="data-engineer-case-study",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas==2.2.1",
        "numpy==1.26.4",
        "sqlalchemy==2.0.27",
    ],
    python_requires=">=3.9",
) 