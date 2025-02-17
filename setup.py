from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="statscan-wrapper",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A Python wrapper for Statistics Canada data tables",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/statscan-wrapper",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "polars>=0.18.0",
        "requests>=2.25.0",
    ],
) 