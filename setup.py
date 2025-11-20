"""Setup script for EduThreat-CTI."""

from setuptools import setup, find_packages

# Read README for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="eduthreat-cti",
    version="1.0.0",
    author="EduThreat-CTI Contributors",
    description="Real-time cyber threat intelligence pipeline for the global education sector",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-username/EduThreat-CTI",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Security",
        "Topic :: Education",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "eduthreat-pipeline=src.edu_cti.cli.pipeline:main",
            "eduthreat-ingest=src.edu_cti.cli.ingestion:main",
            "eduthreat-build=src.edu_cti.cli.build_dataset:main",
        ],
    },
)

