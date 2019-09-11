import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dremel",
    version="0.0.1",
    author="codefever",
    author_email="who@example.com",
    description="Simple Python Implementation of Dremel Algorithms",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/codefever/dremel.py",
    packages=setuptools.find_packages(exclude=("tests",)),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'protobuf>=3.9',
    ],
)
