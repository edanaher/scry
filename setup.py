import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="scry",
    version="0.0.1",
    author="Evan Danaher",
    author_email="python@edanaher.net",
    description="A convenient tool for querying SQL databases",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/edanaher/scry",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
	"Operating System :: OS Independent",
    ],
    python_requires=">3.6",
    entry_points = {
        'console_scripts': ['scry = scry.scry:main'],
    },
    install_requires = [
        "lark-parser",
        "prompt-toolkit",
        "psycopg2",
    ]

)
