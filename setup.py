import setuptools
# https://packaging.python.org/tutorials/packaging-projects/

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pysea-common-constants", # Replace with your own username
    version="0.0.16",
    author="Eugene Komissarov",
    author_email="ekom@cian.ru",
    description="Most common constants and functions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="git@bitbucket.org:cianmedia/common_constants.git",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: Linux",
    ],
    python_requires='>=3.7',
)