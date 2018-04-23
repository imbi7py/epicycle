from setuptools import setup, find_packages

setup(
    name="epicycle",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "zope.interface"
    ],
    extras_require = {
        "dev": [
            "Hypothesis",
            "pytest",
        ],
    }
)
