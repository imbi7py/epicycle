"""
An implementation of hierarchical timing wheels.
"""

from setuptools import setup, find_packages

setup(
    name="epicycle",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "zope.interface"
    ],
    license='MIT',
    extras_require={
        "dev": [
            "Hypothesis",
            "pytest",
        ],
    }
)
