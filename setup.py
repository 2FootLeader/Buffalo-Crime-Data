from setuptools import find_packages, setup

setup(
    name="CrimeData",
    packages=find_packages(exclude=["CrimeData_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
