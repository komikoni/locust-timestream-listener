import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="locust_timestream_listener", # Replace with your own username
    version="0.0.1",
    author="komikoni",
    author_email="komikoni+github@gmail.com",
    description="Locust.io 1.X timestream listener. This is a fork of locust_influxdb_listener and modified",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/komikoni/locust-timestream-listener",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'locust>=1.1.1',
        'boto3>=1.15.10',
    ],
    python_requires='>=3.6',
)