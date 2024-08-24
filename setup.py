from setuptools import setup, find_packages

setup(
    name='tickr',
    version='1.0.0',
    description='A high-level API client for accessing cryptocurrency candle data from a GitHub repository.',
    author='syncsoftco',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'absl-py>=2.1.0,<3.0',
        'backoff>=2.2.1,<3.0',
        'ccxt>=4.3.87,<5.0',
        'fsspec>=2024.6.1',
        'pandas>=2.2.2,<3.0',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
