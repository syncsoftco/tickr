from setuptools import setup, find_packages

setup(
    name='tickr',
    version='1.0.0',
    description='A high-level API client for accessing cryptocurrency candle data from a GitHub repository.',
    author='syncsoftco',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'ccxt>=4.3.87,<5',
        'PyGithub>=1.55,<2',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
