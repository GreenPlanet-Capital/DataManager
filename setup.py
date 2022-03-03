from setuptools import setup, find_packages

setup_args = dict(
    long_description_content_type="text/markdown",
    packages=find_packages(where='src'),
)

if __name__ == '__main__':
    setup(**setup_args)
