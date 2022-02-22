from setuptools import setup, find_packages

with open('README.md') as readme_file:
    README = readme_file.read()

setup_args = dict(
    name='datamanager',
    version='0.0.0',
    description='A library to manage and return financial asset data using Alpaca',
    long_description_content_type="text/markdown",
    long_description=README + '\n',
    license='Proprietary',
    packages=find_packages(),
    author='Green Planet Capital',
    author_email='greenplanetcap.unofficial@gmail.com',
    keywords=['GPC', 'GreenPlanetCapital'],
    url='https://github.com/GreenPlanet-Capital/DataManager',
    download_url='https://github.com/GreenPlanet-Capital/DataManager',
    include_package_data=True,
    entry_points={
        'console_scripts': ['datamgr=DataManager.shell:main']
    },
)

install_requires = [
    'setuptools',
    'wheel',
    'dataset',
    'pytest',
    'iexfinance',
    'alpaca-trade-api',
    'pandas_market_calendars',
    'lxml',
    'typer',
]

if __name__ == '__main__':
    setup(**setup_args, install_requires=install_requires)