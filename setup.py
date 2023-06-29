from setuptools import setup, find_packages

setup(
    name='AirflowTutorial',
    version='0.1',
    packages=find_packages(),
)

# Enable or disable downloader middlewares
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 1,
}
