DESCRIPTION = 'Anomaly detection in timeseries data'
LONG_DESCRIPTION = 'http://earthgecko-skyline.readthedocs.io'
# NAME = 'earthgecko_skyline'
NAME = 'skyline'
AUTHOR = 'Abe Stanway'
AUTHOR_EMAIL = 'abe@etsy.com'
MAINTAINER = 'Gary Wilson'
MAINTAINER_EMAIL = 'garypwilson@gmail.com'
URL = 'https://github.com/earthgecko/skyline'
DOWNLOAD_URL = 'https://github.com/earthgecko/skyline/tarball/master'
LICENSE = 'MIT License'

from setuptools import setup, find_packages
import sys
import os
# skyline_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "skyline")
# sys.path.insert(0, skyline_path)
# import skyline
# import skyline_version
# VERSION = skyline_version.__version__
VERSION = '2.0.0'

# @added 20161127 - Branch #922: ionosphere
# Added setup_package


def setup_package():
    # needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    # sphinx = ['sphinx', 'sphinx_rtd_theme'] if needs_sphinx else []
    setup(
        name=NAME,
        version=VERSION,
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        author=AUTHOR,
        author_email=AUTHOR_EMAIL,
        maintainer=MAINTAINER,
        maintainer_email=MAINTAINER_EMAIL,
        url=URL,
        download_url=DOWNLOAD_URL,
        license=LICENSE,

        classifiers=[
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'Environment :: Web Environment',
            'Intended Audience :: System Administrators',
            'Intended Audience :: Developers',
            'Topic :: System :: Monitoring',
            'Topic :: Scientific/Engineering :: Information Analysis',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 3.8',
            'Natural Language :: English',
            'Operating System :: POSIX :: Linux',
        ],
        keywords='timeseries anomaly detection numpy pandas statsmodels',
        packages=['skyline'],
        # dependency_links=['http://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-1.2.3.zip#md5=6d42998cfec6e85b902d4ffa5a35ce86'],
        # install_requires=[
        #     'setuptools', 'pip', 'wheel', 'redis==2.10.5', 'hiredis==0.2.0',
        #     'python-daemon==2.1.1', 'Flask==0.11.1', 'simplejson==3.8.2',
        #     'six==1.10.0', 'unittest2==1.1.0', 'mock==2.0.0', 'numpy==1.11.1',
        #     'scipy==0.17.1', 'matplotlib==1.5.1', 'pandas==0.18.1', 'patsy==0.4.1',
        #     'statsmodels==0.6.1', 'msgpack-python==0.4.7', 'requests==2.10.0',
        #     'gunicorn==19.6.0'
        # ],
    )


if __name__ == '__main__':
    print('Starting')
    setup_package()

# if __name__ == "__main__":
#     print('Starting')
#     setup_package()
