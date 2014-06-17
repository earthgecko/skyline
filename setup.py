from setuptools import setup, find_packages

# work around a nose test bug: http://bugs.python.org/issue15881#msg170215
try:
    import multiprocessing
except ImportError:
    pass

setup(name='skyline',
      version='0.1.0',
      description='''
                  It'll detect your anomalies!
                  ''',
      author='etsy',
      author_email='',
      license='MIT',
      url='https://github.com/etsy/skyline',
      keywords=['anomaly detection', 'timeseries', 'monitoring'],
      classifiers=['Programming Language :: Python'],

      setup_requires=['nose>=1.0', 'unittest2', 'mock'],
      install_requires=['redis==2.7.2',
                        'hiredis==0.1.1',
                        'python-daemon==1.6',
                        'flask==0.9',
                        'simplejson==2.0.9',
                        'numpy',
                        'scipy',
                        'pandas',
                        'patsy',
                        'statsmodels',
                        'msgpack_python'],

      packages=find_packages('src'),
      package_dir={'': 'src'},
      include_package_data=True,

      entry_points={
          'console_scripts': [
              'analyzer-agent = skyline.analyzer.agent:run',
              'horizon-agent = skyline.horizon.agent:run',
              'skyline-webapp = skyline.webapp.webapp:run'
              ]
      },

      scripts=['bin/analyzer.d', 'bin/horizon.d', 'bin/webapp.d'],

      test_suite='nose.collector',
      )
