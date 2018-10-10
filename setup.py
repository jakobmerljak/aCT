from setuptools import setup, find_packages

setup(name='aCT',
      version='0.1',
      description='ATLAS Control Tower',
      url='http://github.com/ATLASControlTower/aCT',
      python_requires='>=2.7',
      author='aCT team',
      author_email='act-dev@cern.ch',
      license='MIT',
      packages=find_packages('src/act'),
      install_requires=[
          'mysql-connector == 2.1.*',  # connection to MySQL database
          'htcondor',                 # bindings to use HTCondor to submit jobs
          'pylint',                   # for travis automatic tests
          'requests'                  # for APF mon calls
          ]
)
 
