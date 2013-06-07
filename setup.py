#!/usr/bin/python

from distutils.core import setup

setup(name='IDEAS',
      version='0.2',
      description='IDEAS Framework Package',
      author='Ron Lai',
      author_email='ron@goideas.org',
      url='http://goideas.org/',
      packages=[
        'IDEAS', 
        'IDEAS.api', 
        'IDEAS.base',
        'IDEAS.config', 
        'IDEAS.db', 

         #these are libraries that are already created
        'IDEAS.lib.xlwt',
        'IDEAS.lib', 'IDEAS.lib.MySQLdb', 'IDEAS.lib.MySQLdb.constants',
        'IDEAS.lib.sqlparse', 'IDEAS.lib.sqlparse.engine'
      ],
     )
