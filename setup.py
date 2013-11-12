'''
Created on 30 окт. 2013 г.

@author: garet
'''

from setuptools import setup, find_packages
from os.path import join, dirname

setup(
    name='SqlMaker',
    version='0.1.7a',
    packages=find_packages(),
    long_description=open(join(dirname(__file__), 'README.txt')).read(),
)
