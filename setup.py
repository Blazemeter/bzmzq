from setuptools import setup

setup(name='bzmzq',
      version='0.1',
      description='Blazemeter Zookeeper Queue',
      url='',
      author='Yuri Grigorian',
      author_email='yuri.grigorian@ca.com',
      license='MIT',
      packages=['bzmzq'],
      install_requires=[
          'kazoo==2.4.0',
      ])
