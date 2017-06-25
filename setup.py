from setuptools import setup

setup(name='bzmzq',
      version='0.1',
      description='Blazemeter Zookeeper Queue',
      url='',
      author='Yuri Grigorian',
      author_email='yuri.grigorian@ca.com',
      license='MIT',
      packages=['bzmzq'],
      include_package_data=True,
      install_requires=[
          'kazoo==2.4.0',
      ],
      entry_points={
          "console_scripts": ['bzmzq-worker = bzmzq.worker:main',
                              'bzmzq-scheduler = bzmzq.scheduler:main']
      }, )
