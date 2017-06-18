from setuptools import setup as depSetup
# from distutils.core import setup as exeSetup
# import py2exe

depSetup(name='yoedge-spider',
      version='0.0.1',
      packages=['spider', 'util', 'db', 'web'],
      install_requires=[
            'requests',
            'lxml',
            'Flask'
      ],
      zip_safe=False
      )


# exeSetup(
#         console = ['spider/main.py']
#         )