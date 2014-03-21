from setuptools import setup

setup(name='MongoDBStorage', version='1.0',
      description='OpenShift Python-2.7 Community Cartridge based application',
      author='Your Name', author_email='ramr@example.org',
      url='http://www.python.org/sigs/distutils-sig/',

      install_requires=['pymongo>=2.4.1',
                        'python-dateutil>=2.1']
     )
