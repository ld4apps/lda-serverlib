from setuptools import setup

setup(name='lda-serverlib',
      version='1.0',
      description='LDA Server Library',
      author='Your Name',
      author_email='example@example.com',
      url='http://www.python.org/sigs/distutils-sig/',
      install_requires=[#'lda-clientlib',
                        'webob==1.4',
                        'pycrypto==2.6.1',
                        'pymongo==2.7',
                        'isodate==0.5.0',
                        'rdflib==4.2.0',
                        'rdflib-jsonld==0.2',
                        'werkzeug==0.9.4']
     )
