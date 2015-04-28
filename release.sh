#!/bin/bash
#apt-get install -y python-dev python-virtualenv
#virtualenv /tmp/lda-serverlib
source /tmp/lda-serverlib/bin/activate
cp -R /vagrant/lda-serverlib /tmp/lda-serverlib
cd /tmp/lda-serverlib
#python setup.py register
python setup.py install
python setup.py sdist upload
