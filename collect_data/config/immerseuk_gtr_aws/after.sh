#!bin/bash

phantomjs --version
cp /usr/lib/node_modules/phantomjs-prebuilt/lib/phantom/bin/phantomjs $VIRTUAL_ENV/lib64/python3.6/site-packages/
cp /usr/lib64/libfreetype* $VIRTUAL_ENV/lib64/python3.6/site-packages/
cp /usr/lib64/libfontconfig* $VIRTUAL_ENV/lib64/python3.6/site-packages/
pip-3.6 install lxml
