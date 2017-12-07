#!/bin/bash
set -ex
VENV_BUILD_DIR=/aws_test/

function BASE_INSTALL(){
    yum update -y
    yum install -y \
	sudo \
	gcc \
	gcc-c++ \
	lapack-devel \
	python36-devel \
	python36-virtualenv \
	make \
	findutils \
	zip \
	bzip2 \
	which
    	#atlas-devel \
	#atlas-sse3-devel \
	#blas-devel \
}

function GENERATE_VIRTUALENV() {
    /usr/bin/virtualenv-3.6 \
	--python=python3.6 $VENV_BUILD_DIR \
	--always-copy \
	--no-site-packages
    source $VENV_BUILD_DIR/bin/activate
}

function INSTALL_REQUIREMENTS() {
    pushd /outputs/    
    # Generate a requirements file
    pip install pipreqs &> /dev/null
    pip install --upgrade pip wheel &> /dev/null
    pipreqs --force /outputs/
    # Use the requirements to build an environment
    while read PKGNAME; do
	_PKGNAME=$(echo $PKGNAME | awk -F== '{print $1}')
	echo "Installing "$_PKGNAME
	yes | pip install $PKGNAME
    done < requirements.txt
    rm requirements.txt
    popd
}

function COPY_SHARED_LIBS () {
    LIBDIR="$VIRTUAL_ENV/lib64/python3.6/site-packages/lib/"
    mkdir -p $LIBDIR || true
    #cp /usr/lib64/atlas/* $LIBDIR
    cp /usr/lib64/libquadmath.so.0 $LIBDIR
    #cp /usr/lib64/libgfortran.so.3 $LIBDIR
}

function STRIP_VIRTUALENV () {
    if [[ -n $(find $VIRTUAL_ENV/lib64/python3.6/site-packages/ -name "*.so") ]];
    then
	echo "venv original size $(du -sh $VIRTUAL_ENV | cut -f1)"
	find $VIRTUAL_ENV/lib64/python3.6/site-packages/ -name "*.so" | xargs strip
	echo "venv stripped size $(du -sh $VIRTUAL_ENV | cut -f1)"
    fi
	
    pushd $VIRTUAL_ENV/lib64/python3.6/site-packages/ && zip -r -9 -q /outputs/venv.zip * ; popd
    pushd $VIRTUAL_ENV/lib/python3.6/site-packages/ && zip -rg -9 -q /outputs/venv.zip * ; popd    
    echo "site-packages compressed size $(du -sh /outputs/venv.zip | cut -f1)"

    pushd $VIRTUAL_ENV && zip -r -q /outputs/full-venv.zip * ; popd
    echo "venv compressed size $(du -sh /outputs/full-venv.zip | cut -f1)"    
}

function EXECUTE_IF_EXISTS () {
    if [ -f /outputs/$1 ]; then
	source /outputs/$1
    else
	echo "$1 not found"
	ls /outputs/
    fi
}

BASE_INSTALL
EXECUTE_IF_EXISTS before.sh
GENERATE_VIRTUALENV
INSTALL_REQUIREMENTS
COPY_SHARED_LIBS
EXECUTE_IF_EXISTS after.sh
STRIP_VIRTUALENV
echo "Done build!"
