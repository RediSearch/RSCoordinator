#!/usr/bin/env python2

import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "deps/readies"))
import paella

#----------------------------------------------------------------------------------------------

class RedisRSCoordinatorSetup(paella.Setup):
    def __init__(self, nop=False):
        paella.Setup.__init__(self, nop)

    def common_first(self):
        self.setup_pip()
        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")

        self.install("git wget lcov")

    def debian_compat(self):
        self.install("libatomic1")
        self.install("build-essential")
        self.install("libtool m4 automake")
        if self.osnick == 'trusty':
            self.install("cmake3")
        else:
            self.install("cmake")
        self.install("python-psutil")

    def redhat_compat(self):
        self.install("redhat-lsb-core")
        self.install("libatomic")
        self.group_install("'Development Tools'")
        self.install("libtool m4 automake")
        self.install("cmake3")
        self.run("ln -s `command -v cmake3` /usr/local/bin/cmake")

        # self.run("yum remove -y python-setuptools || true")
        # self.pip_install("-IU --force-reinstall setuptools")
        
    def fedora(self):
        self.install("libatomic")
        self.group_install("'Development Tools'")
        self.install("cmake")

    def macosx(self):
        if sh('xcode-select -p') == '':
            fatal("Xcode tools are not installed. Please run xcode-select --install.")
        self.install("cmake")

    def common_last(self):
        # redis-py-cluster should be installed from git due to redis-py dependency
        self.run("python2 -m pip uninstall -y -q redis redis-py-cluster ramp-packer RLTest rmtest semantic-version")
        self.pip_install("--no-cache-dir git+https://github.com/Grokzen/redis-py-cluster.git@master")
        self.pip_install("--no-cache-dir git+https://github.com/RedisLabsModules/RLTest.git@master")
        self.pip_install("--no-cache-dir git+https://github.com/RedisLabs/RAMP@master")
        
        self.pip_install("awscli pudb")
        self.pip_install("jinja2 semantic_version six")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RedisRSCoordinatorSetup(nop = args.nop).setup()
