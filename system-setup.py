#!/usr/bin/env python2

import sys
import os
import argparse

ROOT = HERE = os.path.abspath(os.path.dirname(__file__))
READIES = os.path.join(ROOT, "deps/readies")
sys.path.insert(0, READIES)
import paella

#----------------------------------------------------------------------------------------------

class RedisRSCoordinatorSetup(paella.Setup):
    def __init__(self, nop=False):
        paella.Setup.__init__(self, nop)

    def common_first(self):
        self.install_downloaders()
        self.pip_install("wheel")
        self.pip_install("--ignore-installed --no-cache-dir setuptools --upgrade")

        self.install("git gawk lcov jq")

    def debian_compat(self):
        self.install("libatomic1")
        if self.osnick == 'trusty':
            self.ubuntu_trusty()
        else:
            self.run("%s/bin/getgcc" % READIES)
            self.install("libtool m4 automake")
        
    def ubuntu_trusty(self):
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("libtool m4 automake") # after modern gcc
        self.install("realpath")
        self.install_linux_gnu_tar()

    def redhat_compat(self):
        self.install("redhat-lsb-core")
        self.install("libatomic")
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("libtool m4 automake")
        
    def fedora(self):
        self.install("libatomic")
        self.run("%s/bin/getgcc" % READIES)

    def macos(self):
        pass

    def common_last(self):
        self.run("%s/bin/getcmake" % READIES)
        self.run("{PYTHON} {READIES}/bin/getrmpytools".format(PYTHON=self.python, READIES=READIES))
        self.pip_install("awscli pudb")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RedisRSCoordinatorSetup(nop = args.nop).setup()
