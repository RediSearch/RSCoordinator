#!/usr/bin/env python2

from __future__ import print_function

import argparse
from subprocess import Popen
import os
import sys
import platform
import multiprocessing
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "deps/readies"))
import paella

osnick = paella.Platform().osnick
DEFAULT_VECSIM_ARCH='skylake-avx512' if 'osnick' in osnick else 'x86-64-v4'

ap = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
ap.add_argument('--build-dir', help="Build directory to use")
ap.add_argument('--with-libuv', help="UV install directory. Will assume build dir")
ap.add_argument('--enable-debug', help="Build unoptimized with debug symbols", action="store_true")
ap.add_argument('--concurrency', '-j', help='Concurrent make build',
                default=multiprocessing.cpu_count() * 2)
ap.add_argument('--with-cmake', help='Path to cmake', default='cmake')
ap.add_argument('--with-sanitizer', help='Sanitizer to use', choices=('asan', 'tsan', 'msan'))
ap.add_argument('--vecsim-arch', type=str, default=DEFAULT_VECSIM_ARCH, help='VECSIM architecture')

options = ap.parse_args()
# Determine the build directory
BUILD_DIR = options.build_dir
SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))

if not BUILD_DIR:
    cwd = os.path.abspath(os.getcwd())
    self_dir = os.path.dirname(os.path.abspath(__file__))
    if cwd == self_dir:
        print("Running ./configure in source root. Build will be in `build`")
        BUILD_DIR = os.path.join(os.getcwd(), 'build')
    else:
        BUILD_DIR = cwd
else:
    BUILD_DIR = os.path.abspath(BUILD_DIR)

UV_DIR = options.with_libuv if options.with_libuv else os.path.join(
    BUILD_DIR, 'libuv-' + platform.platform().replace(' ', '_'))

UV_A = os.path.join(UV_DIR, 'lib', 'libuv.a')
UV_SRC = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'src', 'dep', 'libuv')
CMAKE = options.with_cmake

print("BUILD_DIR: {}".format(BUILD_DIR))
print("UV_DIR: {}".format(UV_DIR))
# sys.exit(0)

def build_uv(uv_src, uv_dst, build_dir):
    # See if the UV source exists in the build dir
    copy_dst = os.path.join(build_dir, 'libuv-src')
    if not os.path.exists(copy_dst):
        shutil.copytree(uv_src, copy_dst)
    if options.with_sanitizer == 'msan':
        os.environ['CFLAGS'] = '-fsanitize=memory'
        os.environ['LDFLAGS'] = '-fsanitize=memory'
    elif options.with_sanitizer == 'asan':
        os.environ['CFLAGS'] = os.environ['LDFLAGS'] = '-fsanitize=address'
    elif options.with_sanitizer == 'thread':
        os.environ['CFLAGS'] = os.environ['LDFLAGS'] = '-fsanitize=thread'
    if options.with_sanitizer:
        os.environ['CC'] = 'clang'
        os.environ['CXX'] = 'clang++'

    uv_src = copy_dst
    po = Popen(['sh', os.path.join(uv_src, 'autogen.sh')])
    if po.wait() != 0:
        raise Exception("Couldn't generate configure script!")
    os.chdir(build_dir)  # Presumably the build dir

    configure = os.path.join(uv_src, 'configure')
    po = Popen([configure, '--prefix', uv_dst, '--with-pic',
                '--disable-shared', '--enable-static', '--disable-silent-rules'])
    if po.wait() != 0:
        raise Exception("Couldn't execute configure")

    print("Executing 'make'")
    po = Popen(['make', '-j', str(options.concurrency), 'install'])
    if po.wait() != 0:
        raise Exception("Couldn't build libuv!")


if not os.path.exists(BUILD_DIR):
    os.makedirs(BUILD_DIR)

if not os.path.exists(UV_A):
    uv_build_dir = os.path.join(BUILD_DIR, 'UV_BUILD')
    if not os.path.exists(uv_build_dir):
        os.makedirs(uv_build_dir)
    build_uv(UV_SRC, UV_DIR, uv_build_dir)

os.chdir(BUILD_DIR)
args = [CMAKE, SOURCE_DIR, '-DLIBUV_ROOT=' + UV_DIR,
        '-DCMAKE_BUILD_TYPE={}'.format('DEBUG' if options.enable_debug else 'RELWITHDEBINFO')]
if options.with_sanitizer == 'asan':
    args += ['-DUSE_ASAN=ON']
elif options.with_sanitizer == 'msan':
    args += ['-DUSE_MSAN=ON']
elif options.with_sanitizer == 'tsan':
    args += ['-DRUSE_TSAN=ON']
if options.with_sanitizer:
    args += ['-DCMAKE_C_COMPILER=clang', '-DCMAKE_CXX_COMPILER=clang++']

args += ['-DVECSIM_ARCH={}'.format(options.vecsim_arch)]

print("Running: " + " ".join(args))
if Popen(args).wait() != 0:
    raise Exception("Couldn't execute CMake!")
