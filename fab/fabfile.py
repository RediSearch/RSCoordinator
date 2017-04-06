from fabric.api import *
from fabric.contrib import files
import os
from fabric import api

root = os.getenv("REDIS_ROOT", "/home/ubuntu")
git_root = os.getenv("GIT_ROOT", "https://github.com")
apt_gets = ["git-core", "ruby", "build-essential", "gdb", 'htop', 'libuv-dev' ]
modules_root = root + '/modules'

env.user = 'ubuntu'
env.roledefs = {
    'redis': ['52.29.254.134'],
}

def git_url(namespace, repo):
    global git_root
    url = '{}/{}/{}'.format(git_root, namespace, repo)
    print url
    return url

@task
@runs_once
@roles("redis")
def install_essentials():
    """
    Install essential build requirements for all our projects
    """
    run('mkdir -p {}'.format(modules_root))
    # Install apt-get deps
    sudo("apt-get update && apt-get -y install {}".format(" ".join(apt_gets)))

    # Install foreman
    sudo("gem install foreman")
    # Install golang
    # run("wget https://storage.googleapis.com/golang/go1.5.3.linux-amd64.tar.gz")
    # sudo("tar -C /usr/local -xzf go1.5.3.linux-amd64.tar.gz")

    # # Install godep - go dependency vendor
    # run("go get github.com/tools/godep")

    # #Create logging directory
    # sudo("mkdir /var/log/evme")
    # sudo("chown -R evme /var/log/evme")

    # # Install foreman
    # sudo("gem install foreman")

def fetch_git_repo(namespace, name):
    """
    Fetch sources from a git repo by cloning or pulling
    """

    puts("Cloning {} from git...".format(git_url(namespace, name)))

    with settings(warn_only=True):

        if files.exists(name):
            with cd(name):
                run("git pull -ff origin master")
        else:
            run("git clone --depth 1 {}".format(git_url(namespace, name)))


@task
@roles("redis")
def install_redis():

    fetch_git_repo('antirez', 'redis')

    with cd('redis'):
        run('make clean distclean all')
        sudo('make install')
    
def install_module(module_name, target_name):
    global modules_root

    run('cp -f {} {}/{}'.format(module_name, modules_root, target_name))

@task
@roles("redis")
def install_modules():
    fetch_git_repo('RedisLabsModules', 'RediSearch')
    fetch_git_repo('RedisLabsModules', 'RSCoordinator')

    with cd('RediSearch/src'):
        run('make release')

        install_module('module.so', 'redisearch.so')
    with cd('RSCoordinator/src'):
        run('make')
        install_module('module.so', 'rscoord.so')

def get_local_ip():
    return run('curl checkip.amazonaws.com')

@task
@roles("redis")
def bootstrap_cluster(num_nodes):
    my_ip = get_local_ip()
    print("My Ip:", myIp)

@task
def deploy():
    execute(install_essentials)
    execute(install_redis)
    execute(install_modules)