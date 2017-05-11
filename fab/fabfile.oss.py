import os
import itertools
from fabric.api import *
from fabric.contrib import files
import json
env.user = 'redis'

root = os.getenv("REDIS_ROOT", "/home/{}".format(env.user))
git_root = os.getenv("GIT_ROOT", "git@github.com")
apt_gets = ["git-core", "ruby", "build-essential", "gdb", 'htop', 'python-dev', 'python-setuptools', 'unzip']
modules_root = root + '/modules'
cluster_root = root + '/run'

env.roledefs = {
    'redis': [],
}

def git_url(namespace, repo):
    global git_root
    url = '{}:{}/{}'.format(git_root, namespace, repo)
    print url
    return url

@task
@parallel
@roles("redis")
def install_essentials():
    """
    Install essential build requirements for all our projects
    """
    run('mkdir -p {}'.format(modules_root))
    run('mkdir -p {}'.format(cluster_root))
    # Install apt-get deps
    sudo("apt-get update && apt-get -y install {}".format(" ".join(apt_gets)))

    # Install foreman
    sudo("gem install foreman redis")
    

def fetch_git_repo(namespace, name, keyFile = None):
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
@parallel
@roles("redis")
def install_redis():

    if not files.exists('/usr/local/bin/redis-server'):
        run('wget https://github.com/antirez/redis/archive/unstable.zip && unzip -u unstable.zip')

        with cd('redis-unstable'):
            run('make all')
            sudo('make install')
    else:
        print("Redis already installed")

def install_module(module_name, target_name):
    global modules_root

    run('cp -f {} {}/{}'.format(module_name, modules_root, target_name))

def module_path(mod):
    global modules_root
    return '{}/{}'.format(modules_root, mod)

@task
@parallel
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
    return run('host `hostname` | cut -d " " -f 4')

def resolve(host):
    return run('host {} | cut -d " " -f 4'.format(host))

def mkconfig(host, num, num_hosts):
    fname = 'Procfile'
    with open(fname, "w+") as f:
        for i in range(num):
            port = 7000 + i
            f.write("redis-%d: redis-server ./common.conf --port %d --cluster-config-file %d.conf "
                    "--loadmodule %s PARTITIONS %d TYPE redis_oss ENDPOINT localhost:%d " 
                    "--dbfilename %d.rdb\n"
                    % (port, port, port, module_path('rscoord.so'), num*num_hosts, port, port))
    return fname

def config_path(fname):
    global cluster_root
    return '{}/{}'.format(cluster_root, fname)

@task
@parallel
@roles("redis")
def bootstrap_cluster(num_nodes,num_hosts):
    my_ip = get_local_ip()
    
    fname= mkconfig(my_ip, int(num_nodes), int(num_hosts))
    put(fname, config_path(fname))
    put('res/common.conf', config_path('common.conf'))
    put('res/redis-trib.rb', 'redis-trib.rb')
    run('chmod +x ./redis-trib.rb')
    with cd(cluster_root):
        run('rm 70*.conf')

hosts = []
@task
@roles("redis")
@parallel
def collect_host():
    return get_local_ip()

@task
@roles("redis")
@runs_once
def mkcmd(num_nodes):
    hosts = execute(collect_host)

    eps = []
    for h in hosts.values():
        for p in range(7000,7000+int(num_nodes)):
            eps.append('{}:{}'.format(h, p))
    run('chmod +x ./redis-trib.rb')
    run('./redis-trib.rb create --replicas 0 ' + ' '.join(eps))



@task
def deploy():
    execute(install_essentials)
    execute(install_redis)
    execute(install_modules)
    execute(bootstrap_cluster, 10)


