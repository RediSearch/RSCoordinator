from fabric.api import *
from fabric.contrib import files
import os
from fabric import api
import itertools

root = os.getenv("REDIS_ROOT", "/home/ubuntu")
git_root = os.getenv("GIT_ROOT", "https://github.com")
apt_gets = ["git-core", "ruby", "build-essential", "gdb", 'htop', 'libuv-dev' ]
modules_root = root + '/modules'
cluster_root = root + '/run'

env.user = 'ubuntu'
env.roledefs = {
    'redis': [ '52.57.246.105', '52.57.50.243', '52.59.87.185', '52.29.254.134',],
}

def git_url(namespace, repo):
    global git_root
    url = '{}/{}/{}'.format(git_root, namespace, repo)
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
@parallel
@roles("redis")
def install_redis():

    fetch_git_repo('antirez', 'redis')

    with cd('redis'):
        run('make all')
        sudo('make install')
    
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

    #run("echo 'machine github.com login dvirsky password bzpw3e!@' > ~/.netrc && chmod 0600 ~/.netrc")

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
    global roledefs
    fname= mkconfig(my_ip, int(num_nodes), int(num_hosts))
    put(fname, config_path(fname))
    put('common.conf', config_path('common.conf'))
    put('redis-trib.rb', 'redis-trib.rb')
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
