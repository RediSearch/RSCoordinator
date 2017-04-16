import os
import itertools
from fabric.api import *
from fabric.contrib import files
import json
env.user = 'redis'

root = os.getenv("REDIS_ROOT", "/home/{}".format(env.user))
git_root = os.getenv("GIT_ROOT", "git@github.com")
apt_gets = ["git-core", "ruby", "build-essential", "gdb", 'htop', 'libuv-dev', 'python-dev', 'python-setuptools', 'unzip']
modules_root = root + '/modules'
cluster_root = root + '/run'

env.roledefs = {
    'redis': [],#'52.57.246.105', '52.57.50.243', '52.59.87.185', '52.29.254.134'],
    'rlec': ['104.199.38.214', '104.199.96.128','35.187.4.125', '104.155.31.211', '104.199.110.108',
             '35.187.89.0', '130.211.100.195','35.187.53.172','130.211.67.244'  ],
    'rlec_master': ['23.251.139.76'],
}

def git_url(namespace, repo):
    global git_root
    url = '{}:{}/{}'.format(git_root, namespace, repo)
    print url
    return url

@task
@parallel
@roles("redis", "rlec", "rlec_master")
def install_essentials():
    """
    Install essential build requirements for all our projects
    """
    run('mkdir -p {}'.format(modules_root))
    run('mkdir -p {}'.format(cluster_root))
    # Install apt-get deps
    sudo("apt-get update && apt-get -y install {}".format(" ".join(apt_gets)))

    # Install foreman
    #sudo("gem install foreman redis")
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
@roles("rlec", "rlec_master")
def install_rlec():
    package_name = 'RediSearchPackage.tar'
    if not files.exists(package_name):
        put(package_name, package_name)
        run('tar -xf {}'.format(package_name))
    sudo('./install.sh -y')

@task
@parallel
@roles("rlec_master")
def install_ramp():
    fetch_git_repo('RedisLabs', 'RAMP')
    with cd('RAMP'):
        sudo('python setup.py build install')

@task
@roles("rlec_master")
def deploy_ssh_key(generate=False):
    if generate:
        local('sshkeygen -P "" -t rsa -f coordinator.key')
    put('coordinator.key', '~/.ssh/id_rsa', mode=0600)
    put('coordinator.key.pub', '~/.ssh/id_rsa.pub', mode=0600)



def deploy_module(module):
    resp =  json.loads(run('curl -k -u "{}:{}" -F "module=@{}" https://127.0.0.1:9443/v1/modules'.format(rlec_user, rlec_pass, module)))
    return resp['uid']

def rladmin(cmd):
    sudo('/opt/redislabs/bin/rladmin ' + cmd)

@task
@roles("rlec_master")
def create_database(db_name, num_shards, num_partitions):

    rladmin('tune cluster default_shards_placement sparse')
    search_uid = deploy_module( 'redisearch.zip')
    coord_uid = deploy_module( 'rscoord.zip')
    run("""curl -k -X POST -u "{rlec_user}:{rlec_pass}" -H "Content-Type: application/json" \
        -d '{{ "name": "{db_name}", "replication":true, "sharding":true, "shards_count":{num_shards}, "version": "4.0", "memory_size": {mem_size}, "type": "redis", \
        "module_list":["{coord_uid}","{search_uid}"], "module_list_args":["PARTITIONS {num_partitions} TYPE redislabs", "PARTITIONS {num_partitions} TYPE redislabs"] }}' \
        https://127.0.0.1:9443/v1/bdbs""".format(rlec_user=rlec_user, rlec_pass=rlec_pass, db_name=db_name,
        num_shards=num_shards, num_partitions=num_partitions, mem_size=5000000000*int(num_shards), search_uid=search_uid, coord_uid=coord_uid))
    

@task
@roles("rlec_master")
def pack_modules():

    fetch_git_repo('RedisLabsModules', 'RediSearch')
    
    with cd('RediSearch/src'):
        run('make release')

        run('module_packer `pwd`/module.so -ar 64 && mv ./module.zip ~/redisearch.zip')

    fetch_git_repo('RedisLabsModules', 'RSCoordinator')
    with cd('RSCoordinator/src'):
        run('make all')
        run('module_packer `pwd`/module.so -ar 64 -c "TYPE redislabs PARTITIONS 1" && mv ./module.zip ~/rscoord.zip')


rlec_user = 'search@redislabs.com'
rlec_pass = 'search1234'
rlec_license_file = 'license.txt'

@task
@runs_once
@roles("rlec_master")
def bootstrap_rlec_cluster(cluster_name):
    put(rlec_license_file, rlec_license_file)
    rladmin("cluster create name {} username \"{}\" password \"{}\" license_file \"{}\"".format(
        cluster_name, rlec_user, rlec_pass, rlec_license_file
    ))

@task
@serial
@roles("rlec")
def join_rlec(cluster_host):
    ip = resolve(cluster_host)
    rladmin("cluster join nodes \"{}\" username \"{}\" password \"{}\"".format(
        ip, rlec_user, rlec_pass
    ))

@task
@parallel
@roles("redis", "rlec", "rlec_master")
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


@task
def deploy_rlec():
    execute(install_essentials)
    execute(install_redis)
    execute(install_rlec)

@task
def prepare_rlec_master():#cluster_name,db_name,num_shards,num_partitions):
    execute(deploy_ssh_key)
    execute(install_ramp)
    execute(pack_modules)
    execute(bootstrap_rlec_cluster, cluster_name)
    #execute(create_database, db_name,num_shards,num_partitions)
    #execute(bootstrap_cluster, 10)

@task
def upgrade_bdb_module(bdb_uid, module_list):
    # overrides bdb's module list with the one provided.
    # TODO: retrieve BDB's module list and update instead of overriding.
    run("""curl -k -X PUT -u "{rlec_user}:{rlec_pass}" -H "Content-Type: application/json" \
        -d '{ "module_list": {module_list} }' \
        https://127.0.0.1:9443/v1/bdbs/{uid}""".format(rlec_user=rlec_user, rlec_pass=rlec_pass, module_list=module_list, uid=bdb_uid))