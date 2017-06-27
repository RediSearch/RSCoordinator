import os
import itertools
from fabric.api import *
from fabric.contrib import files
import json

env.user = 'ubuntu'
root = os.getenv("REDIS_ROOT", "/home/{}".format(env.user))
git_root = "git@github.com"
rlec_uid = 'redislabs'
rlec_user = 'search@redislabs.com'
rlec_pass = 'search1234'
rlec_license_file = 'license.txt'
rlec_path = root + '/data'
#modules_root = root + '/modules'
cluster_root = root + '/run'




apt_gets = ["git-core", "ruby", "build-essential", "gdb", 'htop', 'libuv-dev', 'python-dev', 'python-setuptools', 'python-pip', 'unzip', 's3cmd']
pips = ['ramp-packer', 'boto']

env.roledefs = {
    'rlec':  os.getenv('RL_HOSTS', '').split(','),
    'rlec_master': os.getenv('RL_MASTER', '').split(','),
}

@task
@parallel
@roles("rlec", "rlec_master")
def install_essentials():
    """
    Install essential build requirements for all our projects
    """
    #run('mkdir -p {}'.format(modules_root))
    run('mkdir -p {}'.format(cluster_root))
    # Install apt-get deps
    sudo("apt-get update && apt-get -y install {}".format(" ".join(apt_gets)))

    # Install global pip deps
    sudo('pip install {}'.format(' '.join(pips)))
    
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
@roles("rlec")
def install_rlec():
    package_name = 'RediSearchPackage.tar'
    if True: #not files.exists(package_name):
        put(local_resource(package_name), package_name)
        run('tar -xf {}'.format(package_name))
    sudo('./install.sh -y')
    sudo('mkdir -p {} && chown -R {}.{} {}'.format(rlec_path, rlec_uid, rlec_uid, rlec_path))


@task   
@parallel
@roles("rlec_master")
def install_s3_creds():

    global root
    if not files.exists('{}/.s3cfg'.format(root)):
        put('./res/s3cfg', '{}/.s3cfg'.format(root))

def deploy_module(module):
    resp = json.loads(run('curl -k -u "{}:{}" -F "module=@{}" https://127.0.0.1:9443/v1/modules'.format(
        rlec_user, rlec_pass, module)))
    return resp['uid']

def rladmin(cmd):
    sudo('/opt/redislabs/bin/rladmin ' + cmd)

@task
@roles("rlec_master")
def create_database(db_name, num_shards):

    rladmin('tune cluster default_shards_placement sparse')
    search_uid = deploy_module( 'redisearch.zip')
    coord_uid = deploy_module( 'rscoord.zip')
    run("""curl -k -X POST -u "{rlec_user}:{rlec_pass}" -H "Content-Type: application/json" \
        -d '{{ "name": "{db_name}", "replication":true, "sharding":true, "shards_count":{num_shards}, "version": "4.0", "memory_size": {mem_size}, "type": "redis", \
        "module_list":["{coord_uid}","{search_uid}"], "module_list_args":["PARTITIONS {num_partitions} TYPE redislabs", "PARTITIONS {num_partitions} TYPE redislabs"] }}' \
        https://127.0.0.1:9443/v1/bdbs""".format(rlec_user=rlec_user, rlec_pass=rlec_pass, db_name=db_name,
        num_shards=num_shards, num_partitions=num_shards, mem_size=5000000000*int(num_shards), search_uid=search_uid, coord_uid=coord_uid))
    

def get_module(modulename):
    
    run('s3cmd get --force s3://redismodules/{}/{}.`uname -s`-`uname -m`.latest.zip {}.zip'.format(modulename, modulename, modulename))

@task
@roles("rlec_master")
def get_modules():
    """
    Download the latest versions of both needed modules to the destination server
    """

    get_module('redisearch')
    get_module('rscoordinator')
  


def local_resource(fname):
    return 'res/{}'.format(fname)

@task
@runs_once
@roles("rlec_master")
def bootstrap_rlec_cluster(cluster_name):
    put(local_resource(rlec_license_file), rlec_license_file)
    rladmin("cluster create name {} username \"{}\" password \"{}\" persistent_path \"{}\" license_file \"{}\"".format(
        cluster_name, rlec_user, rlec_pass, rlec_path, rlec_license_file
    ))

@task
@serial
@roles("rlec")
def join_rlec(cluster_host):
    ip = cluster_host
    rladmin("cluster join nodes \"{}\" username \"{}\" password \"{}\" persistent_path \"{}\"".format(
        ip, rlec_user, rlec_pass, rlec_path
    ))

@task
@parallel
@roles("rlec", "rlec_master")
def install_redis():

    if not files.exists('/usr/local/bin/redis-server'):
        run('wget https://github.com/antirez/redis/archive/unstable.zip && unzip -u unstable.zip')

        with cd('redis-unstable'):
            run('make -j4 all')
            sudo('make install')
    else:
        print("Redis already installed")

def module_path(mod):
    return mod
    


def get_local_ip():
    return run('host `hostname` | cut -d " " -f 4')

def resolve(host):
    return run('host {} | cut -d " " -f 4'.format(host))


@task
def upgrade_bdb_module(bdb_uid, module_list):
    # overrides bdb's module list with the one provided.
    # TODO: retrieve BDB's module list and update instead of overriding.
    run("""curl -k -X PUT -u "{rlec_user}:{rlec_pass}" -H "Content-Type: application/json" \
        -d '{ "module_list": {module_list} }' \
        https://127.0.0.1:9443/v1/bdbs/{uid}""".format(rlec_user=rlec_user, rlec_pass=rlec_pass, module_list=json.dumps(module_list), uid=bdb_uid))

@task
def deploy_rlec():
    execute(install_essentials)
    execute(install_redis)
    execute(install_rlec)

@task
def download_modules():
    execute(install_s3_creds)
    execute(get_modules)


