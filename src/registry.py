import datetime
import logging
from typing import Dict

import yaml

from ssh_client import Node
from ssh_client import SshClient

LOG = logging.getLogger('root')


class ZooRegistry:

    nodes: Dict[int, Node]

    def __init__(self):
        self.ssh = SshClient()
        self.cfg = {}
        with open('config.yml', 'r') as f:
            self.conf = yaml.load(f, Loader=yaml.FullLoader)
            self.cfg = self.conf['registry']
        self.nodes = dict()
        for nid in self.cfg['nodes']:
            self.nodes[nid] = Node(self.cfg['nodes'][nid], str(self.cfg['user']),
                                   self.cfg['passwd'], self.cfg['passwordLess'])
        # self.cwd = self.cfg['cwd-prefix'] + datetime.datetime.now().strftime('-%Y%m%d%H%M%S')
        self.cwd = self.cfg['cwd-prefix']
        self.jar = self.conf['artifacts']['fat-jar']
        self.jar_remote = self.cwd + '/' + self.jar.split('/')[-1]
        self.zkcf_remote = self.cwd + '/' + 'zoo.cfg'
        self.zoo_data_dir = self.cfg['dataDir']
        # if path is relative create data dir under working directory
        if not self.zoo_data_dir.startswith('/'):
            self.zoo_data_dir = self.cwd + '/' + self.cfg['dataDir']
        self.qrm_port = self.cfg['quorumPort']
        self.ele_port = self.cfg['leaderElectionPort']
        self.client_port = self.cfg['clientPort']
        self.tick_time = self.cfg['tickTime']
        self.init_limit = self.cfg['initLimit']
        self.sync_limit = self.cfg['syncLimit']
        self.log4j = self.conf['artifacts']['log4j']
        self.log4j_remote = self.cwd + '/log4j.properties'
        self.java_sys_props = self.cfg['java-sys-properties']
        if self.java_sys_props is None:
            self.java_sys_props = ''

    def get_zoo_cf(self):
        zc = str()
        for nid in self.nodes:
            zc += 'server.' + str(nid) + '=' + self.nodes.get(nid).hostname
            zc += ':' + str(self.qrm_port) + ':' + str(self.ele_port) + '\n'
        zc += 'dataDir=' + self.zoo_data_dir + '\n'
        zc += 'clientPort=' + str(self.client_port) + '\n'
        zc += 'tickTime=' + str(self.tick_time) + '\n'
        zc += 'initLimit=' + str(self.init_limit) + '\n'
        zc += 'syncLimit=' + str(self.sync_limit) + '\n'
        LOG.debug("generated zoo config: {}", zc)
        return zc

    def run_on_all(self, cmd):
        for nid in self.nodes:
            self.ssh.execute(self.nodes.get(nid), cmd)

    def setup(self):
        # create working dir
        self.create_cwd()
        # create data dir directory
        self.create_zdd()
        # generate zoo.cfg and copy to working dir
        self.create_zk_cf()
        # create myid file on all nodes
        self.create_myid()
        # copy jar to working dir
        self.put_jar()
        # copy log4j
        self.put_log4j()

    def start(self):
        cmd = ('java ' + self.java_sys_props
               + ' -Dzookeeper.log.dir=' + self.cwd
               + ' -Dlog4j.configuration=file:' + self.log4j_remote
               + " -DsnapDir=" + self.zoo_data_dir + " -DlogDir=" + self.zoo_data_dir
               + ' -jar ' + self.jar_remote
               + ' server ' + self.zkcf_remote + ' &')
        self.run_on_all(cmd)

    def stop(self):
        cmd = "ps aux | grep -v grep | grep " + self.jar_remote + " | grep server | awk '{print $2}' | xargs kill -9"
        self.run_on_all(cmd)

    def destroy(self):
        # delete zookeeper data directory
        self.run_on_all("rm -rf " + self.zoo_data_dir)
        # delete working directory
        self.run_on_all("rm -rf " + self.cwd)
        # close all active ssh connections
        # self.close_all_cnxs()

    # delete zk registry logs and snapshots
    def del_zkreg_data(self):
        self.run_on_all("rm -rf " + self.zoo_data_dir + "/version-2")

    def create_cwd(self):
        self.run_on_all("mkdir -p " + self.cwd)

    def create_zdd(self):
        self.run_on_all("mkdir -p " + self.zoo_data_dir)

    def create_zk_cf(self):
        cmd = 'echo "' + self.get_zoo_cf() + '" > ' + self.zkcf_remote
        self.run_on_all(cmd)

    def create_myid(self):
        for nid in self.nodes:
            cmd = 'echo "' + str(nid) + '" > ' + self.zoo_data_dir + '/' + 'myid'
            self.ssh.execute(self.nodes.get(nid), cmd)

    def put_jar(self):
        for nid in self.nodes:
            self.ssh.put(self.nodes.get(nid), self.jar, self.jar_remote)

    def put_log4j(self):
        for nid in self.nodes:
            self.ssh.put(self.nodes.get(nid), self.log4j, self.log4j_remote)

    def close_all_cnxs(self):
        for nid in self.nodes:
            self.ssh.close(self.nodes.get(nid))
