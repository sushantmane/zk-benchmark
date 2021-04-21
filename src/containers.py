import logging
import yaml

from ssh_client import SshClient, Node

LOG = logging.getLogger('root')


class ServerContainer:

    def __init__(self):
        self.ssh = SshClient()
        self.cfg = {}
        with open('config.yml', 'r') as f:
            self.conf = yaml.load(f, Loader=yaml.FullLoader)
            self.cfg = self.conf['server-containers']
            self.registry = list(self.conf['registry']['nodes'].values())[0]
            self.registry_port = self.conf['registry']['clientPort']
        self.nodes = dict()
        for nid in self.cfg['nodes']:
            self.nodes[nid] = Node(self.cfg['nodes'][nid], str(self.cfg['user']),
                                   self.cfg['passwd'], self.cfg['passwordLess'])
        # self.cwd = self.cfg['cwd-prefix'] + datetime.datetime.now().strftime('-%Y%m%d%H%M%S')
        self.cwd = self.cfg['cwd-prefix']
        self.jar = self.conf['artifacts']['fat-jar']
        self.jar_remote = self.cwd + '/' + self.jar.split('/')[-1]
        self.ic_test_dir = self.cfg['icTestDir']
        self.ic_data_dir = self.cfg['icDataDir']
        self.ic_log_dir = self.cfg['icDataLogDir']
        self.log4j = self.conf['artifacts']['log4j']
        self.log4j_remote = self.cwd + '/log4j.properties'

    def run_on_all(self, cmd):
        for nid in self.nodes:
            self.ssh.execute(self.nodes.get(nid), cmd)

    def setup(self):
        # create dirs
        self.create_dirs()
        # copy jar to working dir
        self.put_jar()
        # copy log4j file
        self.put_log4j()

    # for forcing the use of specific ip, add ip address to use under nodes in config file
    # and then set use_ic_ip to True
    def start(self, sys_props=" -Dzookeeper.digest.enabled=false ", use_ic_ip=False):
        for nid in self.nodes:
            ic_ip = ' '
            if use_ic_ip:
                ic_ip = " -Dtest.ic.ip=" + self.nodes.get(nid).hostname

            cmd = ("java " + sys_props
                   + ic_ip
                   + " -Dzookeeper.4lw.commands.whitelist=* "
                   + " -Dzookeeper.log.dir=" + self.cwd
                   + " -Dlog4j.configuration=file:" + self.log4j_remote
                   + " -Dtest.data.dir=" + self.ic_test_dir
                   + " -DsnapDir=" + self.ic_data_dir + " -DlogDir=" + self.ic_log_dir
                   + " -jar " + self.jar_remote + " ic " + "sic-" + str(nid) + " "
                   + self.registry + ":" + str(self.registry_port)
                   + " /generateLoad &")
            self.ssh.execute(self.nodes.get(nid), cmd)

    def stop(self):
        cmd = ("ps aux | grep -v grep | grep " + self.jar_remote
               + " | grep sic- | awk '{print $2}' | xargs kill -9")
        self.run_on_all(cmd)

    # delete zk server instance container data
    def del_sic_data(self):
        self.run_on_all("rm -rf " + self.ic_log_dir.strip() + "/*")
        self.run_on_all("rm -rf " + self.ic_data_dir.strip() + "/*")
        self.run_on_all("rm -rf " + self.ic_test_dir.strip() + "/*")

    def destroy(self):
        # delete zookeeper data directory
        self.run_on_all("rm -rf " + self.ic_log_dir)
        self.run_on_all("rm -rf " + self.ic_data_dir)
        self.run_on_all("rm -rf " + self.ic_test_dir)
        self.run_on_all("rm -rf " + self.cwd)
        # close all active ssh connections
        # self.close_all_cnxs()

    def create_dirs(self):
        self.run_on_all("mkdir -p " + self.cwd)
        self.run_on_all("mkdir -p " + self.ic_test_dir)
        self.run_on_all("mkdir -p " + self.ic_data_dir)
        self.run_on_all("mkdir -p " + self.ic_log_dir)

    def put_jar(self):
        for nid in self.nodes:
            self.ssh.put(self.nodes.get(nid), self.jar, self.jar_remote)

    def put_log4j(self):
        for nid in self.nodes:
            self.ssh.put(self.nodes.get(nid), self.log4j, self.log4j_remote)

    def close_all_cnxs(self):
        for nid in self.nodes:
            self.ssh.close(self.nodes.get(nid))


class ClientContainer:

    def __init__(self):
        self.ssh = SshClient()
        self.cfg = {}
        with open('config.yml', 'r') as f:
            self.conf = yaml.load(f, Loader=yaml.FullLoader)
            self.cfg = self.conf['client-containers']
            self.registry = list(self.conf['registry']['nodes'].values())[0]
            self.registry_port = self.conf['registry']['clientPort']
        self.nodes = dict()
        for nid in self.cfg['nodes']:
            self.nodes[nid] = Node(self.cfg['nodes'][nid], str(self.cfg['user']),
                                   self.cfg['passwd'], self.cfg['passwordLess'])
        self.cwd = self.cfg['cwd-prefix']
        self.jar = self.conf['artifacts']['fat-jar']
        self.jar_remote = self.cwd + '/' + self.jar.split('/')[-1]
        self.log4j = self.conf['artifacts']['log4j']
        self.log4j_remote = self.cwd + '/log4j.properties'

    def run_on_all(self, cmd):
        for nid in self.nodes:
            self.ssh.execute(self.nodes.get(nid), cmd)

    def setup(self):
        # create dirs
        self.create_dirs()
        # copy jar to working dir
        self.put_jar()
        # copy log4j file
        self.put_log4j()

    def start(self, sys_props=''):
        for nid in self.nodes:
            cmd = ("java " + sys_props
                   + " -Dzookeeper.log.dir=" + self.cwd
                   + " -Dlog4j.configuration=file:" + self.log4j_remote
                   + " -jar " + self.jar_remote + " ic " + " cic-" + str(nid) + " "
                   + self.registry + ":" + str(self.registry_port)
                   + " /generateLoad &")
            self.ssh.execute(self.nodes.get(nid), cmd)

    def stop(self):
        cmd = ("ps aux | grep -v grep | grep " + self.jar_remote
               + " | grep cic- | awk '{print $2}' | xargs kill -9")
        self.run_on_all(cmd)

    def destroy(self):
        self.run_on_all("rm -rf " + self.cwd)

    def create_dirs(self):
        self.run_on_all("mkdir -p " + self.cwd)

    def put_jar(self):
        for nid in self.nodes:
            self.ssh.put(self.nodes.get(nid), self.jar, self.jar_remote)

    def put_log4j(self):
        for nid in self.nodes:
            self.ssh.put(self.nodes.get(nid), self.log4j, self.log4j_remote)

    def close_all_cnxs(self):
        for nid in self.nodes:
            self.ssh.close(self.nodes.get(nid))
