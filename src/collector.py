import logging
import yaml
import datetime

from ssh_client import SshClient, Node

LOG = logging.getLogger('root')


class LoadManager:

    def __init__(self):
        self.ssh = SshClient()
        self.cfg = {}
        with open('config.yml', 'r') as f:
            self.conf = yaml.load(f, Loader=yaml.FullLoader)
            self.cfg = self.conf['load-manager']
            self.registry = list(self.conf['registry']['nodes'].values())[0]
            self.registry_port = self.conf['registry']['clientPort']
        self.host = Node(self.cfg['host'], str(self.cfg['user']),
                                   self.cfg['passwd'], self.cfg['passwordLess'])
        self.cwd = self.cfg['cwd']
        self.jar = self.conf['artifacts']['fat-jar']
        self.jar_remote = self.cwd + '/' + self.jar.split('/')[-1]
        self.log4j = self.conf['artifacts']['log4j']
        self.log4j_remote = self.cwd + '/log4j.properties'
        self.load_data_file = "load.dat"


    def setup(self):
        # create dirs
        self.create_dirs()
        # copy jar to working dir
        self.put_jar()
        # copy log4j file
        self.put_log4j()

    def start(self, sys_props, n_srv, n_cli, req_size):
        cmd = ("java "
               + sys_props
               + " -jar " + self.jar_remote + " generateLoad "
               + self.registry + ":" + str(self.registry_port)
               + " /generateLoad "
               + str(n_srv) + " " + str(n_cli) + " " + str(req_size) + " < "
               + self.cwd + '/' + self.load_data_file
               + " &")
        self.ssh.execute(self.host, cmd)

    def gen_load_file(self, sleep_dur, ext, samples):
        time = 0
        # samples = random.sample(range(0, 101, 10), 11)
        gen_out = self.cwd + "/bench." + ext
        with open(self.load_data_file, 'w') as f:
            f.write("sleep 30\n")
            time += 30
            f.write("save " + gen_out + "\n")
            for per in samples:
                f.write("percentage " + str(per) + "\n")
                f.write("sleep " + str(sleep_dur) + "\n")
                time += sleep_dur
            f.write("exit\n")

        # copy load file
        self.put_loadfile()

        # time in seconds executor should wait before killing lm
        return time

    def stop(self):
        cmd = ("ps aux | grep -v grep | grep " + self.jar_remote
               + " | grep sic- | awk '{print $2}' | xargs kill -9")
        self.ssh.execute(self.host, cmd)

    def destroy(self):
        self.ssh.execute(self.host, "rm -rf " + self.cwd)

    def create_dirs(self):
        self.ssh.execute(self.host, "mkdir -p " + self.cwd)

    def put_jar(self):
        self.ssh.put(self.host, self.jar, self.jar_remote)

    def put_loadfile(self):
        self.ssh.put(self.host, self.load_data_file, self.cwd + '/' + self.load_data_file)

    def put_log4j(self):
        self.ssh.put(self.host, self.log4j, self.log4j_remote)

    def get_bench_data(self, ext):
        da = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.ssh.get(self.host, self.cwd + '/bench.' + ext, "../bench-data/" + da + "." + ext)

    def close_all_cnxs(self):
        self.ssh.close(self.host)