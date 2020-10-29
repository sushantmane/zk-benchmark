import logging
import paramiko

from re import escape

LOG = logging.getLogger('root')
logging.getLogger("paramiko").setLevel(logging.WARNING)


class Node:

    def __init__(self, hostname: str, usr: str, passwd: str, passwd_less: bool):
        self.hostname = hostname
        self.usr = usr
        self.passwd = passwd
        self.passwd_less = passwd_less


class SshClient(object):

    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            LOG.info("creating instance of ssh client...")
            cls.__instance = super(SshClient, cls).__new__(cls)
            cls.cnxs = dict()
        return cls.__instance

    def _get_client(self, node: Node):
        cli = self.cnxs.get(node.hostname)

        # disable connection caching

        # if cli is not None and cli.get_transport() is not None and cli.get_transport().is_active():
        #     return cli
        if cli is not None:
            try:
                cli.close()
            except Exception:
                pass

        cli = paramiko.SSHClient()
        # cli.load_system_host_keys()
        cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if node.passwd_less:
            cli.connect(hostname=node.hostname, username=node.usr)
        else:
            cli.connect(hostname=node.hostname, username=node.usr, password=node.passwd)
        LOG.info("connected to %s...", node.hostname)
        self.cnxs[node.hostname] = cli
        return cli

    def execute(self, node: Node, cmd: str):
        LOG.info('execute - host: %s cmd: %s', node.hostname, cmd)
        client = self._get_client(node)
        _in, _out, _err = client.exec_command(cmd)
        ready = _out.channel.exit_status_ready()
        rc = _out.channel.recv_exit_status()
        # LOG.info("rc: %s", rc)
        return Response(rc, ready, _in, _out, _err)

    def put(self, remote_host: Node, src: str, dst: str):
        LOG.info("copy - %s on %s:%s", src, remote_host.hostname, dst)
        self.close(remote_host)
        sftp = self._get_client(remote_host).open_sftp()
        sftp.put(src, dst)
        LOG.info("copied - src:%s  dst:%s:%s", src, remote_host.hostname, dst)
        sftp.close()

    def get(self, remote_host: Node, src: str, dst: str):
        LOG.info("copy - %s:%s to %s", remote_host.hostname, dst, src)
        sftp = self._get_client(remote_host).open_sftp()
        sftp.get(src, dst)
        sftp.close()

    def close(self, node: Node):
        LOG.info("closing connection to %s...", node.hostname)
        cnx = self.cnxs[node.hostname]
        if cnx is not None:
            cnx.close()
            del self.cnxs[node.hostname]


class Response:

    def __init__(self, rc, ready, _in, _out, _err):
        self.rc = rc
        self.ready = ready
        self._in = _in
        self._out = _out
        self._err = _err
