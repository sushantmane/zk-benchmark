import logging
import time

from containers import ServerContainer, ClientContainer
from registry import ZooRegistry
from collector import LoadManager

LOG = logging.getLogger('root')
logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s: %(message)s")

sic = ServerContainer()
cic = ClientContainer()
reg = ZooRegistry()
lm = LoadManager()


def setup():
    reg.setup()
    sic.setup()
    cic.setup()
    lm.setup()


def start(sys_props):
    LOG.info("starting registry...")
    reg.start()
    # time.sleep(2)
    LOG.info("starting server ics...")
    sic.start(sys_props)
    # time.sleep(2)
    LOG.info("starting client ics...")
    cic.start()
    # time.sleep(2)


def stop():
    LOG.info("stopping client ics...")
    cic.stop()
    LOG.info("stopping server ics...")
    sic.stop()
    LOG.info("stopping registry ics...")
    reg.stop()


def destroy():
    cic.destroy()
    sic.destroy()
    reg.destroy()
    lm.destroy()


def del_data():
    sic.del_sic_data()
    reg.del_zkreg_data()


def run_test(name='', algo='', sys_prop='', reqsz=1024, servers=3, clients=9):
    LOG.info("BENCH: %s", name)
    LOG.info("__config__ - props:%s reqsz:%s server:%s clients:%s",
             sys_prop, reqsz, servers, clients)

    # start registry and instance containers
    start(sys_prop)

    samples = range(0, 101, 40)
    tm = lm.gen_load_file(24, name, samples)

    LOG.info("start load manager...")
    lm.start(sys_prop, servers, clients, reqsz)

    LOG.info("waiting...")
    time.sleep(tm)

    LOG.info("pull bench data file...")
    lm.get_bench_data(name)

    LOG.info("stop load manager...")
    lm.stop()

    LOG.info("stop ics and registry...")
    # stop registry and instance containers
    stop()

    LOG.info("delete data...")
    # delete data from prev run
    del_data()


def test_noadhash_digest():
    LOG.info("===> test: no adhash (no digest check) ")
    reqsz_bytes = [1024]
    for sz in reqsz_bytes:
        name = "NA_" + str(sz) + "KiB"
        run_test(name=name, algo="NA", sys_prop=' -Dzookeeper.digest.enabled=false ', reqsz=sz)


def test_digest():
    hash_algos = ['CRC-32', 'SHA', 'SHA-256', 'SHA-512', 'MD5']
    reqsz_bytes = [1024, 2048, 4096, 65536]
    predictive_digest = [True, False]
    for pd in predictive_digest:
        for sz in reqsz_bytes:
            for ha in hash_algos:
                name = ha + '_PD-' + str(pd) + '_' + str(sz) + 'KiB'
                prop = (' -Dzookeeper.digest.enabled=true'
                        + ' -Dzookeeper.digest.algorithm=' + str(ha)
                        + ' -Dzookeeper.predictive.digest=' + str(pd).lower() + ' ')
                run_test(name=name, algo=ha, sys_prop=prop, reqsz=sz)


if __name__ == '__main__':
    setup()
    # Three types of tests
    # test_noadhash_digest()
    test_digest()
    stop()
    destroy()
