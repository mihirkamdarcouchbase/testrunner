import time, os

from threading import Thread
import threading
from basetestcase import BaseTestCase
from rebalance.rebalance_base import RebalanceBaseTest
from membase.api.exception import RebalanceFailedException
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper


class RebalanceHighOpsWithPillowFight(BaseTestCase):
    def setUp(self):
        super(RebalanceHighOpsWithPillowFight, self).setUp()
        self.rate_limit = self.input.param("rate_limit",100000)
        self.batch_size = self.input.param("batch_size",1000)
        self.doc_size = self.input.param("doc_size",100)

    def tearDown(self):
        super(RebalanceHighOpsWithPillowFight, self).tearDown()

    PREFIX = "test_"

    def load(self, server, items, batch=1000, docsize=100, rate_limit=100000):
        import subprocess
        from lib.testconstants import COUCHBASE_FROM_SPOCK
        rest = RestConnection(server)
        import multiprocessing

        num_cores = multiprocessing.cpu_count()
        num_cycles = items/batch * 2

        cmd = "cbc-pillowfight -U couchbase://{0}/default -I {1} -m {3} -M {3} -B {2} -c {5} --sequential --json -t {4} --rate-limit={6}" \
            .format(server.ip, items, batch, docsize, num_cores/2, num_cycles, rate_limit)
        if rest.get_nodes_version()[:5] in COUCHBASE_FROM_SPOCK:
            cmd += " -u Administrator -P password"
        self.log.info("Executing '{0}'...".format(cmd))
        rc = subprocess.call(cmd, shell=True)
        if rc != 0:
            self.fail("Exception running cbc-pillowfight: subprocess module returned non-zero response!")

    def check_dataloss(self, server, bucket):
        from couchbase.bucket import Bucket
        from couchbase.exceptions import NotFoundError
        from lib.memcached.helper.data_helper import VBucketAwareMemcached
        bkt = Bucket('couchbase://{0}/{1}'.format(server.ip, bucket.name))
        rest = RestConnection(self.master)
        VBucketAware = VBucketAwareMemcached(rest, bucket.name)
        _, _, _ = VBucketAware.request_map(rest, bucket.name)
        batch_start = 0
        batch_end = 0
        batch_size = 10000
        errors = []
        while self.num_items > batch_end:
            batch_end = batch_start + batch_size
            keys = []
            for i in xrange(batch_start, batch_end, 1):
                keys.append(str(i).rjust(20, '0'))
            try:
                bkt.get_multi(keys)
                self.log.info("Able to fetch keys starting from {0} to {1}".format(keys[0], keys[len(keys)-1]))
            except Exception as e:
                self.log.error(e)
                self.log.info("Now trying keys in the batch one at a time...")
                key = ''
                try:
                    for key in keys:
                        bkt.get(key)
                except NotFoundError:
                    vBucketId = VBucketAware._get_vBucket_id(key)
                    errors.append("Missing key: {0}, VBucketId: {1}".
                                  format(key, vBucketId))
            batch_start += batch_size
        return errors

    def test_rebalance_in(self):
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = Thread(target=self.load,
                               name="pillowfight_load",
                               args=(self.master, self.num_items, self.batch_size, self.doc_size, self.rate_limit))

        self.log.info('starting the load thread...')
        load_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[self.nodes_init:self.nodes_init + self.nodes_in],
                                                 [])
        rebalance.result()
        load_thread.join()
        errors = self.check_dataloss(self.master, bucket)
        if errors:
            self.log.info("Printing missing keys:")
        for error in errors:
            print error
        if self.num_items != rest.get_active_key_count(bucket):
            self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                          format(self.num_items, rest.get_active_key_count(bucket) ))


