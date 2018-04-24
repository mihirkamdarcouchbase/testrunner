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
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster, PasswordAuthenticator
from couchbase.exceptions import NotFoundError, CouchbaseError
from lib.memcached.helper.data_helper import VBucketAwareMemcached


class RebalanceHighOpsWithPillowFight(BaseTestCase):
    def setUp(self):
        super(RebalanceHighOpsWithPillowFight, self).setUp()
        self.rate_limit = self.input.param("rate_limit",100000)
        self.batch_size = self.input.param("batch_size",1000)
        self.doc_size = self.input.param("doc_size",100)
        self.loader = self.input.param("loader", "pillowfight")
        self.instances = self.input.param("instances", 1)
        self.recovery_type = self.input.param("recovery_type", None)
        self.node_out = self.input.param("node_out", 0)

    def tearDown(self):
        super(RebalanceHighOpsWithPillowFight, self).tearDown()

    def load_buckets_with_high_ops(self, server, bucket, items, batch=20000,
                                   threads=5, start_document=0, instances=1):
        import subprocess
        from lib.testconstants import COUCHBASE_FROM_SPOCK
        cmd_format = "python scripts/high_ops_doc_loader.py  --node {0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} " \
                     "--batch_size {5} --threads {6} --start_document {7}"
        if instances > 1:
            cmd = cmd_format.format(server.ip, bucket.name,
                                    server.rest_username, server.rest_password,
                                    int(items) / int(instances), batch, threads,
                                    start_document)
        else:
            cmd = cmd_format.format(server.ip, bucket.name,
                                    server.rest_username, server.rest_password,
                                    items, batch,
                                    threads, start_document)
        if instances > 1:
            for i in range(1, instances):
                count = int(items) / int(instances)
                start = count * i + int(start_document)
                if i == instances - 1:
                    count = items - (count * i)
                cmd = "{} & {}".format(cmd,
                                       cmd_format.format(server.ip, bucket.name,
                                                         server.rest_username,
                                                         server.rest_password,
                                                         count, batch, threads,
                                                         start))
        if RestConnection(server).get_nodes_version()[:5] < '5':
            cmd = cmd.replace(
                "--user {0} --password {1} ".format(server.rest_username,
                                                    server.rest_password), '')
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        if error:
            self.log.error(error)
            self.fail("Failed to run the loadgen.")
        if output:
            loaded = output.split('\n')[:-1]
            total_loaded = 0
            for load in loaded:
                total_loaded += int(load)
            self.assertEqual(total_loaded, items,
                             "Failed to load {} items. Loaded only {} items".format(
                                 items,
                                 total_loaded))

    def load(self, server, items, batch=1000, docsize=100, rate_limit=100000, start_at=0):
        import subprocess
        from lib.testconstants import COUCHBASE_FROM_SPOCK
        rest = RestConnection(server)
        import multiprocessing

        num_threads = multiprocessing.cpu_count()/2
        num_cycles = int(items/batch * 1.5 / num_threads)

        cmd = "cbc-pillowfight -U couchbase://{0}/default -I {1} -m {3} -M {3} -B {2} -c {5} --sequential --json -t {4} --rate-limit={6} --start-at={7}"\
            .format(server.ip, items, batch, docsize, num_threads, num_cycles, rate_limit, start_at)
        #cmd = "cbc-pillowfight -U couchbase://{0}/default -I {1} -m {3} -M {3} -B {2} --populate-only --sequential --json -t {4} --rate-limit={6}" \
        #        .format(server.ip, items, batch, docsize, num_cores/2, num_cycles, rate_limit)
        if rest.get_nodes_version()[:5] in COUCHBASE_FROM_SPOCK:
            cmd += " -u Administrator -P password"
        self.log.info("Executing '{0}'...".format(cmd))
        rc = subprocess.call(cmd, shell=True)
        if rc != 0:
            self.fail("Exception running cbc-pillowfight: subprocess module returned non-zero response!")

    def load_docs(self, num_items=0, start_document=0):
        if num_items == 0:
            num_items = self.num_items
        if self.loader == "pillowfight":
            load_thread = Thread(target=self.load,
                                 name="pillowfight_load",
                                 args=(
                                 self.master, num_items, self.batch_size,
                                 self.doc_size, self.rate_limit, start_document))
            return load_thread
        elif self.loader == "high_ops":
            load_thread = Thread(target=self.load_buckets_with_high_ops,
                                 name="high_ops_load",
                                 args=(self.master, self.buckets[0], num_items,
                                       self.batch_size,
                                       self.rate_limit, start_document,
                                       self.instances))
            return load_thread

    def check_dataloss_for_high_ops_loader(self, server, bucket, num_items):
        if RestConnection(server).get_nodes_version()[:5] < '5':
            bkt = Bucket('couchbase://{0}/{1}'.format(server.ip, bucket.name))
        else:
            cluster = Cluster("couchbase://{}".format(server.ip))
            auth = PasswordAuthenticator(server.rest_username,
                                         server.rest_password)
            cluster.authenticate(auth)
            bkt = cluster.open_bucket(bucket.name)

        rest = RestConnection(self.master)
        VBucketAware = VBucketAwareMemcached(rest, bucket.name)
        _, _, _ = VBucketAware.request_map(rest, bucket.name)
        batch_start = 0
        batch_end = 0
        batch_size = self.batch_size
        errors = []
        while num_items > batch_end:
            if batch_start + batch_size > num_items:
                batch_end = num_items
            else:
                batch_end = batch_start + batch_size
            keys = []
            for i in xrange(batch_start, batch_end, 1):
                key = "Key_{}".format(i)
                keys.append(key)
            try:
                result = bkt.get_multi(keys)
                self.log.info(
                    "Able to fetch keys starting from {0} to {1}".format(
                        keys[0], keys[len(keys) - 1]))
                for i in range(batch_start, batch_end):
                    key = "Key_{}".format(i)
                    value = {'val': i}
                    if key in result:
                        val = result[key].value
                        for k in value.keys():
                            if k in val and val[k] == value[k]:
                                continue
                            else:
                                vBucketId = VBucketAware._get_vBucket_id(key)
                                errors.append((
                                              "Wrong value for key: {0}, VBucketId: {1}".format(
                                                  key, vBucketId)))
                    else:
                        vBucketId = VBucketAware._get_vBucket_id(key)
                        errors.append((
                                      "Missing key: {0}, VBucketId: {1}".format(
                                          key, vBucketId)))
                self.log.info(
                    "Validated key-values starting from {0} to {1}".format(
                        keys[0], keys[len(keys) - 1]))
            except CouchbaseError as e:
                self.log.error(e)
                ok, fail = e.split_results()
                if fail:
                    for key in fail:
                        try:
                            bkt.get(key)
                        except NotFoundError:
                            vBucketId = VBucketAware._get_vBucket_id(key)
                            errors.append("Missing key: {0}, VBucketId: {1}".
                                          format(key, vBucketId))
            batch_start += batch_size
        return errors

    def check_dataloss(self, server, bucket):
        if RestConnection(server).get_nodes_version()[:5] < '5':
            bkt = Bucket('couchbase://{0}/{1}'.format(server.ip, bucket.name))
        else:
            cluster = Cluster("couchbase://{}".format(server.ip))
            auth = PasswordAuthenticator(server.rest_username,
                                         server.rest_password)
            cluster.authenticate(auth)
            bkt = cluster.open_bucket(bucket.name)

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

    def check_data(self, server, bucket, num_items=0):
        if self.loader == "pillowfight":
            return self.check_dataloss(server, bucket)
        elif self.loader == "high_ops":
            return self.check_dataloss_for_high_ops_loader(server, bucket,
                                                           num_items)
    '''
    def test_rebalance_in(self):
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs(num_items=self.num_items)
        self.log.info('starting the load thread...')
        load_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[self.nodes_init:self.nodes_init + self.nodes_in],
                                                 [])
        rebalance.result()
        load_thread.join()
        num_items_to_validate = self.num_items
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Printing missing keys:")
        for error in errors:
            print error
        if self.num_items != rest.get_active_key_count(bucket):
            self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                          format(self.num_items, rest.get_active_key_count(bucket) ))

    '''

    def test_rebalance_in(self):
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items)
        load_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 [])
        rebalance.result()
        load_thread.join()
        num_items_to_validate = self.num_items * 3
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Printing missing keys:")
        for error in errors:
            print error
        if self.num_items * 2 != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                format(self.num_items * 2, rest.get_active_key_count(bucket)))


    def test_rebalance_out(self):
        servs_out = [self.servers[self.nodes_init - i - 1] for i in
                     range(self.nodes_out)]
        self.log.info("Servers Out: {0}".format(servs_out))
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items)
        load_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],[],servs_out)
        rebalance.result()
        load_thread.join()
        num_items_to_validate = self.num_items * 3
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Printing missing keys:")
        for error in errors:
            print error
        if self.num_items != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                format(self.num_items, rest.get_active_key_count(bucket)))

    def test_rebalance_in_out(self):
        servs_out = [self.servers[self.nodes_init - i - 1] for i in
                     range(self.nodes_out)]
        self.log.info("Servers Out: {0}".format(servs_out))
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items)
        load_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in], servs_out)
        rebalance.result()
        load_thread.join()
        num_items_to_validate = self.num_items * 3
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Printing missing keys:")
        for error in errors:
            print error
        if self.num_items != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(self.num_items, rest.get_active_key_count(bucket)))

    def test_graceful_failover_addback(self):
        node_out = self.servers[self.node_out]
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items)
        load_thread.start()
        nodes_all = rest.node_statuses()
        for node in nodes_all:
            if node.ip == node_out.ip:
                break

        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=60)

        failover_task.result()

        rest.set_recovery_type(node.id, self.recovery_type)
        rest.add_back_node(node.id)

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [])

        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        load_thread.join()
        num_items_to_validate = self.num_items * 3
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Printing missing keys:")
        for error in errors:
            print error
        if self.num_items != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(self.num_items, rest.get_active_key_count(bucket)))

