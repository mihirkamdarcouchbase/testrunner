from copy import deepcopy
from couchbase.cluster import Cluster, PasswordAuthenticator
from couchbase.bucket import Bucket, LOCKMODE_WAIT, CouchbaseError, \
    ArgumentError, NotFoundError, TimeoutError
import argparse
import multiprocessing
from multiprocessing.dummy import Pool


def parseArguments():
    parser = argparse.ArgumentParser(description='Update some docs in a bucket using the couchbase python client')
    parser.add_argument('--threads', default=5, help="Number of threads to run")
    parser.add_argument('--batch_size', default=5000, help="Batch size of eatch inserts")
    parser.add_argument('--node', '-n', default="localhost", help='Cluster Node to connect to')
    parser.add_argument('--bucket', '-b', default="default", help='Bucket to connect to')
    parser.add_argument('--password', '-p', default="password", help='User password')
    parser.add_argument('--user', '-u', default="Administrator", help='Username')
    parser.add_argument('--prefix', '-k', default="CBPY_", help='Key Prefix')
    parser.add_argument('--timeout', '-t', default=5, type=int, help='Operation Timeout')
    parser.add_argument('--count', '-c', default=1000, type=int, help='Number of documents in the bucket already')
    parser.add_argument('--ops', default=1000, type=int, help='Number of mutations to perform')
    parser.add_argument('--start_document', default=0, type=int, help="Starting document count to start updating from")
    parser.add_argument('--update_counter', default=1, type=int, help="Starting update counter to start updating from")
    parser.add_argument('--cb_version', default='5.0', help="Current version of the couchbase cluster")
    parser.add_argument('--size', default=100, type=int, help="Size of the document to be inserted, in bytes")
    parser.add_argument('--updates', default=False, action='store_true', help="Run updates instead of creates")
    parser.add_argument('--validate', default=False, action='store_true', help="Validate the documents")
    parser.add_argument('--updated', default=False, action='store_true', help="Was updated performed on documents "
                                                                              "before validation")
    parser.add_argument('--replicate_to', default=0, type=int, help="Perform durability checking on this many "
                                                                    "replicas for presence in memory")
    parser.add_argument('--instances', default=1, type=int, help="Create multiple instances of the generator for "
                                                                 "more ops per sec")
    parser.add_argument('--ttl', default=0, type=int, help="Set expiry timer for documents")
    parser.add_argument('--delete', default=False, action='store_true', help="Delete documents from bucket")
    parser.add_argument('--deleted', default=False, action='store_true', help="Was delete of documents run before "
                                                                              "validation")
    parser.add_argument('--deleted_items', default=0, type=int, help="Number of documents that were deleted")
    return parser.parse_args()


class Document:
    def __init__(self, value, size):
        body_size = size - str(value).__len__() - "val".__len__() - "update".__len__() - "body".__len__()
        body = "".rjust(body_size, 'a')
        self.val = int(value)
        self.update = 0
        self.body = body


class SimpleDocGen:
    def __init__(self, args):
        self.host = args.node
        self.bucket_name = args.bucket
        self.user = args.user
        self.password = args.password
        self.cb_version = args.cb_version
        self.num_items = int(args.count)
        self.num_of_ops = int(args.ops)
        self.batch_size = int(args.batch_size)
        self.threads = int(args.threads)
        self.start_document = int(args.start_document)
        self.current_update_counter = int(args.update_counter)
        self.updates = args.updates
        self.validate = args.validate
        self.updated = args.updated
        self.replicate_to = int(args.replicate_to)
        self.size = int(args.size)
        self.timeout = int(args.timeout)
        self.ttl = int(args.ttl)
        self.delete = args.delete
        self.deleted = args.deleted
        self.deleted_items = int(args.deleted_items)
        self.connections = []
        self.batches = []
        self.all_batches = []
        self.retry_batches = []
        self.num_completed = 0
        self.key_exists_error = 0
        self.ops_to_items_ratio = int(self.num_of_ops) / int(self.num_items)
        self.wrong_keys = []
        self.missing_key_val = []
        self.wrong_keys_replica = []
        self.missing_key_val_replica = []
        self.create_connections()

    def create_connections(self):
        for i in range(0, self.threads):
            if self.cb_version > '5':
                cluster = Cluster("couchbase://{}".format(self.host))
                auth = PasswordAuthenticator(self.user, self.password)
                cluster.authenticate(auth)
                bucket = cluster.open_bucket(self.bucket_name, lockmode=LOCKMODE_WAIT, unlock_gil=True)
                bucket.timeout = self.timeout
                self.connections.append(bucket)
            else:
                bucket = Bucket('couchbase://{0}/{1}'.format(self.host, self.bucket_name), lockmode=LOCKMODE_WAIT,
                                unlock_gil=True)
                bucket.timeout = self.timeout
                self.connections.append(bucket)

    def create_create_batches(self):
        for i in range(self.start_document, self.start_document + self.num_items, self.batch_size):
            if i + self.batch_size > self.start_document + self.num_items:
                self.batches.append({"start": i, "end": self.start_document + self.num_items})
            else:
                self.batches.append({"start": i, "end": i + self.batch_size})

    def create_update_batches(self):
        if self.num_items > self.num_of_ops:
            for i in range(self.start_document, self.start_document + self.num_of_ops, self.batch_size):
                if i + self.batch_size > self.start_document + self.num_of_ops:
                    self.batches.append({"start": i, "end": self.start_document + self.num_of_ops})
                else:
                    self.batches.append({"start": i, "end": i + self.batch_size})
                    self.all_batches.append(self.batches)
        else:
            for i in range(self.start_document, self.start_document + self.num_items, self.batch_size):
                if i + self.batch_size > self.start_document + self.num_items:
                    self.batches.append({"start": i, "end": self.start_document + self.num_items})
                else:
                    self.batches.append({"start": i, "end": i + self.batch_size})
            for i in range(0, self.ops_to_items_ratio):
                self.all_batches.append(deepcopy(self.batches))
            if self.num_of_ops > self.ops_to_items_ratio * self.num_items:
                remaining_ops = self.num_of_ops - (self.ops_to_items_ratio * self.num_items)
                last_batch = []
                for i in range(self.start_document, self.start_document + remaining_ops, self.batch_size):
                    if i + self.batch_size > self.start_document + remaining_ops:
                        last_batch.append({"start": i, "end": self.start_document + remaining_ops})
                    else:
                        last_batch.append({"start": i, "end": i + self.batch_size})
                self.all_batches.append(last_batch)

    def create_delete_batches(self):
        if self.num_items < self.num_of_ops:
            self.num_of_ops = self.num_items
        for i in range(self.start_document, self.start_document + self.num_of_ops, self.batch_size):
            if i + self.batch_size > self.start_document + self.num_of_ops:
                self.batches.append({"start": i, "end": self.start_document + self.num_of_ops})
            else:
                self.batches.append({"start": i, "end": i + self.batch_size})
                self.all_batches.append(self.batches)

    def get_create_items(self, start, end):
        items = {}
        for x in range(start, end):
            key = "Key_{}".format(x)
            items[key] = Document(x, self.size).__dict__
        return items

    def get_update_items(self, start, end):
        items = {}
        for x in range(start, end):
            key = "Key_{}".format(x)
            document = Document(x, self.size)
            document.update = self.current_update_counter
            items[key] = document.__dict__
        return items

    def get_create_retry_items(self):
        items = {}
        for item in self.retry_batches:
            items[item] = Document(int(item.split('_')[1]), self.size).__dict__
        return items

    def get_update_retry_items(self):
        items = {}
        for item in self.retry_batches:
            doc_num = int(item.split('_')[1])
            document = Document(doc_num, self.size)
            if self.num_of_ops - (self.ops_to_items_ratio * self.num_items) > 0:
                if doc_num < self.num_of_ops - (self.ops_to_items_ratio * self.num_items):
                    document.update = self.current_update_counter
                else:
                    document.update = self.current_update_counter - 1
            else:
                document.update = self.current_update_counter
            items[item] = document.__dict__
        return items

    def get_delete_retry_keys(self):
        items = self.retry_batches
        return items

    def get_keys(self, start, end):
        keys = []
        for i in range(start, end):
            keys.append('Key_{}'.format(i))
        return keys

    def get_items(self, connection, keys, replica=False):
        try:
            result = connection.get_multi(keys, replica=replica)
            return result
        except TimeoutError as e:
            ok, fail = e.split_results()
            failed_keys = [key for key in fail]
            result = self.get_items(connection, failed_keys, replica)
            result.update(ok)
            return result
        except CouchbaseError as e:
            ok, fail = e.split_results()
            return ok

    def insert_items(self, connection, items):
        try:
            result = connection.upsert_multi(items, ttl=self.ttl, replicate_to=self.replicate_to)
            return result.__len__()
        except ArgumentError:
            self.replicate_to = 0
            return self.insert_items(connection, items)
        except CouchbaseError as e:
            ok, fail = e.split_results()
            num_completed = ok.__len__()
            for key in fail:
                self.retry_batches.append(key)
            self.key_exists_error += 1
            return num_completed

    def delete_items(self, connection, keys):
        try:
            result = connection.remove_multi(keys)
            return result.__len__()
        except NotFoundError as e:
            pass
        except CouchbaseError as e:
            ok, fail = e.split_results()
            for key in fail:
                self.retry_batches.append(key)
            self.key_exists_error += 1
            return ok.__len__()

    def insert_thread(self, connection, batch, update=False):
        if update:
            items = self.get_update_items(batch['start'], batch['end'])
        else:
            items = self.get_create_items(batch['start'], batch['end'])
        completed = self.insert_items(connection, items)
        return completed

    def insert_thread_pool(self, args):
        return self.insert_thread(*args)

    def validate_items(self, connection, start, end, replica=False):
        keys = self.get_keys(start, end)
        result = self.get_items(connection, keys, replica=replica)
        if self.ttl > 0:
            if result:
                for key in result.keys():
                    if replica:
                        self.missing_key_val_replica.append(key)
                        return
                    else:
                        self.missing_key_val.append(key)
                        return
            else:
                return
        for i in range(start, end):
            key = "Key_{}".format(i)
            document = Document(i, self.size)
            if self.updated and self.num_of_ops - (self.ops_to_items_ratio * self.num_items) > 0:
                if i < self.num_of_ops - (self.ops_to_items_ratio * self.num_items):
                    document.update = self.current_update_counter
                else:
                    document.update = self.current_update_counter - 1
            else:
                document.update = self.current_update_counter
            value = document.__dict__
            if key in result:
                if self.deleted and self.deleted_items > int(key.split("_")[1]):
                    if replica:
                        self.missing_key_val_replica.append(key)
                    else:
                        self.missing_key_val.append(key)
                    continue
                val = result[key].value
                for k in value.keys():
                    if k in val and val[k] == value[k]:
                        continue
                    else:
                        if replica:
                            self.wrong_keys_replica.append(key)
                        else:
                            self.wrong_keys.append(key)
            else:
                if self.deleted and self.deleted_items > int(key.split("_")[1]):
                    continue
                if replica:
                    self.missing_key_val_replica.append(key)
                else:
                    self.missing_key_val.append(key)

    def validate_thread(self, connection, batch):
        self.validate_items(connection, batch['start'], batch['end'])
        if self.replicate_to:
            self.validate_items(connection, batch['start'], batch['end'], replica=True)

    def validate_thread_pool(self, args):
        return self.validate_thread(*args)

    def delete_thread(self, connection, batch):
        keys = self.get_keys(batch['start'], batch['end'])
        completed = self.delete_items(connection, keys)
        return completed

    def delete_thread_pool(self, args):
        return self.delete_thread(*args)

    def run_create_load_gen(self):
        self.create_create_batches()
        args = []
        num_of_connections_available = self.connections.__len__()
        for i in range(0, self.batches.__len__()):
            connection = self.connections[i % num_of_connections_available]
            args.append((connection, self.batches[i], False))
        thread_pool = Pool(self.threads)
        result = thread_pool.map(self.insert_thread_pool, args)
        thread_pool.close()
        thread_pool.join()
        for res in result:
            self.num_completed += res
        while self.retry_batches:
            items = self.get_create_retry_items()
            self.retry_batches = []
            completed = self.insert_items(self.connections[0], items)
            self.num_completed += completed

    def run_update_load_gen(self):
        self.create_update_batches()
        self.all_batches.reverse()
        while self.all_batches:
            self.batches = self.all_batches.pop()
            args = []
            num_of_connections_available = self.connections.__len__()
            for i in range(0, self.batches.__len__()):
                connection = self.connections[i % num_of_connections_available]
                args.append((connection, self.batches[i], True))
            thread_pool = Pool(self.threads)
            result = thread_pool.map(self.insert_thread_pool, args)
            thread_pool.close()
            thread_pool.join()
            for res in result:
                self.num_completed += res
            self.current_update_counter += 1
        self.current_update_counter -= 1
        while self.retry_batches:
            items = self.get_update_retry_items()
            self.retry_batches = []
            completed = self.insert_items(self.connections[0], items)
            self.num_completed += completed

    def run_validate_load_gen(self):
        self.create_create_batches()
        if self.updated:
            self.current_update_counter = self.ops_to_items_ratio
            if self.num_of_ops - (self.ops_to_items_ratio * self.num_items) > 0:
                self.current_update_counter += 1
        else:
            self.current_update_counter = 0
        args = []
        num_of_connections_available = self.connections.__len__()
        for i in range(0, self.batches.__len__()):
            connection = self.connections[i % num_of_connections_available]
            args.append((connection, self.batches[i]))
        thread_pool = Pool(self.threads)
        result = thread_pool.map(self.validate_thread_pool, args)
        thread_pool.close()
        thread_pool.join()

    def run_delete_load_gen(self):
        self.create_delete_batches()
        args = []
        num_of_connections_available = self.connections.__len__()
        for i in range(0, self.batches.__len__()):
            connection = self.connections[i % num_of_connections_available]
            args.append((connection, self.batches[i]))
        thread_pool = Pool(self.threads)
        result = thread_pool.map(self.delete_thread_pool, args)
        thread_pool.close()
        thread_pool.join()
        for res in result:
            self.num_completed += res
        while self.retry_batches:
            keys = self.get_delete_retry_keys()
            self.retry_batches = []
            completed = self.delete_items(self.connections[0], keys)
            self.num_completed += completed

    def generate(self):
        if self.updates:
            self.run_update_load_gen()
            print "Updated documents: {}".format(self.num_completed)
        elif self.validate:
            self.run_validate_load_gen()
            if self.missing_key_val:
                print "Missing keys count: {}".format(self.missing_key_val.__len__())
                print "Missing keys: {}".format(self.missing_key_val.__str__())
            if self.wrong_keys:
                print "Mismatch keys count: {}".format(self.wrong_keys.__len__())
                print "Mismatch keys: {}".format(self.wrong_keys.__str__())
            if self.replicate_to > 0 and self.missing_key_val_replica:
                print "Missing keys count from replicas: {}".format(self.missing_key_val_replica.__len__())
                print "Missing keys from replicas: {}".format(self.missing_key_val_replica.__str__())
            if self.replicate_to > 0 and self.wrong_keys_replica:
                print "Mismatch keys count from replicas: {}".format(self.wrong_keys_replica.__len__())
                print "Mismatch keys from replicas: {}".format(self.wrong_keys_replica.__str__())
            if not self.missing_key_val and not self.wrong_keys:
                print "Validated documents: {}".format(self.num_items)
        elif self.delete:
            self.run_delete_load_gen()
            print "Deleted documents: {}".format(self.num_completed)
        else:
            self.run_create_load_gen()
            print "Created documents: {}".format(self.num_completed)

if __name__ == "__main__":
    args = parseArguments()
    if int(args.instances) == 1 or bool(args.validate):
        doc_loader = SimpleDocGen(args)
        doc_loader.generate()
    else:
        workers = []
        arguments = []
        instances = int(args.instances)
        if bool(args.updates):
            if int(args.count) > int(args.ops):
                items = int(args.count) - int(args.ops)
                for i in range(0, instances):
                    arg = deepcopy(args)
                    count = int(items) / int(instances)
                    start = count * i + int(arg.start_document)
                    if i == instances - 1:
                        count = items - (count * i)
                    arg.count = count
                    arg.ops = count
                    arg.start_document = start
                    arguments.append(arg)
            else:
                ops_to_items_ratio = int(args.ops) / int(args.count)
                for i in range(0, instances):
                    arg = deepcopy(args)
                    items = int(args.count) / instances
                    start = items * i + int(arg.start_document)
                    ops = items * ops_to_items_ratio
                    if i == instances - 1:
                        items = int(args.count) - (items * i)
                        ops = items * ops_to_items_ratio
                    arg.count = items
                    arg.ops = ops
                    arg.start_document = start
                    arguments.append(arg)
                if int(args.ops) > int(args.count) * ops_to_items_ratio:
                    remaining_ops = int(args.ops) - (int(args.count) * ops_to_items_ratio)
                    ops_per_instance = int(args.count) / instances
                    num_of_instance_to_change = remaining_ops / ops_per_instance
                    for i in range(0, num_of_instance_to_change):
                        arg = arguments[i]
                        arg.ops += ops_per_instance
                    if remaining_ops > ops_per_instance * num_of_instance_to_change:
                        arg = arguments[num_of_instance_to_change]
                        arg.ops += (remaining_ops - (ops_per_instance * num_of_instance_to_change))
        elif bool(args.delete):
            if int(args.count) < int(args.ops):
                args.ops = int(args.count)
            for i in range(0, instances):
                arg = deepcopy(args)
                items = int(arg.ops) / instances
                start = items * i + int(arg.start_document)
                if i == instances - 1:
                    items = int(arg.ops) - (items * i)
                arg.ops = items
                arg.start_document = start
                arguments.append(arg)
        else:
            for i in range(0, instances):
                arg = deepcopy(args)
                items = int(arg.count) / instances
                start = items * i + int(arg.start_document)
                if i == instances - 1:
                    items = int(arg.count) - (items * i)
                arg.count = items
                arg.start_document = start
                arguments.append(arg)
        for i in range(0, instances):
            arg = arguments[i]
            doc_loader = SimpleDocGen(arg)
            worker = multiprocessing.Process(target=doc_loader.generate, name="Worker {}".format(i))
            workers.append(worker)
            worker.start()
        for worker in workers:
            worker.join()
