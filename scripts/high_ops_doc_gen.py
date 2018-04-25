from copy import deepcopy
from couchbase.cluster import Cluster, PasswordAuthenticator
from couchbase.bucket import Bucket, CouchbaseTransientError, KeyExistsError, LOCKMODE_WAIT, CouchbaseError
import threading
import argparse

def parseArguments():
    parser = argparse.ArgumentParser(description='Update some docs in a bucket using the couchbase python client')
    parser.add_argument('--threads', default=5, help="Number of threads to run")
    parser.add_argument('--batch_size', default=5000, help="Batch size of eatch inserts")
    parser.add_argument('--node', '-n', default="localhost", help='Cluster Node to connect to')
    parser.add_argument('--bucket', '-b', default="default", help='Bucket to connect to')
    parser.add_argument('--password', '-p',default="password", help='User password')
    parser.add_argument('--user', '-u',default="Administrator", help='Username')
    parser.add_argument('--prefix', '-k',default="CBPY_", help='Key Prefix')
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
    return parser.parse_args()

class Document:
    def __init__(self, value, size):
        body_size = size - str(value).__len__() - "val".__len__() - "update".__len__() - "body".__len__()
        body = "".rjust(body_size, 'a')
        self.val = int(value)
        self.update = 0
        self.body = body

class Simple_Doc_gen():

    def __init__(self, args):
        self.host = args.node
        self.bucket_name = args.bucket
        self.user = args.user
        self.password = args.password
        self.cb_version = args.cb_version
        if self.cb_version > '5':
            cluster = Cluster("couchbase://{}".format(self.host))
            auth = PasswordAuthenticator(self.user, self.password)
            cluster.authenticate(auth)
            self.bucket = cluster.open_bucket(self.bucket_name, lockmode=LOCKMODE_WAIT)
        else:
            self.bucket = Bucket('couchbase://{0}/{1}'.format(self.host, self.bucket_name), lockmode=LOCKMODE_WAIT)
        self.bucket.timeout = args.timeout
        self.num_items = int(args.count)
        self.num_of_ops = int(args.ops)
        self.batch_size = int(args.batch_size)
        self.threads = int(args.threads)
        self.start_document = int(args.start_document)
        self.current_update_counter = int(args.update_counter)
        self.updates = args.updates
        self.validate = args.validate
        self.updated = args.updated
        self.size = int(args.size)
        self.batches = []
        self.all_batches = []
        self.retry_batches = []
        self.num_completed = 0
        self.key_exists_error = 0
        self.thread_lock = threading.Lock()
        self.ops_to_items_ratio = int(self.num_of_ops) / int(self.num_items)
        self.wrong_keys = []
        self.missing_key_val = []

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

    def get_keys(self, start, end):
        keys = []
        for i in range(start, end):
            keys.append('Key_{}'.format(i))
        return keys

    def get_items(self, keys):
        try:
            result = self.bucket.get_multi(keys)
            return result
        except CouchbaseError as e:
            ok, fail = e.split_results()
            return ok

    def insert_items(self, items):
        try:
            result = self.bucket.upsert_multi(items)
            return result.__len__()
        except CouchbaseTransientError as e:
            ok, fail = e.split_results()
            num_completed = ok.__len__()
            for key in fail:
                self.retry_batches.append(key)
            return num_completed

    def insert_thread(self, update=False):
        self.thread_lock.acquire()
        if self.batches.__len__() > 0:
            batch = self.batches.pop()
        else:
            self.thread_lock.release()
            return 0
        self.thread_lock.release()
        if update:
            items = self.get_update_items(batch['start'], batch['end'])
        else:
            items = self.get_create_items(batch['start'], batch['end'])
        completed = self.insert_items(items)
        self.thread_lock.acquire()
        self.num_completed += completed
        self.thread_lock.release()
        return completed

    def validate_items(self, start, end):
        keys = self.get_keys(start, end)
        result = self.get_items(keys)
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
                val = result[key].value
                for k in value.keys():
                    if k in val and val[k] == value[k]:
                        continue
                    else:
                        self.wrong_keys.append(key)
            else:
                self.missing_key_val.append(key)

    def validate_thread(self):
        self.thread_lock.acquire()
        if self.batches.__len__() > 0:
            batch = self.batches.pop()
        else:
            self.thread_lock.release()
            return
        self.thread_lock.release()
        self.validate_items(batch['start'], batch['end'])

    def run_create_load_gen(self):
        self.create_create_batches()
        while self.batches:
            threadpool = []
            for i in range(0, self.threads):
                t = threading.Thread(target=self.insert_thread)
                t.start()
                threadpool.append(t)
            for t in threadpool:
                t.join()
        while self.retry_batches:
            items = self.get_create_retry_items()
            self.retry_batches = []
            completed = self.insert_items(items)
            self.num_completed += completed

    def run_update_load_gen(self):
        self.create_update_batches()
        self.all_batches.reverse()
        while self.all_batches:
            self.batches = self.all_batches.pop()
            while self.batches:
                threadpool = []
                for i in range(0, self.threads):
                    t = threading.Thread(target=self.insert_thread, args=(True,))
                    t.start()
                    threadpool.append(t)
                for t in threadpool:
                    t.join()
            self.current_update_counter += 1
        while self.retry_batches:
            items = self.get_update_retry_items()
            self.retry_batches = []
            completed = self.insert_items(items)
            self.num_completed += completed

    def run_validate_load_gen(self):
        self.create_create_batches()
        if self.updated:
            self.current_update_counter = self.ops_to_items_ratio
            if self.num_of_ops - (self.ops_to_items_ratio * self.num_items) > 0:
                self.current_update_counter += 1
        else:
            self.current_update_counter = 0
        while self.batches:
            threadpool = []
            for i in range(0, self.threads):
                t = threading.Thread(target=self.validate_thread)
                t.start()
                threadpool.append(t)
            for t in threadpool:
                t.join()

    def generate(self):
        if self.updates:
            self.run_update_load_gen()
            print "Updated documents: {}".format(self.num_completed)
        elif self.validate:
            self.run_validate_load_gen()
            print "Missing keys count: {}".format(self.missing_key_val.__len__())
            print "Mismatch keys count: {}".format(self.wrong_keys.__len__())
            print "Missing keys: {}".format(self.missing_key_val.__str__())
            print "Mismatch keys: {}".format(self.wrong_keys.__str__())
        else:
            self.run_create_load_gen()
            print "Created documents: {}".format(self.num_completed)

if __name__ == "__main__":
    args = parseArguments()
    doc_loader = Simple_Doc_gen(args)
    doc_loader.generate()