from couchbase.cluster import Cluster, PasswordAuthenticator
from couchbase.bucket import Bucket, CouchbaseTransientError, KeyExistsError, LOCKMODE_WAIT
import threading
import argparse

def parseArguments():
  parser = argparse.ArgumentParser(description='Throw some docs in a bucket using the couchbase python client')
  parser.add_argument('--threads', default=5, help="Number of threads to run")
  parser.add_argument('--batch_size', default=5000, help="Batch size of eatch inserts")
  parser.add_argument('--node', '-n', default="localhost", help='Cluster Node to connect to')
  parser.add_argument('--bucket', '-b', default="default", help='Bucket to connect to')
  parser.add_argument('--password', '-p',default="", help='User password')
  parser.add_argument('--user', '-u',default="", help='Username')
  parser.add_argument('--prefix', '-k',default="CBPY_", help='Key Prefix')
  parser.add_argument('--timeout', '-t', default=2, type=int, help='Operation Timeout')
  parser.add_argument('--count', '-c', default=1000, type=int, help='Number of documents to insert')
  parser.add_argument('--start_document', default=0, type=int, help="Starting document count to start loading from")
  return parser.parse_args()


args = parseArguments()
host = args.node
bucket_name = args.bucket
user = args.user
password = args.password
if user and password:
    cluster = Cluster("couchbase://{}".format(host))
    auth = PasswordAuthenticator(user, password)
    cluster.authenticate(auth)
    bucket = cluster.open_bucket(bucket_name, lockmode=LOCKMODE_WAIT)
else:
    bucket = Bucket('couchbase://{0}/{1}'.format(host, bucket_name))
bucket.timeout = 5
num_items = int(args.count)
batch_size = int(args.batch_size)
threads = int(args.threads)
start_document = int(args.start_document)
batches = []
retry_batches = []
num_completed = 0
key_exists_error = 0
thread_lock = threading.Lock()

for i in range(start_document, start_document + num_items, batch_size):
    if i + batch_size > start_document + num_items:
        batches.append({"start": i, "end": start_document + num_items})
    else:
        batches.append({"start": i, "end": i + batch_size})

def get_items(start, end):
    items = {}
    for x in range(start, end):
        key = "Key_{}".format(x)
        value = {'val': x}
        items[key] = value
    return items

def get_retry_items():
    items = {}
    for item in retry_batches:
        items[item] = {'val': int(item.split('_')[1])}
    return items

def insert_items(items):
    try:
        bucket.upsert_multi(items)
        return items.__len__()
    except CouchbaseTransientError as e:
        ok, fail = e.split_results()
        num_completed = ok.__len__()
        for key in fail:
            retry_batches.append(key)
        return num_completed

def insert_thread():
    thread_lock.acquire()
    if batches.__len__() > 0:
        batch = batches.pop()
    else:
        thread_lock.release()
        return 0
    thread_lock.release()
    items = get_items(batch['start'], batch['end'])
    completed = insert_items(items)
    thread_lock.acquire()
    global num_completed
    num_completed += completed
    thread_lock.release()
    return completed

while batches:
    threadpool = []
    for i in range(0,threads):
        t = threading.Thread(target=insert_thread)
        t.start()
        threadpool.append(t)
    for t in threadpool:
        t.join()
while retry_batches:
    items = get_retry_items()
    retry_batches = []
    completed = insert_items(items)
    num_completed += completed
print num_completed