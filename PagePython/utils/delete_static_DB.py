from cassandra.cluster import Cluster, BatchStatement
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy

from query_utils import *

cluster = Cluster(['172.19.0.2'])
session = cluster.connect()
session.set_keyspace('library_keyspace')

cluster.default_retry_policy = RetryPolicy()
cluster.default_reconnection_policy = ExponentialReconnectionPolicy(base_delay=1, max_delay=60, max_attempts=60)

delete_reservations_table(session)
print("Deleted reservations table")

delete_books_table(session)
print("Deleted books table")

delete_users_table(session)
print("Deleted users table")

delete_keyspace(session)
print("Deleted keyspace")

session.shutdown()
cluster.shutdown()