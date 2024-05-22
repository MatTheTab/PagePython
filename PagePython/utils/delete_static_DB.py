from cassandra.cluster import Cluster, BatchStatement
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy

from query_utils import *

cluster = Cluster(['172.19.0.2'])
session = cluster.connect()

cluster.default_retry_policy = RetryPolicy()
cluster.default_reconnection_policy = ExponentialReconnectionPolicy(base_delay=1, max_delay=60, max_attempts=60)

delete_reservations_table(session)
delete_books_table(session)
delete_users_table(session)
delete_keyspace(session)

session.shutdown()
cluster.shutdown()