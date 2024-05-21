from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy

from query_utils import *
    
if __name__ == "__main__":
    cluster = Cluster(['172.19.0.2'])
    session = cluster.connect()

    cluster.default_retry_policy = RetryPolicy()
    cluster.default_reconnection_policy = ExponentialReconnectionPolicy(base_delay=1, max_delay=60, max_attempts=60)

    delete_reservations_table(session)
    print("Deleted Reservations Table")
    delete_books_table(session)
    print("Deleted the Books Table")
    delete_users_table(session)
    print("Deleted the Users Table")
    delete_keyspace(session)
    print("Deleted library_keyspace")

    print("Finished")
    session.shutdown()
    cluster.shutdown()