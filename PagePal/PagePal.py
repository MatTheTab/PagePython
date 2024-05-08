from cassandra.cluster import Cluster

cluster = Cluster(['172.19.0.2'])
session = cluster.connect()

keyspace_query = """
    CREATE KEYSPACE IF NOT EXISTS library_keyspace 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
"""

session.execute(keyspace_query)
session.set_keyspace('library_keyspace')
table_query = """
    CREATE TABLE IF NOT EXISTS reservations (
        reservation_id UUID PRIMARY KEY,
        user_id UUID,
        reservation_time TIMESTAMP,
        book_id UUID
    );
"""

session.execute(table_query)

session.shutdown()
cluster.shutdown()
