from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy
import uuid
import time

cluster = Cluster(['172.20.0.2'])
session = cluster.connect()

# Set retry and reconnection policies with longer timeout
cluster.default_retry_policy = RetryPolicy()
cluster.default_reconnection_policy = ExponentialReconnectionPolicy(base_delay=1, max_delay=60, max_attempts=60)

keyspace_query = """
    CREATE KEYSPACE IF NOT EXISTS library_keyspace 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
"""

session.execute(keyspace_query, timeout=120)
session.set_keyspace('library_keyspace')
table_query = """
    CREATE TABLE IF NOT EXISTS reservations (
        reservation_id UUID,
        user_id INT,
        reservation_start TIMESTAMP,
        reservation_end TIMESTAMP,
        book_id UUID,
        book_name TEXT,
        book_genre TEXT,
        PRIMARY KEY (reservation_id, user_id)
    );
"""
session.execute(table_query, timeout=120)
print("Created Table")

reservation_id = uuid.uuid4()
user_id = 1  # Assuming user_id as integer
book_id = uuid.uuid4()
insert_query = """
    INSERT INTO reservations (reservation_id, user_id, reservation_start, reservation_end, book_id, book_name, book_genre)
    VALUES  (%s, %s, toTimestamp(now()), toTimestamp(now()) + 1d, %s, 'Catcher in the Ray', 'Drama'); 
"""
time.sleep(10) #Created because of Eventual Consistency

session.execute(insert_query, [reservation_id, user_id, book_id], timeout=120)
print("Inserted Data")

selection_query = """
    SELECT * FROM reservations;
"""

result = session.execute(selection_query)
for row in result:
    print(row)

table_deletion_query = """ 
    DROP TABLE IF EXISTS reservations;
"""
session.execute(table_deletion_query, timeout=120)
print("Deleted Data")

keyspace_deletion_query = """ 
    DROP KEYSPACE IF EXISTS library_keyspace;
"""
session.execute(keyspace_deletion_query, timeout=120)
print("Deleted Keyspace")

print("Finished")
session.shutdown()
cluster.shutdown()
