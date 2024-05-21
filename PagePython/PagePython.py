from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy
from utils.query_utils import *
import uuid
import time

cluster = Cluster(['172.21.0.2'])
session = cluster.connect()

cluster.default_retry_policy = RetryPolicy()
cluster.default_reconnection_policy = ExponentialReconnectionPolicy(base_delay=1, max_delay=60, max_attempts=60)

create_keyspace(session, 0, 0, 0)
session.set_keyspace('library_keyspace')
create_reservations_table(session)
print("Created Reservations Table")
create_books_table(session)
print("Created Books table")
create_users_table(session)
print("Created users table")

reservation_id = uuid.uuid4()
user_id = uuid.uuid4()
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

delete_reservations_table()
print("Deleted Reservations Table")
delete_books_table()
print("Deleted the Books Table")
delete_users_table()
print("Deleted the Users Table")
delete_keyspace()
print("Deleted the Keyspace")

print("Finished")
session.shutdown()
cluster.shutdown()
