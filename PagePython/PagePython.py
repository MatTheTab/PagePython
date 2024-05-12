from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy
from utils.select_utils import *
import uuid
import time

cluster = Cluster(['172.23.0.2'])
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
        book_id INT,
        book_name TEXT,
        book_genre TEXT,
        PRIMARY KEY (reservation_id)
    );
"""
session.execute(table_query, timeout=120)
print("Created Table")

reservation_id = uuid.uuid4()
user_id = 1
book_id = 1
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

################### (SMALL) TESTS ###################
print()
print("Commensing Function Tests")
print()

make_reservation(session = session, user_id = 1, book_id = 2)
make_reservation(session = session, user_id = 3, book_id = 5)
print("Reservations Made")

time.sleep(10)
results = select_all_reservations(session)
print()
print("All Reservations")
for row in results:
    print(row)

print()
print("User Reservations for user_id = 1")
results = select_all_reservations(session = session)
user_reservation_id = None 
for row in results:
    if row.user_id == 1:
        print(row)
        user_reservation_id = row.reservation_id

print()
print("Updating Reservation Time")
update_reservation_end_time(session = session, reservation_id = user_reservation_id, new_end_time = "toTimestamp(now()) + 10d")
time.sleep(10)
results = select_all_reservations(session = session)
for row in results:
    print(row)

print()
print("Cancelling Reservations")
cancel_reservation(session = session, reservation_id = user_reservation_id)
time.sleep(10)
results = select_all_reservations(session = session)
for row in results:
    print(row)

print()
print("Tests Passed")
print()
################### END OF TESTS ###################

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
