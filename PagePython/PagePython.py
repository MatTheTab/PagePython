from cassandra.cluster import Cluster

cluster = Cluster(['172.21.0.2'])
session = cluster.connect()

keyspace_query = """
    CREATE KEYSPACE IF NOT EXISTS library_keyspace 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
"""

session.execute(keyspace_query)
session.set_keyspace('library_keyspace')
table_query = """
    CREATE TABLE IF NOT EXISTS reservations (
        reservation_id UUID,
        user_id UUID,
        reservation_start TIMESTAMP,
        reservation_end TIMESTAMP,
        book_id UUID,
        book_name STRING,
        book_genre STRING,
        PRIMARY KEY (reservation_id, user_id)
    );
"""
session.execute(table_query)

insert_query = """
    INSERT INTO reservations (reservation_id, user_id, reservation_start, reservation_end, book_id, book_name, book_genre)
    VALUES  (uuid(), uuid(), toTimeStamp(now()), toTimeStamp(now()+3600), uuid(), 'Catcher in the Ray', 'Drama'); 
"""
session.execute(insert_query)

selection_query = """
    SELECT * FROM reservations;
"""

result = session.execute(selection_query)
for row in result:
    print(row)

table_deletion_query = """ 
    DROP TABLE IF EXISTS reservations;
"""
session.execute(table_deletion_query)

keyspace_deletion_query = """ 
    DROP KEYSPACE IF EXISTS library_keyspace;
"""
session.execute(keyspace_deletion_query)

print("Finished")
session.shutdown()
cluster.shutdown()
