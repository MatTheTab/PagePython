from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy
from cassandra import InvalidRequest, ReadTimeout, ReadFailure
import uuid
import time

#TODO: Add and test everything
cluster = Cluster(['172.21.0.2'])
session = cluster.connect()

cluster.default_retry_policy = RetryPolicy()
cluster.default_reconnection_policy = ExponentialReconnectionPolicy(base_delay=1, max_delay=60, max_attempts=60)

#TODO: Potentially set the below values for W, R, N
def create_keyspace(session, W, R, N, timeout = 120):
    keyspace_query = """
        CREATE KEYSPACE IF NOT EXISTS library_keyspace 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
    """
    session.execute(keyspace_query, timeout = timeout)

def delete_keyspace(session, timeout = 120):
    keyspace_deletion_query = """ 
        DROP KEYSPACE IF EXISTS library_keyspace;
    """
    session.execute(keyspace_deletion_query, timeout = timeout)

def create_reservations_table(session, timeout = 120):
    reservation_table_query = """
        CREATE TABLE IF NOT EXISTS reservations (
            reservation_id UUID,
            user_id UUID,
            user_name TEXT,
            book_name TEXT,
            book_id INT,
            PRIMARY KEY (reservation_id)
        );
    """
    session.execute(reservation_table_query, timeout = timeout)

def create_books_table(session, timeout = 120):
    books_table_query = """
        CREATE TABLE IF NOT EXISTS books (
            book_id UUID,
            book_name TEXT,
            is_reserved BOOLEAN,
            PRIMARY KEY (book_id)
        );
    """
    session.execute(books_table_query, timeout = timeout)

def create_users_table(session, timeout = 120):
    users_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            user_id UUID,
            user_name TEXT,
            reservation_ids_list LIST<TEXT>,
            PRIMARY KEY (user_id)
        );
    """
    session.execute(users_table_query, timeout = timeout)

def delete_reservations_table(session, timeout = 120):
    reservations_table_deletion_query = """ 
        DROP TABLE IF EXISTS reservations;
    """
    session.execute(reservations_table_deletion_query, timeout = timeout)

def delete_books_table(session, timeout = 120):
    books_table_deletion_query = """ 
        DROP TABLE IF EXISTS books;
    """
    session.execute(books_table_deletion_query, timeout = timeout)

def delete_users_table(session, timeout = 120):
    users_table_deletion_query = """ 
        DROP TABLE IF EXISTS users;
    """
    session.execute(users_table_deletion_query, timeout = timeout)

def set_book_reserved(session, book_id, reserved, timeout=120):
    book_set_query = """
        UPDATE books SET is_reserved = %s WHERE book_id = %s;
    """
    try:
        session.execute(book_set_query, [reserved, book_id], timeout=timeout)
        print("Book marked as reserved successfully.")
    except InvalidRequest as e:
        print("Error occurred while updating book reservation status:", e)
        raise e
    
def add_book():
    #TODO: Finish
    pass

def add_user(session, user_id, user_name, reservation_id, timeout = 120):
    #TODO: Finish
    pass

def append_user_book(session, user_id, user_name, reservation_id, timeout = 120):
    try:
        user = get_user(session, user_id, timeout = timeout)
        if user is None:
            add_user(session, user_id, user_name, reservation_id, timeout = timeout)
        else:
            pass
            #TODO: add the book_id to the list of books
    except InvalidRequest as e:
        print("Error Occured while adding a book to the user's look list: ", e)
        raise e

def add_reservation(session, reservation_id, user_id, user_name, book_name, book_id, timeout = 120):
    insert_reservation_query = """
        INSERT INTO reservations (reservation_id, user_id, user_name, book_name, book_id)
        VALUES (%s, %s, %s, %s, %s);
    """
    try:
        set_book_reserved(session, book_id=book_id, reserved="true", timeout = timeout)
        session.execute(insert_reservation_query, [reservation_id, user_id, user_name, book_name, book_id], timeout=timeout)
    except InvalidRequest as e:
        print("Error occurred while inserting reservation:", e)

def update_reservation():
    #TODO: Finish
    pass

def cancel_reservation():
    #TODO: Finish
    pass

def get_all_reservations(session, timeout = 120):
    selection_query = """
        SELECT * FROM reservations;
    """
    result = session.execute(selection_query, timeout = timeout)
    return result

def get_all_users(session, timeout = 120):
    selection_query = """
        SELECT * FROM users;
    """
    result = session.execute(selection_query, timeout = timeout)
    return result

def get_all_books(session, timeout = 120):
    selection_query = """
        SELECT * FROM books;
    """
    result = session.execute(selection_query, timeout = timeout)
    return result

def get_reservation_by_id(session, reservation_id, timeout=120):
    query = """
        SELECT * FROM reservations WHERE reservation_id = %s;
    """
    try:
        prepared = session.prepare(query)
        result = session.execute(prepared, [reservation_id], timeout=timeout)
        reservation = result.one()
        if reservation is None:
            raise ValueError("No reservation found with ID: {}".format(reservation_id))
        return reservation
    except (InvalidRequest, ReadTimeout, ReadFailure, ValueError) as e:
        print("Error occurred while fetching the reservation:", e)
        return None
    
def get_book(session, book_id, timeout=120):
    query = """
        SELECT * FROM books WHERE book_id = %s;
    """
    try:
        prepared = session.prepare(query)
        result = session.execute(prepared, [book_id], timeout=timeout)
        book = result.one()
        if book is None:
            raise ValueError("No book found with ID: {}".format(book_id))
        return book
    except (InvalidRequest, ReadTimeout, ReadFailure, ValueError) as e:
        print("Error occurred while fetching the book:", e)
        return None

def get_user(session, user_id, timeout=120):
    query = """
        SELECT * FROM users WHERE user_id = %s;
    """
    try:
        prepared = session.prepare(query)
        result = session.execute(prepared, [user_id], timeout=timeout)
        user = result.one()
        if user is None:
            raise ValueError("No user found with ID: {}".format(user_id))
        return user
    except (InvalidRequest, ReadTimeout, ReadFailure, ValueError) as e:
        print("Error occurred while fetching the user:", e)
        return None

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
