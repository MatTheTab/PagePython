import uuid
from collections import namedtuple

from cassandra.cluster import Cluster, BatchStatement
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy

from query_utils import *
    
Reservation = namedtuple('Reservation', ['id', 'user_id', 'user_name', 'book_name', 'book_id'])
Book = namedtuple('Book', ['book_id', 'book_name', 'is_reserved'])
User = namedtuple('User', ['user_id', 'user_name', 'reservation_ids_list'])

if __name__ == "__main__":
    cluster = Cluster(['172.19.0.2'])
    session = cluster.connect()
    
    cluster.default_retry_policy = RetryPolicy()
    cluster.default_reconnection_policy = ExponentialReconnectionPolicy(base_delay=1, max_delay=60, max_attempts=60)

    # DEBUG purpose: Set up DB
    create_keyspace(session)
    session.set_keyspace('library_keyspace')
    print("Created library_keyspace")
    create_reservations_table(session)
    print("Created Reservations Table")
    create_books_table(session)
    print("Created Books table")
    create_users_table(session)
    print("Created users table")

    book_ids = []
    insert_statement_books = session.prepare("""
        INSERT INTO books (book_id, book_name, is_reserved) VALUES (?, ?, ?)
    """)
    batch_books = BatchStatement()
    for i in range(10):
        book_id = uuid.uuid4()
        book_ids.append(book_id)
        batch_books.add(insert_statement_books, (book_id, f'Book {i+1}', False))
    try:
        session.execute(batch_books, timeout=120)
    except InvalidRequest as e:
        raise e
    
    user_ids = []
    insert_statement_users = session.prepare("""
        INSERT INTO users (user_id, user_name, reservation_ids_list) VALUES (?, ?, ?)
    """)
    batch_users = BatchStatement()
    for i in range(5):
        user_id = uuid.uuid4()
        user_ids.append(user_id)
        batch_users.add(insert_statement_users, (user_id, f'User {i+1}', []))
    try:
        session.execute(batch_users, timeout=120)
    except InvalidRequest as e:
        raise e

    
    add_reservation(session, uuid.uuid4(), user_ids[0], "User 1", "Book 1", book_ids[0])
    add_reservation(session, uuid.uuid4(), user_ids[0], "User 1", "Book 2", book_ids[1])
    add_reservation(session, uuid.uuid4(), user_ids[0], "User 1", "Book 3", book_ids[2])
    add_reservation(session, uuid.uuid4(), user_ids[1], "User 2", "Book 4", book_ids[3])
    add_reservation(session, uuid.uuid4(), user_ids[1], "User 2", "Book 5", book_ids[4])
    add_reservation(session, uuid.uuid4(), user_ids[2], "User 3", "Book 6", book_ids[5])
    add_reservation(session, uuid.uuid4(), user_ids[3], "User 4", "Book 7", book_ids[6])
    add_reservation(session, uuid.uuid4(), user_ids[3], "User 4", "Book 8", book_ids[7])

    
    print("RESERVATIONS:")
    result = get_all_reservations(session)
    for row in result:
        print(row)

    
    print("BOOKS:")
    result = get_all_books(session)
    for row in result:
        print(row)
    
    print("USERS:")
    result = get_all_users(session)
    for row in result:
        print(row)

    session.shutdown()
    cluster.shutdown()