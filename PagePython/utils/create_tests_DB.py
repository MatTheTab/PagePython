import uuid
import random

from collections import namedtuple

from cassandra.cluster import Cluster, BatchStatement
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy

from query_utils import *

if __name__ == "__main__":
    cluster = Cluster(['172.21.0.2'])
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

    # Add books
    print("Adding books")
    book_ids = []
    book_names = []
    insert_statement_books = session.prepare("""
        INSERT INTO books (book_id, book_name, is_reserved) VALUES (?, ?, ?)
    """)
    for i in range(200):
        batch_books = BatchStatement()
        for j in range(10):
            book_id = uuid.uuid4()
            book_ids.append(book_id)
            book_name = f'Book {i*10+j+1}'
            book_names.append(book_name)
            batch_books.add(insert_statement_books, (book_id, book_name, False))
        try:
            session.execute(batch_books, timeout=120)
        except InvalidRequest as e:
            raise e
        
    book_id = uuid.uuid4()
    book_ids.append(book_id)
    book_name = "Book 2001"
    book_names.append(book_name)
    add_book(session, book_id, book_name, False)
    
    # Add users
    print("Adding users")
    user_ids = []
    user_names = []
    #insert_statement_users = session.prepare("""
    #    INSERT INTO users (user_id, user_name, reservation_ids_list) VALUES (?, ?, ?)
    #""")
    for i in range(10):
        batch_users = BatchStatement()
        for j in range(10):
            user_id = uuid.uuid4()
            user_ids.append(user_id)
            user_name = f'User {i*10+j+1}'
            user_names.append(user_name)
            #batch_users.add(insert_statement_users, (user_id, user_name, []))
        #try:
        #    session.execute(batch_users, timeout=120)
        #except InvalidRequest as e:
        #    raise e

    user_id = uuid.uuid4()
    user_ids.append(user_id)
    user_name = "User 101"
    user_names.append(user_name)
    #add_user(session, user_id, user_name)

    # Add reservations   
    print("Adding reservations")

    ## Use this code for testing updating all (1000) reservations
    res_count = 1000
    for book_id, book_name in zip(book_ids, book_names):
        if res_count == 0:
            break

        id = random.randint(0, len(user_ids)-1)
        user_id = user_ids[id]
        user_name = user_names[id]

        add_reservation(session, uuid.uuid4(), user_id, user_name, book_name, book_id)
        res_count -= 1
    
    # Use this for remaining tests
    add_reservation(session, uuid.uuid4(), user_ids[-1], user_names[-1], book_name, book_id)
    
    
    print("RESERVATIONS:")
    result = get_all_reservations(session)
    # for row in result:
    #     print(row)
    print(len(list(result)))

    # print("BOOKS:")
    # result = get_all_books(session)
    # for row in result:
    #     print(row)
    
    # print("USERS:")
    # result = get_all_users(session)
    # for row in result:
    #     print(row)

    session.shutdown()
    cluster.shutdown()