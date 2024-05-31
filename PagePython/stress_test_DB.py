import unittest
import uuid
import random
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, BatchStatement
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory

from threading import Thread, Lock

from utils.query_utils import *

CLUSTER_IDS = ['172.19.0.2'] # Replace with your nodes
KEYSPACE = 'library_keyspace'

PROFILE = ExecutionProfile(
    retry_policy = DowngradingConsistencyRetryPolicy,
    consistency_level = ConsistencyLevel.TWO,
    serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL,
    request_timeout = 200,
)

def clear_DB(session):
    query_users = "TRUNCATE users;"
    query_books = "TRUNCATE books;"
    query_reservations = "TRUNCATE reservations;"
        
    try:
        session.execute(query_users)
        session.execute(query_books)
        session.execute(query_reservations)

        time.sleep(0.5)

    except Exception as e:
        raise e

class TestCassandra(unittest.TestCase):
    @classmethod
    def setUpClass(cls):        
        cls.cluster = Cluster(CLUSTER_IDS, execution_profiles={EXEC_PROFILE_DEFAULT: PROFILE})
        cls.session = cls.cluster.connect()

        create_keyspace(cls.session)
        cls.session.set_keyspace('library_keyspace')
        print("Created library_keyspace")

        create_reservations_table(cls.session)
        print("Created Reservations Table")

        create_books_table(cls.session)
        print("Created Books table")

        create_users_table(cls.session)
        print("Created users table")
        

    @classmethod
    def tearDownClass(cls):
        delete_reservations_table(cls.session)
        print("\nDeleted reservations table")

        delete_books_table(cls.session)
        print("Deleted books table")

        delete_users_table(cls.session)
        print("Deleted users table")

        delete_keyspace(cls.session)
        print("Deleted keyspace")

        cls.session.shutdown()
        cls.cluster.shutdown()

    def test_reserve_cancel(self):
        # Creates and cancels a reservation 10_000 times
        print("\nExecuting test: Reserve cancel 10 000 times...")
        reservation_id = uuid.uuid4()
        user_id = uuid.uuid4()
        user_name = "User Test"
        book_id = uuid.uuid4()
        book_name = "Book Test"

        add_book(self.session, book_id, book_name, False)

        start_time = time.time()

        repeat_num = 10_000
        for _ in range(repeat_num):
            add_reservation(self.session, reservation_id, user_id, user_name, book_name, book_id)
            cancel_reservation(self.session, reservation_id)

        add_reservation(self.session, reservation_id, user_id, user_name, book_name, book_id)
        
        end_time = time.time()

        # Added for testing:
        time.sleep(0.5)
        reservation = get_reservation_by_id(self.session, reservation_id)

        assert reservation is not None
        assert reservation.reservation_id == reservation_id
        assert reservation.user_id == user_id
        assert reservation.user_name == user_name
        assert reservation.book_id == book_id
        assert reservation.book_name == book_name

        clear_DB(self.session)
        print(f"Execution time: {round(end_time - start_time, 2)}s")


    def test_same_request(self):
        # Runs an update of the book in a reservation 10_000 times
        print("\nExecuting test: Same request (update book in a reservation) 10 000 times...")

        reservation_id = uuid.uuid4()
        user_id = uuid.uuid4()
        user_name = "User Test"
        book_id = uuid.uuid4()
        book_name = "Book Test"

        new_book_id = uuid.uuid4()
        new_book_name = "New Book Test"

        add_book(self.session, book_id, book_name, False)
        add_book(self.session, new_book_id, new_book_name, False)
        
        add_reservation(self.session, reservation_id, user_id, user_name, book_name, book_id)

        start_time = time.time()

        repeat_num = 10_000
        for _ in range(repeat_num):
            update_reservation(self.session, reservation_id, new_book_id)
        
        end_time = time.time()

        # Added for testing
        time.sleep(0.5)
        reservation = get_reservation_by_id(self.session, reservation_id)

        assert reservation is not None
        assert reservation.reservation_id == reservation_id
        assert reservation.user_id == user_id
        assert reservation.user_name == user_name
        assert reservation.book_id == new_book_id
        assert reservation.book_name == new_book_name

        clear_DB(self.session)
        print(f"Execution time: {round(end_time - start_time, 2)}s")

    def test_update_reservations(self):
        # Updates a 1000 reservations
        print("\nExecuting test: Update the book in 1000 reservations...")

        print("Adding books")
        book_ids = []
        book_names = []
        insert_statement_books = self.session.prepare("""
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
                self.session.execute(batch_books, timeout=120)
            except InvalidRequest as e:
                raise e

        user_id = uuid.uuid4()
        user_name = "User Test"

        # Add reservations   
        print("Adding reservations")

        res_count = 1000
        for book_id, book_name in zip(book_ids, book_names):
            if res_count == 0:
                break

            add_reservation(self.session, uuid.uuid4(), user_id, user_name, book_name, book_id)
            res_count -= 1

        time.sleep(0.5)

        reservations = list(get_all_reservations(self.session))
        old_reservation_ids = set([res.reservation_id for res in reservations])
        old_book_ids = set([res.book_id for res in reservations])

        books = list(get_all_books(self.session))
        available_books = [book for book in books if not book.is_reserved]
        new_book_ids = set([book.book_id for book in available_books])

        time.sleep(0.5)

        print("Running test")
        start_time = time.time()

        num_reservations = 1000
        assert len(reservations) == num_reservations, f"There should be {num_reservations} existing reservations"
        assert len(available_books) >= num_reservations, f"There should be at least {num_reservations} available books"

        for i in range(num_reservations):
            reservation = reservations[i]
            new_book = available_books[i]

            update_reservation(self.session, reservation.reservation_id, new_book.book_id)
        
        end_time = time.time()
        time.sleep(0.5)

        new_reservations = list(get_all_reservations(self.session))
        assert len(new_reservations) == 1000

        for res in new_reservations:
            assert res.book_id in new_book_ids
            assert res.book_id not in old_book_ids
            assert res.reservation_id in old_reservation_ids
            assert res.user_id == user_id
            assert res.user_name == user_name

        clear_DB(self.session)
        print(f"Execution time: {round(end_time - start_time, 2)}s")

    def test_make_all_reservations_two_users(self):
        # Simulates two users reserving all possible (2000) books
        print("\nExecuting test: Two users reserve all (2000) books...")

        print("Adding books")
        book_ids = []
        book_names = []
        insert_statement_books = self.session.prepare("""
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
                self.session.execute(batch_books, timeout=120)
            except InvalidRequest as e:
                raise e
            
        books = get_all_books(self.session)
        book_ids = []
        for book in books:
            book_ids.append(book.book_id)
        assert len(book_ids) == 2000
        
        lock = Lock()
        def make_all_reservations(user_id, user_name):
            # Simulates user making all possible reservations one by one

            user_cluster = Cluster(CLUSTER_IDS)
            user_session = user_cluster.connect(KEYSPACE)

            # Update available book state
            books = get_all_books(user_session)
            available_books = [book for book in books if not book.is_reserved]
        
            while len(available_books) > 0:
                selected_random_book = random.choice(available_books)
                
                try:
                    # Consistent
                    with lock:
                        add_reservation(user_session, uuid.uuid4(), user_id, user_name, selected_random_book.book_name, selected_random_book.book_id)
                        time.sleep(random.uniform(0.01, 0.05))
                    
                    # Fast
                    #add_reservation(user_session, uuid.uuid4(), user_id, user_name, selected_random_book.book_name, selected_random_book.book_id, verbose=False)

                except:
                    pass
                
                books = get_all_books(user_session)
                available_books = [book for book in books if not book.is_reserved]

            user_session.shutdown()
            user_cluster.shutdown()

        user1_id = uuid.uuid4()
        user1_name = "User 1 Test"

        user2_id = uuid.uuid4()
        user2_name = "User 2 Test"
        
        time.sleep(0.5)
        print("Running test")
        start_time = time.time()

        thread1 = Thread(target=make_all_reservations, args=[user1_id, user1_name])
        thread2 = Thread(target=make_all_reservations, args=[user2_id, user2_name])

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        end_time = time.time()

        all_reservations = list(get_all_reservations(self.session))
        
        print("Reservations made:", len(all_reservations))
        assert len(all_reservations) == 2000

        user_1_reservations = 0
        user_2_reservations = 0
        for res in all_reservations:
            if res.user_id == user1_id:
                user_1_reservations += 1

            if res.user_id == user2_id:
                user_2_reservations += 1

            assert (res.user_id == user1_id) or (res.user_id == user2_id)
            assert (res.user_name == user1_name) or (res.user_name == user2_name)

        assert len(set([res.book_id for res in all_reservations])) == 2000
        assert len(set([res.book_name for res in all_reservations])) == 2000

        print(f"User 1 reservation count: {user_1_reservations}")
        print(f"User 2 reservation count: {user_2_reservations}")

        assert abs(user_1_reservations - user_2_reservations) < 100
        assert user_1_reservations + user_2_reservations == 2000

        clear_DB(self.session)
        print(f"Execution time: {round(end_time - start_time, 2)}s")


    def test_make_random_requests_two_users_fast(self):
        # Simulates two users making 10_000 total random requests
        print("\nExecuting test: Two users make 10 000 random actions...")

        print("Adding 10 000 books")
        book_ids = []
        book_names = []
        insert_statement_books = self.session.prepare("""
            INSERT INTO books (book_id, book_name, is_reserved) VALUES (?, ?, ?)
        """)
        for i in range(1000):
            batch_books = BatchStatement()
            for j in range(10):
                book_id = uuid.uuid4()
                book_ids.append(book_id)
                book_name = f'Book {i*10+j+1}'
                book_names.append(book_name)
                batch_books.add(insert_statement_books, (book_id, book_name, False))
            try:
                self.session.execute(batch_books, timeout=120)
            except InvalidRequest as e:
                raise e

        user_ids = []
        user_names = []
        for i in range(10):
            for j in range(10):
                user_id = uuid.uuid4()
                user_ids.append(user_id)
                user_name = f'User {i*10+j+1}'
                user_names.append(user_name)

        print("Adding about a 1000 random reservations")
        res_count = 1000
        for book_id, book_name in zip(book_ids, book_names):
            if res_count == 0:
                break

            u_id = random.randint(0, len(user_ids)-1)
            user_id = user_ids[u_id]
            user_name = user_names[u_id]

            b_id = random.randint(0, len(book_ids)-1)
            book_id = book_ids[b_id]
            book_name = book_names[b_id]

            add_reservation(self.session, uuid.uuid4(), user_id, user_name, book_name, book_id, verbose=False)
            res_count -= 1

        def make_random_requests_fast(user_id, user_name, num_actions):
            # Simulates a user making random requests num_actions number of times

            user_cluster = Cluster(CLUSTER_IDS)
            user_session = user_cluster.connect(KEYSPACE)

            possible_requests = ["update_reservation_book", "update_reservation_user", "cancel_reservation", "make_reservation"]

            while num_actions > 0:
                
                if not num_actions%100:
                    reservations = list(get_all_reservations(user_session))
                    books = list(get_all_books(user_session))
                    users = list(get_all_users(user_session))
                    
                num_actions -= 1
                selected_action = random.choice(possible_requests)

                # Update DB state information

                # If no reservations left, make a reservation
                #if len(reservations) == 0:
                #    selected_action = "make_reservation"
                
                if selected_action == "update_reservation_book":
                    reservation = random.choice(reservations)
                    book = random.choice(books)

                    # Does not assume book is available
                    try:
                        update_reservation(user_session, reservation.reservation_id, book.book_id, verbose=False)

                    except:
                        pass

                elif selected_action == "update_reservation_user":
                    reservation = random.choice(reservations)
                    user = random.choice(users)

                    # Does not assume user does not possess reservation already
                    try:
                        update_reservation_user(user_session, reservation.reservation_id, user.user_id, user.user_name, verbose=False)
                    except:
                        pass

                elif selected_action == "cancel_reservation":
                    reservation = random.choice(reservations)
                    try:
                        cancel_reservation(user_session, reservation.reservation_id, verbose=False)

                        # Locally update memory to make less errors
                        reservations.remove(reservation)
                    except:
                        pass

                elif selected_action == "make_reservation":
                    available_books = [book for book in books if not book.is_reserved]

                    # There are books available
                    if len(available_books) > 0:
                        selected_random_book = random.choice(available_books)

                        try:
                            add_reservation(user_session, uuid.uuid4(), user_id, user_name, selected_random_book.book_name, selected_random_book.book_id, verbose=False)
                        except:
                            pass
                        
            user_session.shutdown()
            user_cluster.shutdown()

        u_id = random.randint(1, len(user_ids)-1)
        
        user1_id = user_ids[u_id-1]
        user1_name = user_names[u_id-1]

        user2_id = user_ids[u_id]
        user2_name = user_names[u_id]

        num_repeats = 10_000
        time.sleep(0.5)

        print("Running test")
        start_time = time.time()

        thread1 = Thread(target=make_random_requests_fast, args=[user1_id, user1_name, num_repeats//2])
        thread2 = Thread(target=make_random_requests_fast, args=[user2_id, user2_name, num_repeats//2])

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        end_time = time.time()
        time.sleep(0.5)

        clear_DB(self.session)
        print(f"Execution time: {round(end_time - start_time, 2)}s")



if __name__ == '__main__':
    unittest.main()
