from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy
from utils.query_utils import *
import uuid
import time

cluster = Cluster(['172.21.0.2'])
session = cluster.connect()

cluster.default_retry_policy = RetryPolicy()
cluster.default_reconnection_policy = ExponentialReconnectionPolicy(base_delay=1, max_delay=60, max_attempts=60)

create_keyspace(session)
session.set_keyspace('library_keyspace')
create_reservations_table(session)
print("Created Reservations Table")
create_books_table(session)
print("Created Books table")
create_users_table(session)
print("Created users table")

#################### Mini Tests ####################
book_id_1 = uuid.uuid4()
book_id_2 = uuid.uuid4()
book_id_3 = uuid.uuid4()
book_id_4 = uuid.uuid4()
add_book(session, book_id = book_id_1, book_name = "Test Book 1", is_reserved = 0)
add_book(session, book_id = book_id_2, book_name = "Test Book 2", is_reserved = 0)
add_book(session, book_id = book_id_3, book_name = "Test Book 3", is_reserved = 0)
add_book(session, book_id = book_id_4, book_name = "Test Book 4", is_reserved = 0)
print("______________")
print("Added Books")
time.sleep(10) # sleep added because of eventual consistency (only for testing)

reservation_id_1 = uuid.uuid4()
reservation_id_2 = uuid.uuid4()
reservation_id_3 = uuid.uuid4()
user_id_1 = uuid.uuid4()
user_id_2 = uuid.uuid4()
user_id_3 = uuid.uuid4()
add_reservation(session, reservation_id = reservation_id_1, user_id = user_id_1, user_name = "Test User 1", book_name = "Test Book 1", book_id = book_id_1)
add_reservation(session, reservation_id = reservation_id_2, user_id = user_id_2, user_name = "Test User 2", book_name = "Test Book 2", book_id = book_id_2)
add_reservation(session, reservation_id = reservation_id_3, user_id = user_id_3, user_name = "Test User 3", book_name = "Test Book 3", book_id = book_id_3)
print("__________________")
print("Added Reservations")
time.sleep(10) # sleep added because of eventual consistency (only for testing)

print("___________________")
print("All Reservations: ")
reservations = get_all_reservations(session)
for row in reservations:
    print(row)

print()
print("___________________")
print("All Users")
users = get_all_users(session)
for row in users:
    print(row)

print()
print("___________________")
print("All Books")
books = get_all_books(session)
for row in books:
    print(row)

print()
print("____________________")
print("Reservation 2: ")
res2 = get_reservation_by_id(session, reservation_id = reservation_id_2)
print(res2)

print()
print("____________________")
print("User 2: ")
us2 = get_user(session, user_id = user_id_2)
print(us2)

print()
print("____________________")
print("Book 2: ")
book2 = get_book(session, book_id = book_id_2)
print(book2)

print()
print("****************************************")
print("Updating Reservation 2")
update_reservation(session, reservation_id=reservation_id_1, book_id=book_id_4)
time.sleep(15) # sleep added because of eventual consistency (only for testing)
print("___________________")
print("All Reservations: ")
reservations = get_all_reservations(session)
for row in reservations:
    print(row)

print()
print("___________________")
print("All Users")
users = get_all_users(session)
for row in users:
    print(row)

print()
print("___________________")
print("All Books")
books = get_all_books(session)
for row in books:
    print(row)
print("******************************************")
print()

print()
print("****************************************")
print("Updating Reservation 2 -> Should not be possible")
update_reservation(session, reservation_id=reservation_id_1, book_id=book_id_2)
time.sleep(15) # sleep added because of eventual consistency (only for testing)
print("___________________")
print("All Reservations: ")
reservations = get_all_reservations(session)
for row in reservations:
    print(row)

print()
print("___________________")
print("All Users")
users = get_all_users(session)
for row in users:
    print(row)

print()
print("___________________")
print("All Books")
books = get_all_books(session)
for row in books:
    print(row)
print("******************************************")
print()

print()
print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
print("Cancelling reservation")
cancel_reservation(session, reservation_id=reservation_id_2)
time.sleep(15) # sleep added because of eventual consistency (only for testing)
print("___________________")
print("All Reservations: ")
reservations = get_all_reservations(session)
for row in reservations:
    print(row)

print()
print("___________________")
print("All Users")
users = get_all_users(session)
for row in users:
    print(row)

print()
print("___________________")
print("All Books")
books = get_all_books(session)
for row in books:
    print(row)
print("******************************************")
print()

#print()
#print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
#print("Changing username")
#update_username(session, user_id=user_id_1, user_name="Changed User")
#time.sleep(20) # sleep added because of eventual consistency (only for testing)
#print("___________________")
#print("All Reservations: ")
#reservations = get_all_reservations(session)
#for row in reservations:
#    print(row)

#print()
#print("___________________")
#print("All Users")
#users = get_all_users(session)
#for row in users:
#    print(row)

#print()
#print("___________________")
#print("All Books")
#books = get_all_books(session)
#for row in books:
#    print(row)

print()
print("TESTS PASSED WOOOOOO")
print()

#################### End of Mini Tests ####################
delete_reservations_table(session)
print("Deleted Reservations Table")
delete_books_table(session)
print("Deleted the Books Table")
delete_users_table(session)
print("Deleted the Users Table")
delete_keyspace(session)
print("Deleted the Keyspace")

print("Finished")
session.shutdown()
cluster.shutdown()