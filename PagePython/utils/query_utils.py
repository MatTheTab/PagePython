from cassandra import InvalidRequest, ReadTimeout, ReadFailure
import uuid

# Function Descriptions:
# - create_keyspace(session, W, R, N, timeout = 120) -> creates keyspace called "library_keyspace", W,R,N to be implemented
# - delete_keyspace(session, timeout = 120) -> deletes the "library_keyspace" keyspace
# - create_reservations_table(session, timeout = 120) -> creates the reservations table (structure available in notes.txt)
# - create_books_table(session, timeout = 120) -> creates the books table (structure available in notes.txt)
# - create_users_table(session, timeout = 120) -> creates the users table (structure available in notes.txt)
# - delete_reservations_table(session, timeout = 120) -> deletes the reservations table
# - delete_books_table(session, timeout = 120) -> deletes the books table
# - delete_users_table(session, timeout = 120) -> deletes the users table
# - set_book_reserved(session, book_id, reserved, timeout=120) -> set the book's reserved status, reserved can be 1 or 0
# - add_book(session, book_id, book_name, is_reserved, timeout = 120) -> insert query into books with specified values
# - add_user(session, user_id, user_name, reservation_ids, timeout = 120) -> insert query into users with specified values
# - add_reservation_to_list(session, user_id, reservation_id, timeout = 120) -> add a reservation to the list of user reservations
# - append_user_reservation(session, user_id, user_name, reservation_id, timeout = 120) -> (use this to add a reservation, if user may not yet exist) add a reservation to the list of user reservations, but if no user is found, a new list of reservations is created
# - add_reservation(session, reservation_id, user_id, user_name, book_name, book_id, timeout = 120) -> handles everything related to adding a new reservation (changes the reservations, users and books tables)
# - update_reservation(session, reservation_id, book_id, timeout = 120) -> handles everything related to updating which book was selected in a reservation (allows to change the book_id for a reservation), chnages the reservations, users and books tables
# - update_username(session, user_id, user_name, timeout = 120) -> Allows for changing of the username
# - delete_user_reservation(session, user_id, reservation_id, timeout = 120) -> deletes a reservation from list of user's reservations in users table
# - cancel_reservation(session, reservation_id, timeout = 120) -> handles everything related to cancelling the reservation by chosen reservation_id
# - get_all_reservations(session, timeout = 120) -> retrieves all reservations from the reservations table
# - get_all_users(session, timeout = 120) -> retrieves all users from the users table
# - get_all_books(session, timeout = 120) -> retreieves all books from the books table
# - get_reservation_by_id(session, reservation_id, timeout=120) -> retrieves a reservation based on reservation_id from the reservations table
# - get_book(session, book_id, timeout=120) -> retrieves a book based on book_id from the books table
# - get_user(session, user_id, timeout=120) -> retrieves a user based on user_id from the users table


#TODO: Potentially set the below values for W, R, N
def create_keyspace(session, replication_factor = 2, timeout = 120):
    keyspace_query = """
        CREATE KEYSPACE IF NOT EXISTS library_keyspace 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %s};
    """
    session.execute(keyspace_query, [replication_factor], timeout = timeout)

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
    if reserved:
        book_set_query = """
            UPDATE books SET is_reserved = true WHERE book_id = %s;
        """
    else:
        book_set_query = """
            UPDATE books SET is_reserved = false WHERE book_id = %s;
        """
    try:
        session.execute(book_set_query, [book_id], timeout=timeout)
    except InvalidRequest as e:
        print("Error occurred while updating book reservation status:", e)
        raise e
    
def add_book(session, book_id, book_name, is_reserved, timeout = 120):
    if is_reserved:
        insert_book_query = """
            INSERT INTO books (book_id, book_name, is_reserved)
            VALUES (%s, %s, true);
        """
    else:
        insert_book_query = """
            INSERT INTO books (book_id, book_name, is_reserved)
            VALUES (%s, %s, false);
        """
    try:
        session.execute(insert_book_query, [book_id, book_name], timeout = timeout)
    except InvalidRequest as e:
        print("Error Occured while inserting a new book to the table: ", e)

def add_user(session, user_id, user_name, timeout = 120):
    insert_user_query = """
        INSERT INTO users (user_id, user_name)
        VALUES (%s, %s);
    """
    try:
        session.execute(insert_user_query, [user_id, user_name], timeout = timeout)
    except InvalidRequest as e:
        print("Error Occured while inserting a new user to the table: ", e)

def add_reservation_to_list(session, user_id, reservation_id, timeout = 120):
    book_list_update_query = f"""UPDATE users SET reservation_ids_list = reservation_ids_list + [{reservation_id}] WHERE user_id = {user_id}"""
    try:
        session.execute(book_list_update_query, timeout = timeout)
    except InvalidRequest as e:
        print("Error Occured while appending a book to list: ", e)
        raise e
        
def append_user_reservation(session, user_id, user_name, reservation_id, timeout = 120):
    try:
        user = get_user(session, user_id, timeout = timeout, print_error_message = False)
        if user is None:
            add_user(session, user_id, user_name, timeout = timeout)
        add_reservation_to_list(session, user_id, reservation_id, timeout=timeout)
    except InvalidRequest as e:
        print("Error Occured while adding a reservation to the user's reservation list: ", e)
        raise e

def add_reservation(session, reservation_id, user_id, user_name, book_name, book_id, timeout = 120):
    insert_reservation_query = """
        INSERT INTO reservations (reservation_id, user_id, user_name, book_name, book_id)
        VALUES (%s, %s, %s, %s, %s);
    """
    try:
        book = get_book(session, book_id = book_id)
        if not book.is_reserved:
            set_book_reserved(session, book_id=book_id, reserved = True, timeout = timeout)
            append_user_reservation(session, user_id, user_name, reservation_id, timeout = timeout)
            session.execute(insert_reservation_query, [reservation_id, user_id, user_name, book_name, book_id], timeout=timeout)
    except InvalidRequest as e:
        print("Error occurred while inserting reservation:", e)

def update_reservation(session, reservation_id, book_id, timeout = 120):
    update_reservation_query = """
        UPDATE reservations SET book_id = %s, book_name = %s WHERE reservation_id = %s
    """
    try:
        reservation = get_reservation_by_id(session, reservation_id, timeout = timeout)
        book = get_book(session, book_id, timeout = timeout)
        if not book.is_reserved:
            new_book_name = book.book_name
            past_book_id = reservation.book_id
            user_id = reservation.user_id
            set_book_reserved(session, past_book_id, reserved = False, timeout = timeout)
            set_book_reserved(session, book_id, reserved = True, timeout = timeout)
            delete_user_reservation(session, user_id, reservation_id, timeout = timeout)
            add_reservation_to_list(session, user_id, reservation_id, timeout = timeout)
            session.execute(update_reservation_query, [book_id, new_book_name, reservation_id], timeout = timeout)
    except InvalidRequest as e:
        print("Error occurred while updating a reservation:", e)

def update_username(session, user_id, user_name, timeout = 120):
    update_user_name_reservation_query = """
        UPDATE reservations SET user_name = %s WHERE reservation_id = %s
    """
    update_user_name_query = """
        UPDATE users SET user_name = %s WHERE user_id = %s
    """
    try:
        all_reservations = get_all_reservations(session, timeout = timeout)
        for reservation in all_reservations:
            if reservation.user_id == user_id:
                user_reservation_id = reservation.reservation_id
                session.execute(update_user_name_reservation_query, [user_name, user_reservation_id], timeout = timeout)
        session.execute(update_user_name_query, [user_name, user_id], timeout = timeout)
    except InvalidRequest as e:
        print("Error occurred while updating a reservation:", e)

def delete_user_reservation(session, user_id, reservation_id, timeout = 120):
    user_reservation_delete_query = f"""UPDATE users SET reservation_ids_list = reservation_ids_list - [{reservation_id}] WHERE user_id = {user_id}"""
    try:
        session.execute(user_reservation_delete_query, timeout=timeout)
    except InvalidRequest as e:
        print("Error occurred while removing reservation ID from user:", e)
        raise e

def cancel_reservation(session, reservation_id, timeout = 120):
    reservation_cancel_query = """
        DELETE FROM reservations WHERE reservation_id = %s;
    """
    try:
        reservation = get_reservation_by_id(session, reservation_id, timeout = timeout)
        user_id = reservation.user_id
        book_id = reservation.book_id
        set_book_reserved(session, book_id, reserved = False, timeout = timeout)
        delete_user_reservation(session, user_id, reservation_id, timeout = timeout)
        session.execute(reservation_cancel_query, [reservation_id], timeout = timeout)
    except InvalidRequest as e:
        print("Error canceling the reservation: ", e)

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
    query = f"""SELECT * FROM reservations WHERE reservation_id = {reservation_id}"""
    try:
        prepared = session.prepare(query)
        result = session.execute(prepared, timeout=timeout)
        reservation = result.one()
        if reservation is None:
            raise ValueError("No reservation found with ID: {}".format(reservation_id))
        return reservation
    except (InvalidRequest, ReadTimeout, ReadFailure, ValueError) as e:
        print("Error occurred while fetching the reservation:", e)
        return None
    
def get_book(session, book_id, timeout=120):
    query = f"""SELECT * FROM books WHERE book_id = {book_id}"""
    try:
        prepared = session.prepare(query)
        result = session.execute(prepared, timeout=timeout)
        book = result.one()
        if book is None:
            raise ValueError("No book found with ID: {}".format(book_id))
        return book
    except (InvalidRequest, ReadTimeout, ReadFailure, ValueError) as e:
        print("Error occurred while fetching the book:", e)
        return None

def get_user(session, user_id, timeout=120, print_error_message = True):
    query = f""" SELECT * FROM users WHERE user_id = {user_id}"""
    try:
        prepared = session.prepare(query)
        result = session.execute(prepared, timeout=timeout)
        user = result.one()
        if user is None:
            raise ValueError("No user found with ID: {}".format(user_id))
        return user
    except (InvalidRequest, ReadTimeout, ReadFailure, ValueError) as e:
        if print_error_message:
            print("Error occurred while fetching the user:", e)
        return None