import time
import uuid

def select_all_reservations(session, retries = 10, timeout = 120):
    while retries > 0:
        try:
            selection_query = """
                SELECT * FROM reservations;
            """
            result = session.execute(selection_query, timeout = timeout)
            return result
        except Exception as e:
            retries -= 1
            if retries > 0:
                time.sleep(5)
            else:
                raise e
            
def select_reservation(session, reservation_id, retries=10, timeout = 120):
    while retries > 0:
        try:
            selection_query = """
                SELECT * FROM reservations WHERE reservation_id = %s;
            """
            result = session.execute(selection_query, [reservation_id], timeout = timeout)
            return result
        except Exception as e:
            retries -= 1
            if retries > 0:
                time.sleep(5)
            else:
                raise e

def make_reservation(session, user_id, book_id, reservation_start="toTimestamp(now())", reservation_end="toTimestamp(now()) + 1d", book_name=None, genre=None, retries=10, timeout=120):
    while retries > 0:
        try:
            reservation_id = uuid.uuid4()
            if book_name is None:
                book_name = "NULL"
            else:
                book_name = f"'{book_name}'"
            if genre is None:
                genre = "NULL"
            else:
                genre = f"'{genre}'"
            insert_query = f"INSERT INTO reservations (reservation_id, user_id, reservation_start, reservation_end, book_id, book_name, book_genre) VALUES  ({reservation_id}, {user_id}, {reservation_start}, {reservation_end}, {book_id}, {book_name}, {genre});"
            session.execute(insert_query, timeout=timeout)
            return
        except Exception as e:
            retries -= 1
            if retries > 0:
                time.sleep(5)
            else:
                raise e

def update_reservation_end_time(session, reservation_id, new_end_time, retries=10, timeout = 120):
    while retries > 0:
        try:
            update_query = f"UPDATE reservations SET reservation_end = {new_end_time} WHERE reservation_id = {reservation_id};"
            print(update_query)
            session.execute(update_query, timeout=timeout)
            return
        except Exception as e:
            retries -= 1
            if retries > 0:
                time.sleep(5)
            else:
                raise e

def update_reservation_book_title(session, reservation_id, new_book_title, retries=10, timeout = 120):
    while retries > 0:
        try:
            update_query = """
                UPDATE reservations SET book_name = %s WHERE reservation_id = %s;
            """
            session.execute(update_query, [new_book_title, reservation_id], timeout=timeout)
            return
        except Exception as e:
            retries -= 1
            if retries > 0:
                time.sleep(5)
            else:
                raise e
            
def update_reservation_book_genre(session, reservation_id, new_book_genre, retries=10, timeout = 120):
    while retries > 0:
        try:
            update_query = """
                UPDATE reservations SET book_genre = %s WHERE reservation_id = %s;
            """
            session.execute(update_query, [new_book_genre, reservation_id], timeout=timeout)
            return
        except Exception as e:
            retries -= 1
            if retries > 0:
                time.sleep(5)
            else:
                raise e
            
def update_reservation_book_id(session, reservation_id, new_book_id, retries=10, timeout = 120):
    while retries > 0:
        try:
            update_query = """
                UPDATE reservations SET book_id = %s WHERE reservation_id = %s;
            """
            session.execute(update_query, [new_book_id, reservation_id], timeout=timeout)
            return
        except Exception as e:
            retries -= 1
            if retries > 0:
                time.sleep(5)
            else:
                raise e

def cancel_reservation(session, reservation_id, retries=10, timeout = 120):
    while retries > 0:
        try:
            delete_query = """
                DELETE FROM reservations WHERE reservation_id = %s;
            """
            session.execute(delete_query, [reservation_id], timeout=timeout)
            return
        except Exception as e:
            retries -= 1
            if retries > 0:
                time.sleep(5)
            else:
                raise e