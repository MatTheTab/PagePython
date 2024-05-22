import os
import tkinter as tk
import customtkinter as ctk

from PIL import Image
from collections import namedtuple

from cassandra.cluster import Cluster, BatchStatement
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy

from utils.query_utils import *

# SET SESSION ##############################
cluster = Cluster(['172.19.0.2'])
session = cluster.connect()
session.set_keyspace('library_keyspace')

cluster.default_retry_policy = RetryPolicy()
cluster.default_reconnection_policy = ExponentialReconnectionPolicy(base_delay=1, max_delay=60, max_attempts=60)

# SET WINDOW ##############################
DARK_MODE = "dark"
ctk.set_appearance_mode(DARK_MODE)
ctk.set_default_color_theme("dark-blue")

WINDOW_WIDTH = 950
WINDOW_HEIGHT = 600

FRAME_WIDTH = 750
FRAME_HEIGHT = 600

Reservation = namedtuple('Reservation', ['id', 'user_id', 'user_name', 'book_name', 'book_id'])
reservations = [
    Reservation(1, 101, "John", "To Kill a Mockingbird", 1001),
    Reservation(2, 102, "John", "1984", 1002),
    Reservation(3, 103, "John", "The Great Gatsby", 1003),
    Reservation(4, 104, "John", "The Catcher in the Rye", 1004),
    Reservation(5, 105, "John", "Moby Dick", 1005),
    Reservation(6, 106, "John", "Pride and Prejudice", 1006),
    Reservation(7, 107, "John", "The Lord of the Rings", 1007),
    Reservation(8, 108, "John", "Jane Eyre", 1008),
    Reservation(9, 109, "John", "The Hobbit", 1009),
    Reservation(10, 110, "John", "Fahrenheit 451", 1010),
    Reservation(11, 111, "John", "Brave New World", 1011),
    Reservation(12, 112, "John", "Animal Farm", 1012),
    Reservation(13, 113, "John", "War and Peace", 1013),
    Reservation(14, 114, "John", "Crime and Punishment", 1014),
    Reservation(15, 115, "John", "Wuthering Heights", 1015),
    Reservation(16, 116, "John", "The Odyssey", 1016),
    Reservation(17, 117, "John", "Great Expectations", 1017),
    Reservation(18, 118, "John", "Little Women", 1018),
    Reservation(19, 119, "John", "Harry Potter and the Sorcerer's Stone", 1019),
    Reservation(20, 120, "John", "The Chronicles of Narnia", 1020)
]

def find_available_id(reservations):
    for i in range(1, len(reservations)+1):
        if i not in set([x.id for x in reservations]):
            return i

    return i + 1

class ReservationFrame(ctk.CTkFrame):
    # TODO
    def __init__(self, master):
        super().__init__(master, width=FRAME_WIDTH, height=FRAME_HEIGHT//2, fg_color="transparent")
        self.reservation_entries = []
        
        self.header_label = ctk.CTkLabel(self, text="Make a reservation", font=("Calibri", 19, 'bold'), fg_color="transparent")
        self.header_label.grid(row=0, column=0, padx=0, pady=(0, 10))
        
        for i, value in enumerate(["User ID", "Book name", "Book ID"], start=1):
            entry_label = ctk.CTkLabel(self, text=value, font=("Calibri", 17), fg_color="transparent")
            entry_label.grid(row=i, column=0, padx=(0, 15), pady=5, sticky="e")

            entry = ctk.CTkEntry(self, placeholder_text="Enter a value", width=300)
            entry.grid(row=i, column=1, padx=0, pady=5)
            self.reservation_entries.append(entry)

        self.confirm_button = ctk.CTkButton(self, text="Confirm", command=self.confirm_reservation, height=40)
        self.confirm_button.grid(row=4, column=0, padx=10, pady=(10, 0), sticky="w")

    def confirm_reservation(self):
        text = ""
        id = find_available_id(reservations)
        text += str(id) + " "
        for entry in self.reservation_entries:
            val = entry.get()
            text += val + " "

        print("Confirm:", text)

class UpdateFrame(ctk.CTkFrame):
    # TODO
    def __init__(self, master):
        super().__init__(master, width=FRAME_WIDTH, height=FRAME_HEIGHT//2, fg_color="transparent")
        self.update_entries = []

        self.header_label = ctk.CTkLabel(self, text="Update a reservation", font=("Calibri", 19, 'bold'), fg_color="transparent")
        self.header_label.grid(row=0, column=0, padx=0, pady=(0, 10))

        for i, value in enumerate(["Reservation ID", "User ID", "Book name", "Book ID"], start=1):
            entry_label = ctk.CTkLabel(self, text=value, font=("Calibri", 17), fg_color="transparent")
            entry_label.grid(row=i, column=0, padx=(0, 15), pady=5, sticky="e")

            entry = ctk.CTkEntry(self, placeholder_text="Enter a new value", width=300)
            entry.grid(row=i, column=1, padx=0, pady=5)
            self.update_entries.append(entry)

        self.update_button = ctk.CTkButton(self, text="Update", command=self.update_reservation, height=40)
        self.update_button.grid(row=5, column=0, padx=10, pady=(10, 0), sticky="w")

    def update_reservation(self):
        text = ""
        for entry in self.update_entries:
            val = entry.get()
            text += val + " "
        print("Update:", text)

    

class ScrollableList(ctk.CTkScrollableFrame):
    # TODO
    def __init__(self, master, **kwargs):
        super().__init__(master, width=FRAME_WIDTH, height=FRAME_HEIGHT, **kwargs)
        self.grid_columnconfigure((0, 1, 2, 3, 4, 5, 6), weight=1)
        
        self.label_list = []
        self.button_list = []
        self.id_button_list = []

        # Define the view column widths and headers
        self.headers = ['Reservation ID', 'User ID', 'User name', 'Book name', 'Book ID']

        for col_index, header in enumerate(self.headers):
            header_label = ctk.CTkLabel(self, text=header, font=("Calibri", 19, 'bold'), fg_color="transparent")
            header_label.grid(row=0, column=col_index, padx=0, pady=(0, 10), sticky="w")
        
    def add_item(self, item):
        id_button = ctk.CTkButton(self, fg_color="blue", text="Get IDs", width=70, height=24, command=lambda: self.get_reservation_id(item))
        id_button.grid(row=len(self.button_list)+1, column=5, padx=(0, 5))
        self.id_button_list.append(id_button)

        delete_button = ctk.CTkButton(self, fg_color="#b33232", text="Cancel", width=70, height=24, command=lambda: self.cancel_reservation(item))
        delete_button.grid(row=len(self.button_list)+1, column=6, padx=(0, 25), pady=(5, 5))
        self.button_list.append(delete_button)
        
        item_values = [str(item.reservation_id), str(item.user_id), item.user_name, item.book_name, str(item.book_id)]
        row_labels = []
        for col_index, value in enumerate(item_values):
            label = ctk.CTkLabel(self, text=value, font=("Calibri", 17), fg_color="transparent")
            label.grid(row=len(self.label_list) + 1, column=col_index, sticky="w", pady=(5, 5))
            label.configure(wraplength=80)
            row_labels.append(label)

        self.label_list.append(row_labels)

    def update_view(self, reservations):
        for row_labels, button, id_button in zip(self.label_list, self.button_list, self.id_button_list):
            for label in row_labels:
                label.destroy()

            self.label_list.remove(row_labels)

            button.destroy()
            self.button_list.remove(button)

            id_button.destroy()
            self.id_button_list.remove(id_button)
            
        for res in reservations:
            self.add_item(res)

    def cancel_reservation(self, item):
        for row_labels, button in zip(self.label_list, self.button_list):
            if item.id == int(row_labels[0].cget("text")):
                for label in row_labels:
                    label.destroy()

                self.label_list.remove(row_labels)

                button.destroy()
                self.button_list.remove(button)
                return
            
    def get_reservation_id(self, item):
        print("IDs:")
        print("Reservation:", item.reservation_id)
        print("User:", item.user_id)
        print("Book:", item.book_id)
            
class ScrollableListTests(ctk.CTkScrollableFrame):
    # TODO
    def __init__(self, master, **kwargs):
        super().__init__(master, width=FRAME_WIDTH, height=FRAME_HEIGHT, **kwargs)

        tests_header_label = ctk.CTkLabel(self, text="Run tests", font=("Calibri", 20, 'bold'), fg_color="transparent")
        tests_header_label.grid(row=0, column=0, padx=0, pady=(0, 10))

        stress_test_labels = [
            "Stress test 1: Make the same request 10,000 times.",
            "Stress test 2: Two or clients make the possible requests randomly 10,000 times.",
            "Stress test 3: Immediate occupancy of all reservations by 2 clients.",
            "Stress test 4: Constant cancellations and seat occupancy for the same seat 10,000 times",
            "Stress test 5: Update of 1000 reservations"
        ]

        commands = [self.stress_test1, self.stress_test2, self.stress_test3, self.stress_test4, self.stress_test5]

        for i, test in enumerate(zip(stress_test_labels, commands), start=1):
            test_label = test[0]
            command = test[1]

            label_stress_test = ctk.CTkLabel(self, text=test_label, font=("Calibri", 17), fg_color="transparent")
            label_stress_test.grid(row=2*i-1, column=0, padx=(0, 15), pady=5, sticky="w")

            stress_test = ctk.CTkButton(self, text="Run", command=command)
            stress_test.grid(row=2*i, column=0, padx=0, pady=(5, 15), sticky="w")
        
    def stress_test1(self):
        print(f"Test 1")

    def stress_test2(self):
        print(f"Test 2")

    def stress_test3(self):
        print(f"Test 3")

    def stress_test4(self):
        print(f"Test 4")

    def stress_test5(self):
        print(f"Test 5")


class App(ctk.CTk):
    frames = {"frame1": None, "frame2": None, "frame3": None}

    def __init__(self):
        super().__init__()

        self.title("Distributed Library")
        self.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}")

        # contains everything
        main_container = ctk.CTkFrame(self)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # left side panel -> for frame selection
        left_side_panel = ctk.CTkFrame(main_container, width=150)
        left_side_panel.pack(side=tk.LEFT, fill=tk.Y, expand=False, padx=10, pady=10)

        # buttons to select the frames
        bt_frame1 = ctk.CTkButton(left_side_panel, text="Reservations", font=("Calibri", 15, 'bold'), command=self.select_updates_frame)
        bt_frame1.grid(row=0, column=0, padx=20, pady=10)

        bt_frame2 = ctk.CTkButton(left_side_panel, text="View", font=("Calibri", 15, 'bold'), command=self.select_view_frame)
        bt_frame2.grid(row=1, column=0, padx=20, pady=10)

        bt_frame2 = ctk.CTkButton(left_side_panel, text="Tests", font=("Calibri", 15, 'bold'), command=self.select_tests_frame)
        bt_frame2.grid(row=2, column=0, padx=20, pady=10)

        
        logo = ctk.CTkImage(light_image=Image.open('PagePython/images/PagePython_logo_2.png'), dark_image=Image.open('PagePython/images/PagePython_logo_2.png'), size=(100, 100))

        my_label = ctk.CTkLabel(left_side_panel, text="", image=logo)
        my_label.grid(row=4, pady=(280, 0), sticky="s")

        self.right_side_panel = ctk.CTkFrame(main_container)
        self.right_side_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.right_side_container = ctk.CTkFrame(self.right_side_panel, fg_color="#000811")
        self.right_side_container.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=0, pady=0)

        # Reservations elements ##############################################################################
        App.frames['frame1'] = ctk.CTkFrame(main_container, fg_color="transparent")
    
        self.reservation_frame = ReservationFrame(App.frames['frame1'])
        self.reservation_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        
        self.update_frame = UpdateFrame(App.frames['frame1'])
        self.update_frame.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True)


        # View elements ######################################################################################
        App.frames['frame2'] = ctk.CTkFrame(main_container, fg_color="transparent")
        
        self.scrollable_list = ScrollableList(master=App.frames['frame2'], corner_radius=0)
        self.scrollable_list.pack(padx=(20, 0), pady=0)
        self.scrollable_list.reservations = get_all_reservations(session)
        for res in self.scrollable_list.reservations:
            self.scrollable_list.add_item(res)
        

        # Tests elements #####################################################################################
        App.frames['frame3'] = ctk.CTkFrame(main_container, fg_color="transparent")
        
        self.scrollable_list_tests = ScrollableListTests(master=App.frames['frame3'], corner_radius=0)
        self.scrollable_list_tests.pack(padx=(20, 0), pady=0)

    def select_updates_frame(self):
        App.frames["frame2"].pack_forget()
        App.frames["frame3"].pack_forget()
        App.frames["frame1"].pack(in_=self.right_side_container, side=tk.TOP, fill=tk.BOTH, expand=True, padx=0, pady=0)

    def select_view_frame(self):
        self.scrollable_list.update_view(get_all_reservations(session, 10))
        App.frames["frame1"].pack_forget()
        App.frames["frame3"].pack_forget()
        App.frames["frame2"].pack(in_=self.right_side_container, side=tk.TOP, fill=tk.BOTH, expand=True, padx=0, pady=0)

    def select_tests_frame(self):
        App.frames["frame1"].pack_forget()
        App.frames["frame2"].pack_forget()
        App.frames["frame3"].pack(in_=self.right_side_container, side=tk.TOP, fill=tk.BOTH, expand=True, padx=0, pady=0)


if __name__ == "__main__":
    app = App()
    app.mainloop()
    print("Exit")

    session.shutdown()
    cluster.shutdown()

# if __name__ == "__main__":
#     app = App()
#     app.mainloop()
#     print("Exit")