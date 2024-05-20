import tkinter as tk
import customtkinter as ctk

from PIL import Image
import os

from collections import namedtuple


DARK_MODE = "dark"
ctk.set_appearance_mode(DARK_MODE)
ctk.set_default_color_theme("dark-blue")

WINDOW_WIDTH = 950
WINDOW_HEIGHT = 600

FRAME_WIDTH = 750
FRAME_HEIGHT = 600

Reservation = namedtuple('Reservation', ['id', 'user_id', 'book_name', 'book_id'])
reservations = [
    Reservation(1, 101, "To Kill a Mockingbird", 1001),
    Reservation(2, 102, "1984", 1002),
    Reservation(3, 103, "The Great Gatsby", 1003),
    Reservation(4, 104, "The Catcher in the Rye", 1004),
    Reservation(5, 105, "Moby Dick", 1005),
    Reservation(6, 106, "Pride and Prejudice", 1006),
    Reservation(7, 107, "The Lord of the Rings", 1007),
    Reservation(8, 108, "Jane Eyre", 1008),
    Reservation(9, 109, "The Hobbit", 1009),
    Reservation(10, 110, "Fahrenheit 451", 1010),
    Reservation(11, 111, "Brave New World", 1011),
    Reservation(12, 112, "Animal Farm", 1012),
    Reservation(13, 113, "War and Peace", 1013),
    Reservation(14, 114, "Crime and Punishment", 1014),
    Reservation(15, 115, "Wuthering Heights", 1015),
    Reservation(16, 116, "The Odyssey", 1016),
    Reservation(17, 117, "Great Expectations", 1017),
    Reservation(18, 118, "Little Women", 1018),
    Reservation(19, 119, "Harry Potter and the Sorcerer's Stone", 1019),
    Reservation(20, 120, "The Chronicles of Narnia", 1020)
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
        self.grid_columnconfigure((0, 1, 2, 3, 4), weight=1)
        
        self.label_list = []
        self.button_list = []

        # Define the view column widths and headers
        self.headers = ['ID', 'User_ID', 'Book_name', 'Book_ID']

        for col_index, header in enumerate(self.headers):
            header_label = ctk.CTkLabel(self, text=header, font=("Calibri", 19, 'bold'), fg_color="transparent")
            header_label.grid(row=0, column=col_index, padx=0, pady=(0, 10), sticky="w")
        

    def add_item(self, item):
        delete_button = ctk.CTkButton(self, fg_color="#b33232", text="Delete", width=100, height=24, command=lambda: self.delete_reservation(item))
        delete_button.grid(row=len(self.button_list)+1, column=4, padx=(0, 25), pady=(5, 5))
        self.button_list.append(delete_button)
        
        item_values = [str(item.id), str(item.user_id), item.book_name, str(item.book_id)]
        row_labels = []
        for col_index, value in enumerate(item_values):
            label = ctk.CTkLabel(self, text=value, font=("Calibri", 17), fg_color="transparent")
            label.grid(row=len(self.label_list) + 1, column=col_index, sticky="w")
            row_labels.append(label)

        self.label_list.append(row_labels)

    def delete_reservation(self, item):
        for row_labels, button in zip(self.label_list, self.button_list):
            if item.id == int(row_labels[0].cget("text")):
                for label in row_labels:
                    label.destroy()

                self.label_list.remove(row_labels)

                button.destroy()
                self.button_list.remove(button)
                return
            
    def label_list_event(self, item: Reservation):
        print(f"Reservation clicked: {item.id}, {item.user_id}, {item.book_name}, {item.book_id}")
            
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
        bt_frame1 = ctk.CTkButton(left_side_panel, text="Reservations", font=("Calibri", 15, 'bold'), command=self.frame1_selector)
        bt_frame1.grid(row=0, column=0, padx=20, pady=10)

        bt_frame2 = ctk.CTkButton(left_side_panel, text="View", font=("Calibri", 15, 'bold'), command=self.frame2_selector)
        bt_frame2.grid(row=1, column=0, padx=20, pady=10)

        bt_frame2 = ctk.CTkButton(left_side_panel, text="Tests", font=("Calibri", 15, 'bold'), command=self.frame3_selector)
        bt_frame2.grid(row=2, column=0, padx=20, pady=10)

        
        logo = ctk.CTkImage(light_image=Image.open('imgs/chat.png'), dark_image=Image.open('imgs/chat.png'), size=(100, 100))

        my_label = ctk.CTkLabel(left_side_panel, text="", image=logo)
        my_label.grid(row=4, pady=(300, 0), sticky="s")

        self.right_side_panel = ctk.CTkFrame(main_container)
        self.right_side_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.right_side_container = ctk.CTkFrame(self.right_side_panel, fg_color="#000811")
        self.right_side_container.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=0, pady=0)

        # Reservations elements ##############################################################################
        # TODO
        App.frames['frame1'] = ctk.CTkFrame(main_container, fg_color="transparent")
    
        self.checkbox_frame_1 = ReservationFrame(App.frames['frame1'])
        self.checkbox_frame_1.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        
        self.checkbox_frame_2 = UpdateFrame(App.frames['frame1'])
        self.checkbox_frame_2.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True)


        # View elements ######################################################################################
        # TODO
        App.frames['frame2'] = ctk.CTkFrame(main_container, fg_color="transparent")
        
        self.scrollable_list = ScrollableList(master=App.frames['frame2'], corner_radius=0)
        self.scrollable_list.pack(padx=(20, 0), pady=0)
        for res in reservations:
            self.scrollable_list.add_item(res)
        

        # Tests elements #####################################################################################
        # TODO
        App.frames['frame3'] = ctk.CTkFrame(main_container, fg_color="transparent")
        
        tests_header_label = ctk.CTkLabel(App.frames['frame3'], text="Run tests", font=("Calibri", 20, 'bold'), fg_color="transparent")
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

            label_stress_test = ctk.CTkLabel(App.frames['frame3'], text=test_label, font=("Calibri", 17), fg_color="transparent")
            label_stress_test.grid(row=2*i-1, column=0, padx=(0, 15), pady=5, sticky="w")

            stress_test = ctk.CTkButton(App.frames['frame3'], text="Run", command=command)
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

    def label_list_event(self, item):
        print(f"label frame clicked: {item}")

    def frame1_selector(self):
        App.frames["frame2"].pack_forget()
        App.frames["frame3"].pack_forget()
        App.frames["frame1"].pack(in_=self.right_side_container, side=tk.TOP, fill=tk.BOTH, expand=True, padx=0, pady=0)

    def frame2_selector(self):
        App.frames["frame1"].pack_forget()
        App.frames["frame3"].pack_forget()
        App.frames["frame2"].pack(in_=self.right_side_container, side=tk.TOP, fill=tk.BOTH, expand=True, padx=0, pady=0)

    def frame3_selector(self):
        App.frames["frame1"].pack_forget()
        App.frames["frame2"].pack_forget()
        App.frames["frame3"].pack(in_=self.right_side_container, side=tk.TOP, fill=tk.BOTH, expand=True, padx=0, pady=0)


if __name__ == "__main__":
    app = App()
    app.mainloop()
    print("Exit")