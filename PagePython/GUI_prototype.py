import tkinter as tk
import tkinter.ttk as ttk

def update_pressed():
    update_button.configure(style='Pressed.TButton')
    reservations_button.configure(style='TButton')
    tests_button.configure(style='TButton')
    canvas.configure(bg='red')

def reservations_pressed():
    update_button.configure(style='TButton')
    reservations_button.configure(style='Pressed.TButton')
    tests_button.configure(style='TButton')
    canvas.configure(bg='green')

def tests_pressed():
    update_button.configure(style='TButton')
    reservations_button.configure(style='TButton')
    tests_button.configure(style='Pressed.TButton')
    canvas.configure(bg='blue')

root = tk.Tk()
root.title("Page Python")

screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()
button_height = int(screen_height * 0.8)

style = ttk.Style()
style.configure('TButton', foreground='black', font=('Helvetica', 12))
style.map('TButton',
          foreground=[('active', 'blue')],
          background=[('active', 'white')])

style.configure('Pressed.TButton', foreground='white', background='blue')

update_button = ttk.Button(root, text="Update", style='TButton', command=update_pressed)
update_button.place(relx=0.05, rely=0.1, anchor=tk.CENTER)
update_button.update()
update_button_width = update_button.winfo_width() + 0.005*screen_width
update_button_width = update_button_width/screen_width

reservations_button = ttk.Button(root, text="Reservations", style='TButton', command=reservations_pressed)
reservations_button.place(relx=0.05 + update_button_width, rely=0.1, anchor=tk.CENTER)
reservations_button.update()
reservations_button_width = reservations_button.winfo_width() - 0.0015*screen_width
reservations_button_width = reservations_button_width/screen_width

tests_button = ttk.Button(root, text="Tests", style='TButton', command=tests_pressed)
tests_button.place(relx=0.05 + update_button_width + reservations_button_width, rely=0.1, anchor=tk.CENTER)
tests_button.update()
test_button_height = tests_button.winfo_height()
test_button_height = test_button_height/screen_height
canvas_height = screen_height*0.5
relative_canvas_height = canvas_height/screen_height
canvas = tk.Canvas(root, bg='white', width=screen_width*2, height=canvas_height)
canvas.place(relx=0.0, rely=relative_canvas_height - (0.1+test_button_height) + 0.005, anchor=tk.CENTER)

root.mainloop()

print("Finished")
