# PagePython
Python app for a library book renting system utilizing distributed systems with Cassandra.

Instructions:

1. Make sure you have the correct requirements installed (requirements.txt)
2. To activate cassandra move to the ./Docker folder and run ```docker-compose up -d``` command.
3. View nodes' address and substitute it in the necessary files in the PagePython folder (GUI_customtk.py, stress_test_DB.py, utils/create_static_DB.py, utils/delete_static_DB.py)
4. To run GUI:
    - Run ```python PagePython/utils/create_static_DB.py``` command.
    - Run ```python PagePython/GUI_customtk.py``` command. 
    - After quiting, run ```python PagePython/utils/delete_static_DB.py``` command.
5. To run stress tests run ```python PagePython/stress_test_DB.py``` command.