import socket
import time
import os

HOST = "127.0.0.1" 
PORT = 12345  

def instructions():
    # instructions to the user
    print ("------------------- INSTRUCTIONS ----------------------")
    print ("<write> [filename] - write to file mode")
    print ("<read> [filename] - read from file mode")
    print ("<edit> [filename] - edit from file mode")
    print ("<delete> [filename] - delete from file mode")
    print ("<list> - lists all existing files")
    print ("<instructions> - lets you see the instructions again")
    print ("<quit> - exits the application")
    print ("-------------------------------------------------------\n")

def read_file(file_name, client_socket):
    message = file_name + '|' + 'r' + '|' + ''
    print(f'encoded text - {message.encode()}')
    client_socket.send(message.encode())
    response = client_socket.recv(1024).decode()
    return response
    
def edit_file(filename):
    file_path = filename
    try:
        with open('replica_1/'+file_path, 'r') as file:
            lines = file.readlines()
    except FileNotFoundError:
        print("File not found. Please check the file path.")
        return
    except IOError:
        print("An error occurred while reading the file.")
        return
    # locking the file
    grant_lock = lock_unlock_file('client 2', file_name, "lock")

    while grant_lock != "file granted":
            print("File not granted, polling again...")
            grant_lock = lock_unlock_file('client2', file_name, "lock")

            if grant_lock == "TIMEOUT":     
                return False

            time.sleep(0.1)     

    print("You are granted the file...")
    
    #editing part
    number = input("\nEnter the line number you want to edit: ")
    if not number.isdigit() or int(number) < 1 or int(number) > len(lines):
        print("Invalid line number.")
        reply_unlock = lock_unlock_file('client2', file_name, "unlock")
        print(reply_unlock)
        return True
    content = input("Enter the new content for this line: ")
    lines[int(number) - 1] = content + "\n"
    try:
        with open(file_path, 'w') as file:
            file.writelines(lines)
        with open('replica_1/'+file_path, 'w') as file:
            file.writelines(lines)
        with open("files/"+file_path, 'w') as file:
            file.writelines(lines)
        print("File updated successfully.")
    except IOError:
        print("An error occurred while writing to the file.")
    
    # unlocking part
    reply_unlock = lock_unlock_file('client2', file_name, "unlock")
    print(reply_unlock)

    return True
    

def write_file(file_name, client_socket):

    # locking the file
    grant_lock = lock_unlock_file('client 2', file_name, "lock")

    while grant_lock != "file granted":
            print("File not granted, polling again...")
            grant_lock = lock_unlock_file('client2', file_name, "lock")

            if grant_lock == "TIMEOUT":     
                return False

            time.sleep(0.1)     

    print("You are granted the file...")

    # writing part
    content = input('Please enter the content to write: ')
    message = file_name + '|' + 'w' + '|' + content
    client_socket.send(message.encode())
    response = client_socket.recv(1024).decode()
    print(f'response after writing to fs {response}')

    # unlocking part
    reply_unlock = lock_unlock_file('client2', file_name, "unlock")
    print(reply_unlock)

    return True

def lock_unlock_file( client_id, filename, lock_or_unlock):
    serverName = 'localhost'
    serverPort = 12367   # port of directory service
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((serverName,serverPort))

        if lock_or_unlock == "lock":
            msg = client_id + '|' + 'LOCK' + '|' + filename  # 1 = lock the file
        elif lock_or_unlock == "unlock":
            msg = client_id + '|' + 'UNLOCK' + '|' + filename   # 2 = unlock the file

        # send the string requesting file info to directory service
        print(f'encoded message is {msg}')
        s.send(msg.encode())
        reply = s.recv(1024)
        reply = reply.decode()
        s.close()

    return reply

def list_files():
    raise NotImplementedError

def delete_file(filename):
    if os.path.exists(filename) or os.path.exists("files/"+filename ) or os.path.exists("replica_1/"+filename):
        try:
            os.remove("files/"+filename )
        except: 
            pass
        try:
            os.remove(filename)
        except:
            pass
        try:
            os.remove("replica_1/"+filename )
        except: 
            pass
        
        print('File removed from the system...')
    else:
        print('File doesnot exist in system...')
    return True

def check_valid_input(file_name):
    return '.txt' in file_name
    

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    while(True):
        instructions()
        _input = input()
        print('1')
        if "<write>" in _input:
            while not check_valid_input(_input):    
                 _input = input('Invalid input ; please try using a valid name')
            
            file_name = _input.split()[1]     
            response = write_file(file_name, s)  

            print(f'write response {response}')
            print ("Exiting <write> mode...\n")
            
        elif "<read>" in _input:
            print('2')
            while not check_valid_input(_input):   
                 _input = input('Invalid input ; please try using a valid name')
            print('3')
            file_name = _input.split()[1]  
            response = read_file(file_name, s) 
            print('4')
            print(f'read response {response}')
            print("Exiting <read> mode...\n")
        
        elif "<edit>" in _input:
            while not check_valid_input(_input):   
                 _input = input('Invalid input ; please try using a valid name')
            file_name = _input.split()[1]  
            response = edit_file(file_name)  
            print(f'edit response {response}')
            print ("Exiting <edit> mode...\n")
        
        elif "<delete>" in _input:
            while not check_valid_input(_input):   
                 _input = input('Invalid input ; please try using a valid name')
            file_name = _input.split()[1]  
            response = delete_file(file_name)  
            print(f'delete response {response}')
            print ("Exiting <delete> mode...\n")


        else:
            match _input:
                case '<list>':
                    list_files()
                case '<instructions>':
                    instructions()
                case '<exit>':
                    break
                case _:
                    print('Invalid query.Please try again !!')