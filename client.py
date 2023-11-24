import socket
import time
import json
import client_lib as client_lib

HOST = "127.0.0.1" 
PORT = 12345  

def instructions():
    # instructions to the user
    print ("------------------- INSTRUCTIONS ----------------------")
    print ("<write> [filename] - write to file mode")
    print ("<read> [filename] - read from file mode")
    print ("<list> - lists all existing files")
    print ("<instructions> - lets you see the instructions again")
    print ("<quit> - exits the application")
    print ("-------------------------------------------------------\n")

def read_file(file_name):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        message = {'operation': 'get_metadata', 'file_path': file_name}
        json_data = json.dumps(message)
        print(f'encoded text - {json_data.encode()}')
        s.send(json_data.encode())
        response = s.recv(1024).decode()
        return response
    
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

def delete_file():
    raise NotImplementedError

def check_valid_input(file_name):
    return '.txt' in file_name
    


while(True):
    instructions()
    _input = input()
    if "<write>" in _input:
        while not check_valid_input(_input):    
                _input = input('Invalid input ; please try using a valid name')
        
        file_name = _input.split()[1]     
        response = write_file(file_name)  

        print(f'write response {response}')
        print ("Exiting <write> mode...\n")
        
    elif "<read>" in _input:
        while not check_valid_input(_input):   
                _input = input('Invalid input ; please try using a valid name')
        file_name = _input.split()[1]  
        # check in cache
        if(client_lib.in_cache(file_name)):
            response = client_lib.cache(file_name, None, 'r')
        else:
            response = read_file(file_name) 
            print(f'read response {response}')
        print("Exiting <read> mode...\n")

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