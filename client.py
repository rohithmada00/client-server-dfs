import socket

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

def read_file(file_name, client_socket):
    message = file_name + '|' + 'r' + '|' + ''
    print(f'encoded text - {message.encode()}')
    client_socket.send(message.encode())
    response = client_socket.recv(1024).decode()
    return response
    

def write_file(file_name, client_socket):
    content = input('Please enter the content to write: ')
    message = file_name + '|' + 'w' + '|' + content
    client_socket.send(message.encode())
    response = client_socket.recv(1024).decode()
    return response

def list_files():
    raise NotImplementedError

def delete_file():
    raise NotImplementedError

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