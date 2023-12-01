from socket import *
import json
import random

HOST = "127.0.0.1"
PORTS = ['11234']

def instructions():
    # instructions to the user
    print("------------------- INSTRUCTIONS ----------------------")
    print("<write> [filename] - write to file mode")
    print("<read> [filename] - read from file mode")
    print("<list> - lists all existing files")
    print("<instructions> - lets you see the instructions again")
    print("<quit> - exits the application")
    print("-------------------------------------------------------\n")

def read_file(file_name):
    client_socket = contact_random_server()
    
    # ask for read
    message = {
        'file_name': file_name,
        'operation': 'r',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    
    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    print(f'Received response from server...')
    status = response.get('status', 'error')

    if status == 'success':
        content = response.get('content', '###')
        print(f'File content:\n{content}')

    client_socket.close()

def contact_random_server():
    port = random.choice(PORTS)
    print(f'Connecting to random server on port {port}...')
    client_socket = socket(AF_INET, SOCK_STREAM)
    client_socket.connect((HOST, int(port)))
    return client_socket

def write_file(file_name):
    client_socket = contact_random_server()
    
    # ask for write
    message = {
        'file_name': file_name,
        'operation': 'w',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    
    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    status = response.get('status', 'error')
    message = response.get('message')

    # if pending wait for another message
    if status == 'pending':
        print(f'Received response from server...')
        print(message)
        
        # Receive another message
        response = client_socket.recv(1024).decode()
        response = json.loads(response)
        status = response.get('status', 'error')
        message = response.get('message')

    # if success take input and send
    if status == 'success':
        print(f'Received response from server...')
        print(message)
        
        print('Sending file contents to server...')
        content = input('Please enter the content to write: ')

        message = {
            'content': content
        }
        data = json.dumps(message).encode()
        client_socket.send(data)

        response = client_socket.recv(1024).decode()
        response = json.loads(response)

    print(f'Received response from server...')
    print(response)
    client_socket.close()
    return True if status == 'success' else False

def create_file(file_name):
    client_socket = contact_random_server()
    
    # ask for write
    message = {
        'file_name': file_name,
        'operation': 'c',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    
    response = client_socket.recv(1024).decode()
    print('oicnois')
    print(response)
    response = json.loads(response)
    status = response.get('status', 'error')
    message = response.get('message')
    print(message)
    if status == 'error':
        return False
    
    content = input('Please enter the content to write: ')
    message = {
        'content': content
    }
    data = json.dumps(message).encode()
    client_socket.send(data)

    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    status = response.get('status', 'error')
    message = response.get('message')
    print(message)

    return True if status == 'success' else False

def delete_file():
    # TODO: Implement this
    print("Deleting a file...")

def seek_file():
    # TODO: Implement this
    print("Deleting a file...")

def check_valid_input(file_name):
    return '.txt' in file_name

if __name__ == "__main__":
    try:
        while True:
            instructions()
            _input = input()
            if "<write>" in _input:
                while not check_valid_input(_input):
                    _input = input('Invalid input; please try using a valid name')

                file_name = _input.split()[1]
                response = write_file(file_name)

                if response:
                    print("File written successfully!")
                else:
                    print("Failed to write file.")

                print("Exiting <write> mode...\n")

            elif "<read>" in _input:
                while not check_valid_input(_input):
                    _input = input('Invalid input; please try using a valid name')

                file_name = _input.split()[1]
                response = read_file(file_name)
                print("Exiting <read> mode...\n")

            elif "<create>" in _input:
                while not check_valid_input(_input):
                    _input = input('Invalid input; please try using a valid name')

                file_name = _input.split()[1]
                response = create_file(file_name)
                print("Exiting <read> mode...\n")

            else:
                match _input:
                    case '<instructions>':
                        instructions()
                    case '<quit>':
                        print("Exiting the application...")
                        exit(0)
                    case _:
                        print('Invalid query. Please try again !!')
    except KeyboardInterrupt:
        print("\nExiting the application...")
