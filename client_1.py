from socket import *
import json
import random
import time

HOST = "127.0.0.1"
PORTS = ['11234']
MAX_RETRIES = 3  
RETRY_INTERVAL = 2

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
    if client_socket is None:
        print('Unable to connect to any server to operate...')
        return False
    
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
    for _ in range(MAX_RETRIES):
        port = random.choice(PORTS)
        print(f'Connecting to random server on port {port}...')
        client_socket = socket(AF_INET, SOCK_STREAM)

        try:
            client_socket.connect((HOST, int(port)))
            return client_socket  # Successful connection, return the socket
        except Exception as e:
            print(f"Error connecting to server on port {port}: {e}")
            time.sleep(RETRY_INTERVAL)  # Wait before retrying

    print(f"Failed to connect to any server after {MAX_RETRIES} attempts.")
    return None  # Return None to indicate failure

def write_file(file_name):
    client_socket = contact_random_server()

    if client_socket is None:
        print('Unable to connect to any server to operate...')
        return False
    
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
    if client_socket is None:
        print('Unable to connect to any server to operate...')
        return False
    
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

def delete_file(file_name):
    client_socket = contact_random_server()
    if client_socket is None:
        print('Unable to connect to any server to operate...')
        return False
    
    # ask for read
    message = {
        'file_name': file_name,
        'operation': 'delete_globally',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    
    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    print(f'Received response from server...')
    status = response.get('status', 'error')
    client_socket.close()

    if status == 'success':
        content = response.get('content', '###')
        print(f'File content:\n{content}')
        return True

    return False

def seek_file(file_name, seek_index):
    client_socket = contact_random_server()
    if client_socket is None:
        print('Unable to connect to any server to operate...')
        return False
    
    # ask for seek
    message = {
        'file_name': file_name,
        'operation': 'seek_files',
        'content' :{
            'seek_index' : seek_index
        }
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    
    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    print(f'Received response from server...')
    status = response.get('status', 'error')
    client_socket.close()

    if status == 'success':
        content = response.get('content', '###')
        print(f'File content:\n{content}')
        return True

    return False


def fail_server():
    client_socket = contact_random_server()
    message = {
        'operation': 'fail_server',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)

    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    message = response.get('message')
    print(message)
    return True

def list_files():
    print('1')
    client_socket = contact_random_server()
    if client_socket is None:
        print('Unable to connect to any server to operate...')
        return False
    print('2')
    # ask for read
    message = {
        'operation': 'list_files',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    print('3')
    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    print(f'Received response from server...')
    status = response.get('status', 'error')

    if status == 'success':
        content = response.get('content', {})
        files_list = content.get('files_list', [])
        print(f'Files list:\n{files_list}')

    client_socket.close()



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
            
            elif "<seek>" in _input:
                while not check_valid_input(_input):
                    _input = input('Invalid input; please try using a valid name')

                file_name = _input.split()[1]
                seek_index = _input.split()[2]

                try:
                    seek_index = int(seek_index)
                except ValueError:
                    print("Please enter an integer index for seeking the file.")
                    print("Exiting <write> mode...\n")
                    continue
                response = seek_file(file_name, seek_index)
                if response:
                    print("File seek successful!")
                else:
                    print("Failed to seek file.")
            
            elif "<list>" in _input:
                response = list_files()
                print("Exiting <list> mode...\n")

            elif "<create>" in _input:
                while not check_valid_input(_input):
                    _input = input('Invalid input; please try using a valid name')

                file_name = _input.split()[1]
                response = create_file(file_name)
                print("Exiting <read> mode...\n")

            elif "<delete>" in _input:
                while not check_valid_input(_input):
                    _input = input('Invalid input; please try using a valid name')

                file_name = _input.split()[1]
                response = delete_file(file_name)
                print("Exiting <read> mode...\n")

            elif '<fail>' in _input:
                response = fail_server()
                print("Exiting <fail> mode...\n")

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
