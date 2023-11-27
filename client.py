import socket
import time
import json
import client_lib as client_lib

HOST = "127.0.0.1"
# PORT = 12345
MASTER_SERVER_HOST = "127.0.0.1"
MASTER_SERVER_PORT = 12345  # Replace with the actual port of the master server

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
    # Get information about primary server and replicas from the master server
    server_info = contact_master_server(file_name)

    if server_info:
        primary_server = server_info['primary_server']
        replicas = server_info['replicas']

        # Now you can proceed with reading from the primary server or replicas
        # Connect to primary server or one of the replicas and fetch the file content
        # ...

        return "Read operation completed successfully"
    else:
        return "Failed to get server information"

def contact_master_server(file_name, operation = 'get_server_info'):
    print('connecting to master server...')
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_socket:
        master_socket.connect((MASTER_SERVER_HOST, MASTER_SERVER_PORT))
        message = {'operation': operation, 'file_path': file_name}
        master_socket.send(json.dumps(message).encode())
        response = master_socket.recv(1024).decode()
        print(f'master servers response {response}')
        return json.loads(response)

def lock_unlock_file(client_id, filename, lock_or_unlock):
    serverName = 'localhost'
    serverPort = 12367  # port of the directory service
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((serverName, serverPort))
        msg = {'client_id': client_id, 'command': lock_or_unlock, 'file_name': filename}
        s.send(json.dumps(msg).encode())
        reply = s.recv(1024).decode()
    print(f'lock_server reply {reply}')
    return reply

def write_file(file_name):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        grant_lock = lock_unlock_file('client', file_name, "LOCK")

        while grant_lock != "file granted":
            print("File not granted, polling again...")
            grant_lock = lock_unlock_file('client', file_name, "LOCK")

            if grant_lock == "TIMEOUT":
                print("File locking timed out, please try again later...")
                return False

            time.sleep(0.1)

        print("You are granted the file...")
        print('connecting to master server....')

        # Get information about primary server and replicas from the master server
        server_info = contact_master_server(file_name, 'create_file')
        primary_server = server_info['primary_server']
        replicas = server_info['replicas']
        print(f'bla bla {primary_server} {replicas}')

        if primary_server is not None and replicas is not None:
            try:
                client_socket.connect((HOST, primary_server))
            except:
                print(f"Couldn't connect to primary server at {HOST}:{primary_server}")
                return False          
            
            # writing part
            content = input('Please enter the content to write: ')
            message = {'file_name': file_name, 'operation': 'write', 'content': content}
            client_socket.send(json.dumps(message).encode())
            response = client_socket.recv(1024).decode()
            print(f'response after writing to fs {response}')

            # unlocking part
            reply_unlock = lock_unlock_file('client2', file_name, "UNLOCK")
            print(reply_unlock)

            return True
        else:
            return "Failed to get server information"

    except Exception as e:
        print(f"Error: {e}")
        return False

    finally:
        client_socket.close()

def list_files():
    raise NotImplementedError

def delete_file():
    raise NotImplementedError

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
                # check in cache
                if client_lib.in_cache(file_name):
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
                    case '<quit>':
                        print("Exiting the application...")
                        exit(0)
                    case _:
                        print('Invalid query. Please try again !!')
    except KeyboardInterrupt:
        print("\nExiting the application...")
