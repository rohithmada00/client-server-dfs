from socket import *
import json
import time
from collections import defaultdict
from threading import Event, Thread

PATH = 'replica_1/'

class DataServer:
    def __init__(self):
        self.primaries = ['doodle.txt',]
        # self.version_vectors = defaultdict(dict)

class LeaseManager:
    def __init__(self, expiration_check_interval=10):
        # Dictionary to store lease info: {file_path: (lease_end_time, client_id)}
        self.leases = defaultdict()
        self.lease_queue = defaultdict(list)
        self.expiration_check_interval = expiration_check_interval
        self.grant_event = Event()

        # Start the expiration check thread
        expiration_check_thread = Thread(target=self.periodic_lease_expiration_check)
        expiration_check_thread.daemon = True
        expiration_check_thread.start()

    def request_lease(self, file_path, client_id, conn: socket, lease_duration=120):
        # For self leasing 
        # set client_id to current, conn to None

        current_time = time.time()
        lease_end_time, existing_client = self.leases.get(file_path, (0, None))

        if current_time > lease_end_time or existing_client == client_id:
            self.leases[file_path] = (current_time + lease_duration, client_id)
            return {'status': 'success', 'message': f'Lease granted for {lease_duration} seconds'}

        self.lease_queue[file_path].append((client_id, conn))
        message =  {'status': 'pending', 'message': f'Lease granted for {lease_duration} seconds'}

        if(conn is not None):
            conn.send(json.dumps(message).encode())

        self.grant_event.clear()  # Clear the event, indicating lease is not yet granted

        # Wait for the grant event to be set by another thread
        self.grant_event.wait()

        # Now the lease should be granted
        return {'status': 'success', 'message': f'Lease granted for {lease_duration} seconds'}

    def check_lease(self, file_path, client_id):
        lease_end_time, lease_client_id = self.leases.get(file_path, (0, None))
        current_time = time.time()

        if current_time <= lease_end_time and lease_client_id == client_id:
            return {'status': 'success', 'message': 'Valid lease'}

        return {'status': 'error', 'message': f'Lease expired or not held by client {client_id}'}

    def release_lease(self, file_path, client_id):
        _, lease_client_id = self.leases.get(file_path, (0, None))

        if lease_client_id == client_id:
            del self.leases[file_path]  # Remove the lease entry
            return {'status': 'success', 'message': 'Lease released'}

        return {'status': 'error', 'message': 'Invalid operation'}

    def periodic_lease_expiration_check(self):
        while True:
            expired_files = self.get_expired_leases()
            self.process_expired_leases(expired_files)
            time.sleep(self.expiration_check_interval)

    def get_expired_leases(self):
        current_time = time.time()
        expired_files = []

        for file_path, (lease_end_time, _) in self.leases.items():
            if current_time > lease_end_time:
                expired_files.append(file_path)

        return expired_files

    def process_expired_leases(self, expired_files):
        for file_path in expired_files:
            # Perform actions for expired leases (e.g., notify clients, release resources)
            del self.leases[file_path]

            # Grant the lease to the next client in the queue, if any
            if self.lease_queue[file_path]:
                next_client, _ = self.lease_queue[file_path].pop(0)
                self.leases[file_path] = (time.time() + 120, next_client)
                self.grant_event.set()  # Set the event to signal that the lease is granted to the next client

def start_server():
    HOST = 'localhost'
    PORT = 11234  
    server_socket = socket(AF_INET,SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(10)
    print ('File server is up...')
    return server_socket

def contact_name_server():
    print(f'Connecting to name server...')
    client_socket = socket(AF_INET, SOCK_STREAM)
    client_socket.connect(('localhost', 12345))
    return client_socket

def contact_data_server(port, host = 'localhost'):
    print(f'Connecting to data server...')
    client_socket = socket(AF_INET, SOCK_STREAM)
    client_socket.connect((host, int(port)))
    return client_socket

def read_file_locally(file_name):
    try:
        with open(f'{PATH}{file_name}', 'r') as file:
            content = file.read()
            print('file read')
            response = {
                'status' : 'success',
                'content' : content
            }
            return response
    except IOError:
        print(f'{file_name} does not exist')
        return {'status' : 'error', 'message' : 'File does not exist in the system...'}

def read_file_globally(file_name):
    try:
        server_socket = contact_name_server()
        # get info about primary
        message = {'operation': 'get_metadata', 'file_path': file_name}
        message = json.dumps(message).encode()
        server_socket.send(message)
        response = server_socket.recv(1024).decode()
        response = json.loads(response)
        data = response.get('content')
        primary_server = data['primary_server']
        server_socket.close()

        if primary_server is None:
            print(f'File {file_name} does not exist in the file system')
            return {'status' : 'error', 'message' : 'File does not exist in the system...'}

        else:
            # contact primary
            server_socket = contact_data_server(int(primary_server))
            message = {'file_name': file_name, 'operation': 'r'}
            message = json.dumps(message).encode()
            server_socket.send(message)
            response = server_socket.recv(1024).decode()
            response = json.loads(response)
            content = response.get('content')
            print('File read successfully')
            # make a local copy
            with open(f'{PATH}{file_name}', 'w') as file:
                file.write(content)
            # return local copy
            return read_file_locally(file_name)
    except Exception as e:
        print(f'Error fetching {file_name}: {e}')
        return {'status' : 'error', 'message' : f'Error fetching {file_name} from the data server: {e}'}
  

def write_file_locally(file_name, conn: socket, lease_manager: LeaseManager):
    try:
        response = lease_manager.request_lease(file_name, 'current', None, 120)
        print(f'self leasing response{response}')

        message = json.dumps(response).encode()
        conn.send(message)

        response = conn.recv(1024).decode()
        response = json.loads(response)

        content = response.get('content')

        with open(f'{PATH}{file_name}', 'w') as file:
            file.write(content)

        # TODO: Replicate
        # replicate(file_name, replicas)
        return { 'status': 'success', 'message': f'successfully written to file'}
    except Exception as e:
        return { 'status': 'error', 'message': f'exception writing to file{e}'}

def write_file_globally(file_name, message, lease_duration, conn :socket):
    try:
        # get info about primary
        server_socket = contact_name_server()
        message = {'operation': 'get_metadata', 'file_path': file_name}
        message = json.dumps(message).encode()
        server_socket.send(message)
        response = server_socket.recv(1024).decode()
        response = json.loads(response)
        data = response.get('content')
        primary_server = data['primary_server']
        server_socket.close()

        if primary_server is None:
            print(f'File {file_name} does not exist in the file system')
            return {'status' : 'error', 'message' : 'File does not exist in the system...'}

        else:
            # TODO: contact primary to request lease
            message = {'file_name': file_name, 'operation': 'lease', 'lease_duration': 120}
            message = json.dumps(message).encode()
            server_socket = contact_data_server(port=primary_server)
            encoded_response = server_socket.recv(1024)
            response = encoded_response.decode()
            response = json.loads(response)
            status = response.get('status', '')
            message = response.get('message')
            # TODO: message client to wait / write

            if(status == 'pending'):
                # message client to wait
                print('lease pending...')
                conn.send(encoded_response)
                
                # Receive another message
                response = server_socket.recv(1024).decode()
                response = json.loads(response)
                status = response.get('status', 'error')
                message = response.get('message')

            if(status == 'success'):
                response = server_socket.recv(1024).decode()
                response = json.loads(response)
                content = response.get('content')

                message = {'file_name': file_name, 'operation': 'w', 'content': content}
                message = json.dumps(message).encode()
                server_socket.send(message)
                response = server_socket.recv(1024).decode()
                response = json.loads(response)
                status = response.get('status', 'error')
                message = response.get('message')
                if(status != 'success'):
                    print('Error writing file')
                    return {'status' : 'error', 'message' : 'Error writing file...'}
                else:
                    print('Successfully wrote file')
                    return {'status' : 'success', 'message' : 'Successfully wrote file...'}

    except Exception as e:
        print(f'Error writing {file_name}: {e}')
        return ('FAILURE', f'Error writing {file_name} to the data server: {e}')
    
# def create_file(file_name):
#     try:
#         # get info about primary
#         message = {'operation': 'get_metadata', 'file_path': file_name}
#         message = json.dumps(message).encode()
#         response = message_to_server('localhost', 12345, message)
#         primary_server = response.get('primary_server')

#         if primary_server is None:
#             print(f'File {file_name} does not exist in the file system')
#             return ('FAILURE', 'File does not exist in the system...')

#         else:
#             # contact primary
#             message = {'file_name': file_name, 'operation': 'r'}
#             message = json.dumps(message).encode()
#             response = message_to_server('localhost', primary_server, message)

#             print('File read successful')
#             return response['message']  # Return content from the response

#     except Exception as e:
#         print(f'Error fetching {file_name}: {e}')
#         return ('FAILURE', f'Error fetching {file_name} from the data server: {e}')

def replicate(file_name, replicas):
    file = open(f'files/{file_name}', 'r')
    content = file.read()
    file.close()

    message = {'file_name': file_name, 'operation': 'rep', 'content': content}

    # TODO: Replicate
    # for replica in replicas:
    #     message_to_server('localhost', replica, message)

def replicate(file_name,  content):
    with open(f'{PATH}{file_name}', 'w') as file:
            file.write(content)
    print(file_name + " successfully replicated...\n")
    return ('SUCCESS', 'File replicated successfully...')

def main():
    data_server = DataServer()
    server = start_server()
    lease_manager = LeaseManager()
    while True:
        response = ''
        conn, addr = server.accept()
        print(f'connected to {conn} {addr}...')

        client_message = conn.recv(1024).decode()
        client_message = json.loads(client_message)

        print(f'client message - {client_message}')

        file_name = client_message.get('file_name', '')
        operation = client_message.get('operation', '')
        message = client_message.get('message', '')
        lease_duration = client_message.get('lease_duration', 120)

        print(f'file name {file_name}, operation {operation}, message {message}')

        match operation:
            case 'r':
                response = read_file_locally(file_name) if file_name in data_server.primaries else read_file_globally(file_name)
            case 'w':
                response = write_file_locally(file_name, conn, lease_manager) if file_name in data_server.primaries else write_file_globally(file_name, message, lease_duration, conn)
            case 'rep':
                response = replicate(file_name, message)
            case 'lease':
                lease_manager.request_lease(file_name, addr[0] + str(addr[1]), conn, lease_duration)
                response = ('SUCCESS', 'Lease granted')
            case _:
                print('Invalid operation. Please try again !!')

        conn.send(json.dumps(response).encode())

if __name__ == '__main__':
    main()
