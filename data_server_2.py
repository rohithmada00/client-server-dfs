from socket import *
import json
import time
from collections import defaultdict
from threading import Event, Thread
import os

PATH = 'replica_2/'
PORT = 11235 

class DataServer:
    FILE_PATH = f'{PATH}primaries.json'

    def __init__(self):
        self.load_primaries()

    def load_primaries(self):
        try:
            with open(self.FILE_PATH, 'r') as file:
                self.primaries = json.load(file)
        except FileNotFoundError:
            # If the file doesn't exist, initialize with a default value
            self.primaries = []

    def save_primaries(self):
        with open(self.FILE_PATH, 'w') as file:
            json.dump(self.primaries, file)

    def add_primary(self, file_name):
        self.primaries.append(file_name)
        self.save_primaries()

    def remove_primary(self, file_name):
        self.primaries = [p for p in self.primaries if p != file_name]
        self.save_primaries()

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
        message =  {'status': 'pending', 'message': f'Lease pending...'}

        if(conn is not None):
            conn.send(json.dumps(message).encode())

        self.grant_event.clear()  # Clear the event, indicating lease is not yet granted

        # Wait for the grant event to be set by another thread
        self.grant_event.wait()

        # Check the current lease for the file
        lease_end_time, current_client_id = self.leases.get(file_path, (0, None))
        if client_id == current_client_id:
            # Now the lease should be granted
            return {'status': 'success', 'message': f'Lease granted for {lease_duration} seconds'}
        
        return {'status': 'error', 'message': f'Lease not granted for {file_path}'}

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
    
    def free_leases(self):
        for file_name, _ in self.leases.items():
            self.free_file_leases(file_name)
            
    def free_file_leases(self, file_name):
        file_lease_queue = self.lease_queue[file_name]
        for _ in file_lease_queue:
            _, _ = self.lease_queue[file_name].pop(0)
            self.grant_event.set()
        self.leases = defaultdict()


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

def fail_server(data_server: DataServer, lease_manager: LeaseManager, conn: socket):
    print('server is about to fail')
    # TODO: fail all lease grantings/ pendings
    lease_manager.free_leases()

    # inform data_server about failures
    ns_conn = contact_name_server()
    message = {
        'operation' : 'update_primaries',
        'content' : {
                        'files': data_server.primaries
                    }
    }
    message = json.dumps(message).encode()
    ns_conn.send(message)

    print(f'message sent to name server {message}')
    response = ns_conn.recv(1024).decode()
    response = json.loads(response)
    print(f'updating primaries result is {response}')
    ns_conn.close()

    response = {'status': 'success', 'message': 'Added to primaries successfully...'}
    conn.send(json.dumps(response).encode())
    print('exit application..')
    exit(0)

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
        server_socket.close()

        if data is None:
            print(f'File {file_name} does not exist in the file system')
            return {'status' : 'error', 'message' : 'File does not exist in the system...'}

        else:
            # contact primary
            primary_server = data['primary_server']
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
  

def seek_file_locally(file_name, seek_index):
    try:
        with open(f'{PATH}{file_name}', 'r') as file:
            file.seek(seek_index)
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

def seek_file_globally(file_name, seek_index):
    try:
        server_socket = contact_name_server()
        # get info about primary
        message = {'operation': 'get_metadata', 'file_path': file_name}
        message = json.dumps(message).encode()
        server_socket.send(message)
        response = server_socket.recv(1024).decode()
        response = json.loads(response)
        data = response.get('content')
        server_socket.close()

        if data is None:
            print(f'File {file_name} does not exist in the file system')
            return {'status' : 'error', 'message' : 'File does not exist in the system...'}

        else:
            # contact primary
            primary_server = data['primary_server']
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
            return seek_file_locally(file_name, seek_index)
    except Exception as e:
        print(f'Error fetching {file_name}: {e}')
        return {'status' : 'error', 'message' : f'Error fetching {file_name} from the data server: {e}'}

def list_files():
    # get info about primary
    server_socket = contact_name_server()
    message = {'operation': 'list_files'}
    message = json.dumps(message).encode()
    server_socket.send(message)
    response = server_socket.recv(1024).decode()
    response = json.loads(response)
    return response


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

        server_socket = contact_name_server()
        message = {'operation': 'get_metadata', 'file_path': file_name}
        message = json.dumps(message).encode()
        server_socket.send(message)
        response = server_socket.recv(1024).decode()
        response = json.loads(response)
        data = response.get('content')
        replicas = data['replicas']
        server_socket.close()

        replicate(file_name, replicas)
        return { 'status': 'success', 'message': f'successfully written to file'}
    except Exception as e:
        return { 'status': 'error', 'message': f'exception writing to file{e}'}

def write_file_globally(file_name, lease_duration, conn :socket):
    try:
        # get info about primary
        server_socket = contact_name_server()
        message = {'operation': 'get_metadata', 'file_path': file_name}
        message = json.dumps(message).encode()
        server_socket.send(message)
        response = server_socket.recv(1024).decode()
        response = json.loads(response)
        data = response.get('content')

        if data is None:
            print(f'File {file_name} does not exist in the file system')
            return {'status' : 'error', 'message' : 'File does not exist in the system...'}
        
        primary_server = data['primary_server']
        replicas = data['replicas']
        latest_commit_id = data['latest_commit_id']
        server_socket.close()

        if primary_server is  None:
            print(f'File {file_name} does not exist in the file system')
            return {'status' : 'error', 'message' : 'File does not exist in the system...'}
        else:
            # TODO: contact primary to request lease
            print('trying to lease...')
            message = {'file_name': file_name, 'operation': 'lease', 'lease_duration': 120}
            message = json.dumps(message).encode()
            server_socket = contact_data_server(port=primary_server)
            server_socket.send(message)
            encoded_response = server_socket.recv(1024)
            print(f'lease response - {encoded_response}')
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
                encoded_response = server_socket.recv(1024)
                response = encoded_response.decode()
                response = json.loads(response)
                status = response.get('status', 'error')
                message = response.get('message')

            if(status == 'success'):
                conn.send(encoded_response)
                print('need to get content...')
                response = conn.recv(1024).decode()
                print(f'recieved content is {response}')
                response = json.loads(response)
                content = response.get('content')

                # save locally
                print(f' file created or updated at.. {PATH}{file_name}')
                with open(f'{PATH}{file_name}', 'w') as file:
                    file.write(content)
                    file.close()

                # replicate
                print(f'primary is {primary_server}')
                print(f'replicas are {replicas}')
                response = replicate(file_name, [primary_server]+replicas)
                print(f'replication result is {response}')
                status = response.get('status', 'error')
                message = response.get('message')

                if PORT not in replicas:
                    # update nameserver with updated metadata
                    ns_conn = contact_name_server()
                    message = {
                        'file_path' : file_name,
                        'operation' : 'update_metadata',
                        'content' : {
                                        "file_path": file_name,
                                        "primary_server": primary_server,
                                        "replicas": replicas+[PORT],
                                        "latest_commit_id": str(int(latest_commit_id)+1) if latest_commit_id is not None else latest_commit_id
                                    }
                    }
                message = json.dumps(message).encode()
                ns_conn.send(message)
                print(f'message sent to name server {message}')
                response = ns_conn.recv(1024).decode()
                response = json.loads(response)
                status = response.get('status')
                print(f'updating metadata result is {response}')
                ns_conn.close()
                
                if(status != 'success'):
                    print('Error writing file')
                    return {'status' : 'error', 'message' : 'Error writing file...'}
                else:
                    print('Successfully wrote file')
                    return {'status' : 'success', 'message' : 'Successfully wrote file...'}

    except Exception as e:
        print(f'Error writing {file_name}: {e}')
        return {'status' : 'error', 'message' : f'Error writing {file_name} to the data server: {e}'}
    
def create_file(file_name, conn: socket):
    try:
        # update nameserver with updated metadata
        ns_conn = contact_name_server()
        message = {
            "file_path": file_name,
            'operation': 'create_file',
            'content' : {
                'primary': str(PORT)
            }
        }
        message = json.dumps(message).encode()
        ns_conn.send(message)
        response = ns_conn.recv(1024).decode()
        response = json.loads(response)
        print(f'response from nameserver: {response}')
        status = response.get('status', 'error')

        if status == 'error':
            print('aido')
            print(f'File {file_name} already exists in the file system')
            return  {'status' : 'error', 'message' : f'{file_name} already exists in file system'}

        data = response.get('content')
        replicas = data.get('replicas')
        ns_conn.close()

        if status == 'success':
            # this server will be the files primary
            message = {'status' : 'success', 'message' : f'{file_name} can be created.. please enter the content'}
            print(message)
            message = json.dumps(message).encode()
            conn.send(message)
            # take input 
            print('got input from client')
            response = conn.recv(1024).decode()
            response = json.loads(response)
            content = response.get('content')
            # save locally
            with open(f'{PATH}{file_name}', 'w') as file:
                    file.write(content)
                    file.close()
            print(f'starting replication - {replicas}')
            response = replicate(file_name, replicas)
            print(response)

            return {'status': 'success', 'message': 'File created successfully..'}

        else:
            print(f'File {file_name} already exists in the file system')
            return  {'status' : 'error', 'message' : f'{file_name} already exists in file system: {e}'}
            
    except Exception as e:
        print(f'Error creating {file_name}: {e}')
        return {'status' : 'error', 'message' : f'Error creating {file_name} to the data server: {e}'}

def replicate(file_name, replicas):
    file = open(f'{PATH}/{file_name}', 'r')
    content = file.read()
    file.close()

    message = {'file_name': file_name, 'operation': 'rep', 'content': content}

    for replica in replicas:
        # Donot replicate to itself
        print(f'replica is {replica} port is {PORT}')
        if str(replica) == str(PORT):
            print(f'this server is also a replica ... not replicating')
            continue
        try:
            server_socket = contact_data_server(port=int(replica))
            server_socket.send(json.dumps(message).encode())
            response = server_socket.recv(1024).decode()
            response = json.loads(response)
            status = response.get('status')
            server_socket.close()
            print(f'response from {replica}')
            print(message)

            # in pessimistic replication every replica should be consistant
            # failure in doing so leads to failed operation
            if status != 'success':
                return response
        except Exception as e:
            return {'status': 'error', 'message': f'failed to replicate due to {e}'}
        
    return {'status': 'success', 'message': 'file replicated successfully..'}
    
        
def save(file_name,  content):
    with open(f'{PATH}{file_name}', 'w') as file:
            file.write(content)
    print(file_name + " successfully replicated...\n")
    return {'status': 'success', 'message':'file replicated successfully..'}

def delete_file_locally(file_name):
    try:
        os.remove(f'{PATH}{file_name}')
        print(f'{file_name} successfully deleted.')
        return {'status': 'success', 'message': 'File deleted successfully.'}
    except FileNotFoundError:
        print(f'{file_name} not found locally')
        return {'status': 'error', 'message': 'File not found.'}
    except Exception as e:
        print(f'Error deleting {file_name}: {e}')
        return {'status': 'error', 'message': 'Error deleting file.'}
    
def delete_file_globally(file_name, lease_manager: LeaseManager, requestee):
    # Get metadata
    # Contact primary to delete globally if its not primary
    # If it is primary then delete locally and send request to other replicas

    # get info about primary
    server_socket = contact_name_server()
    message = {'operation': 'get_metadata', 'file_path': file_name}
    message = json.dumps(message).encode()
    server_socket.send(message)
    response = server_socket.recv(1024).decode()
    response = json.loads(response)
    data = response.get('content')

    if data is None:
        print(f'File {file_name} does not exist in the file system')
        return {'status' : 'error', 'message' : 'File does not exist in the system...'}
    
    primary_server = data['primary_server']
    replicas = data['replicas']
    # latest_commit_id = data['latest_commit_id']
    server_socket.close()

    # check if the current server is primary
    if str(PORT) == primary_server:
        response = lease_manager.request_lease(file_name, 'current', None, 120)
        if response['status'] != 'success':
            print(f"Error leasing :{file_name}")
            return {'status': 'error', 'message': f'Error deleting file {file_name}'}
        for replica in replicas:
            # ignore if requestee is a replica
            if replica == requestee:
                continue
            server_socket = contact_data_server(port=int(replica))
            message = {'file_name': file_name, 'operation': 'delete_locally'}
            server_socket.send(json.dumps(message).encode())
            response = server_socket.recv(1024).decode()
            response = json.loads(response)
            if response['status'] != 'success':
                print(f'Error deleting file from replica {replica}: {response["message"]}')
                return response
        response = delete_file_locally(file_name)
        # free pending leases
        lease_manager.free_file_leases(file_name)
        # remove metadata at the name server
        # get info about primary
        server_socket = contact_name_server()
        message = {'operation': 'delete_metadata', 'file_path': file_name}
        message = json.dumps(message).encode()
        server_socket.send(message)
        response = server_socket.recv(1024).decode()
        response = json.loads(response)
        print(f'response to deleting metadata : {response}')
    else:
        server_socket = contact_data_server(port=int(primary_server))
        message = {'file_name': file_name, 'operation': 'delete_globally'}
        server_socket.send(json.dumps(message).encode())
        response = server_socket.recv(1024).decode()
        response = json.loads(response)
        if response['status'] == 'success':
            delete_file_locally(file_name)
        return response

    return {'status': 'success', 'message': 'File deleted globally.'}

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
        content = client_message.get('content', '')
        lease_duration = client_message.get('lease_duration', 120)

        print(f'file name {file_name}, operation {operation}, content {content}')

        match operation:
            case 'r':
                response = read_file_locally(file_name) 
                # if not in local
                if response['status'] == 'error':
                    response = read_file_globally(file_name)
            case 'w':
                response = write_file_locally(file_name, conn, lease_manager) if file_name in data_server.primaries else write_file_globally(file_name, lease_duration, conn)
            case 'c':
                response = create_file(file_name, conn)
                if(response['status'] == 'success'):
                    data_server.add_primary(file_name)
            case 'rep':
                response = save(file_name, content)
            case 'lease':
                print(f'Lease requested by {addr[0] + str(addr[1])}')
                response = lease_manager.request_lease(file_name,str(addr[1]), conn, lease_duration)
            case 'add_primary':
                response = data_server.add_primary(file_name)
                response = {'status': 'success', 'message': 'Added to primaries successfully...'}
            case 'fail_server':
                fail_server(data_server, lease_manager, conn)
            case 'delete_locally':
                response = delete_file_locally(file_name)
            case 'delete_globally':
                response = delete_file_globally(file_name, lease_manager, str(addr[1]))
                print(f'global deletion response {response}')
                if response['status'] == 'success':
                    data_server.remove_primary(file_name)
            case 'list_files':
                response = list_files()
            case 'seek_files':
                seek_index = content['seek_index']
                response = seek_file_locally(file_name, seek_index) 
                if response['status'] == 'error':
                    response = seek_file_globally(file_name, seek_index)
            case _:
                print('Invalid operation. Please try again !!')

        conn.send(json.dumps(response).encode())
        conn.close()

if __name__ == '__main__':
    main()
