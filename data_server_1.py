from socket import *
import json
import time
from collections import defaultdict
from threading import Event, Thread
import os

PATH = 'replica_1/'
PORT = 11234 

class DataServer:
    FILE_PATH_PRIMARIES = f'{PATH}primaries.json'
    FILE_PATH_VERSION_VECTORS = f'{PATH}version_vectors.json'

    def __init__(self):
        self.load_primaries()
        self.load_version_vectors()

    def load_primaries(self):
        try:
            with open(self.FILE_PATH_PRIMARIES, 'r') as file:
                self.primaries = json.load(file)
        except FileNotFoundError:
            # If the file doesn't exist, initialize with a default value
            self.primaries = []

    def load_version_vectors(self):
        try:
            with open(self.FILE_PATH_VERSION_VECTORS, 'r') as file:
                self.version_vectors = json.load(file)
        except FileNotFoundError:
            # If the file doesn't exist, initialize with an empty dictionary
            self.version_vectors = {}

    def save_primaries(self):
        with open(self.FILE_PATH_PRIMARIES, 'w') as file:
            json.dump(self.primaries, file)

    def save_version_vectors(self):
        with open(self.FILE_PATH_VERSION_VECTORS, 'w') as file:
            json.dump(self.version_vectors, file)

    def add_primary(self, file_name, version_vector=None):
        self.primaries.append(file_name)
        self.version_vectors[file_name] = version_vector if version_vector else {}
        self.save_primaries()
        self.save_version_vectors()

    def remove_primary(self, file_name):
        self.primaries = [p for p in self.primaries if p != file_name]
        del self.version_vectors[file_name]
        self.save_primaries()
        self.save_version_vectors()

    def get_version_vector(self, file_name):
        return self.version_vectors.get(file_name, {})

    def update_version_vector(self, file_name, new_version_vector):
        self.version_vectors[file_name] = new_version_vector
        self.save_version_vectors()

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

def fail_server(data_server: DataServer, conn: socket):
    print('server is about to fail')

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


def write_file_locally(file_name, conn: socket):
    try:
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
        if replica == PORT:
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
        print(f'{file_name} not found.')
        return {'status': 'error', 'message': 'File not found.'}
    except Exception as e:
        print(f'Error deleting {file_name}: {e}')
        return {'status': 'error', 'message': 'Error deleting file.'}
    
def delete_file_globally(file_name):
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
        for replica in replicas:
            server_socket = contact_data_server(port=int(replica))
            message = {'file_name': file_name, 'operation': 'delete_locally'}
            server_socket.send(json.dumps(message).encode())
            response = server_socket.recv(1024).decode()
            response = json.loads(response)
            if response['status'] != 'success':
                print(f'Error deleting file from replica {replica}: {response["message"]}')
                return response
        response = delete_file_locally(file_name)

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
        return response

    return {'status': 'success', 'message': 'File deleted globally.'}

def main():
    data_server = DataServer()
    server = start_server()
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

        print(f'file name {file_name}, operation {operation}, content {content}')

        match operation:
            case 'r':
                response = read_file_locally(file_name) if file_name in data_server.primaries else read_file_globally(file_name)
            case 'w':
                response = write_file_locally(file_name, conn) 
            case 'c':
                response = create_file(file_name, conn)
                if(response['status'] == 'success'):
                    data_server.add_primary(file_name)
            case 'rep':
                response = save(file_name, content)
            case 'add_primary':
                response = data_server.add_primary(file_name)
                response = {'status': 'success', 'message': 'Added to primaries successfully...'}
            case 'fail_server':
                fail_server(data_server, conn)
            case 'delete_locally':
                response = delete_file_locally(file_name)
            case 'delete_globally':
                response = delete_file_globally(file_name)
                print(f'global deletion response {response}')
                if response['status'] == 'success':
                    data_server.remove_primary(file_name)
            case 'list_files':
                response = list_files()
            case 'seek_files':
                seek_index = content['seek_index']
                response = seek_file_locally(file_name, seek_index) if file_name in data_server.primaries else seek_file_globally(file_name, seek_index)
            case _:
                print('Invalid operation. Please try again !!')

        conn.send(json.dumps(response).encode())
        conn.close()

if __name__ == '__main__':
    main()
