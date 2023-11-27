import json
from socket import *
import sqlite3

class MasterServer:
    def __init__(self):
        self.available_servers = ["11234", "11235", "11236", '11237', '11238']

    def select_servers(self):
        # Round robin for server selection
        selected_primary = self.available_servers.pop(0)
        selected_replicas = self.available_servers[:2]
        self.available_servers.append(selected_primary)  # Move primary to the end for the next round
        return selected_primary, selected_replicas

def start_server():
    HOST = 'localhost'
    PORT = 12345
    server_socket = socket(AF_INET, SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(10)
    print('Master server is up...')
    return server_socket

def get_metadata(file_path):
    connection = sqlite3.connect("dfs.db")
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM metadata WHERE file_path=?", (file_path,))
    result = cursor.fetchone()
    connection.close()
    return result

def update_metadata(file_path, primary_server, replicas, latest_commit_id):
    try:
        connection = sqlite3.connect("dfs.db")
        cursor = connection.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO metadata (file_path, primary_server, replicas, latest_commit_id)
            VALUES (?, ?, ?, ?)
        ''', (file_path, primary_server, json.dumps(replicas), latest_commit_id))
        connection.commit()
        connection.close()
        return {'status': 'success'}
    except Exception as e:
        print(f"Error updating metadata: {e}")
        return {'status': 'error', 'message': str(e)}

def create_file(file_path, master_server):
    try:
        if(get_metadata(file_path) is None):
            primary_server, replicas = master_server.select_servers()
            # Update metadata in the database
            update_metadata(file_path, primary_server, replicas, '')

            return {'status': 'success', 'primary_server': primary_server, 'replicas': replicas, 'latest_commit_id': 0}
        else:
            return {'status': 'error', 'message': 'File already exists'}
    except Exception as e:
        print(f"Error creating file: {e}")
        return {'status': 'error', 'message': str(e)}

def main():
    conn = sqlite3.connect('dfs.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            file_path TEXT PRIMARY KEY,
            primary_server TEXT,
            replicas TEXT,
            latest_commit_id TEXT
        )
    ''')
    master_server = MasterServer()
    server = start_server()

    try:
        while True:
            conn, addr = server.accept()
            print(f'Connected to {addr}...')

            client_message = conn.recv(1024)
            client_message = client_message.decode()

            print(f'Client message - {client_message}')
            client_message = json.loads(client_message)

            file_path = client_message.get('file_path', '')
            operation = client_message.get('operation', '')
            message = client_message.get('message', {})

            print(f'File path: {file_path}, Operation: {operation}, Message: {message}')

            response = None
            match operation:
                case 'create_file':
                    response = create_file(file_path, master_server)
                case 'get_metadata':
                    response = get_metadata(file_path)
                case 'update_metadata':
                    primary_server = message.get('primary_server', '')
                    replicas = message.get('replicas', [])
                    latest_commit_id = message.get('latest_commit_id', '')
                    response = update_metadata(file_path, primary_server, replicas, latest_commit_id)
                case _:
                    print('Invalid operation. Please try again !!')
                    response = {'status': 'error', 'message': 'Invalid operation'}

            if response is not None:
                conn.send(json.dumps(response).encode())

    except KeyboardInterrupt:
        print("\nServer shutting down...")

    finally:
        conn.close()

if __name__ == '__main__':
    main()
