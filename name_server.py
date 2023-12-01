import json
from socket import *
import sqlite3
import threading
import time

class MasterServer:
    def __init__(self):
        self.available_servers = ['11234', '11235', '11236', ]
        self.server_check_interval = 60
        # self.server_check_thread = threading.Thread(target=self.periodic_server_check)
        # self.server_check_thread.daemon = True
        # self.server_check_thread.start()

    def periodic_server_check(self):
        while True:
            for server_port in self.available_servers:
                self.connect_to_server('localhost', int(server_port))

            time.sleep(self.server_check_interval)

    def connect_to_server(self, host, port):
        try:
            with socket(AF_INET, SOCK_STREAM) as client_socket:
                client_socket.connect((host, port))
                print(f"Connected to server at {host}:{port}")
        except Exception as e:
            print(f"Error connecting to server at {host}:{port}: {e}")

           
def start_server_listener(data_service, master_server, port):
    HOST = 'localhost'
    PORT = port
    server_socket = socket(AF_INET, SOCK_STREAM)
    server_socket.bind((HOST, int(PORT)))
    server_socket.listen(10)
    print('Master server is up...')
    try:
        while True:
            conn, addr = server_socket.accept()
            print(f'Connected to {addr}...')
            threading.Thread(target=handle_client, args=(conn, addr, data_service, master_server)).start()
          
    except KeyboardInterrupt:
        print("\nServer shutting down...")

    finally:
        server_socket.close()

class DataService:

    def get_record_as_a_dict(self, record: list):
        return {
            "file_path": record[0],
            "primary_server": record[1],
            "replicas": json.loads(record[2]),
            "latest_commit_id": record[3] if len(record) > 3 else None
        }
    
    def get_metadata(self, file_path):
        conn = sqlite3.connect('dfs.db')
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM metadata WHERE file_path=?", (file_path,))
        result = cursor.fetchone()
        conn.close()

        if result is None:
            return {'status': 'success', 'message': 'No metadata available', 'content': None}

        return {'status': 'success', 'message': 'Metadata available', 'content': self.get_record_as_a_dict(result)}


    def update_metadata(self, file_path, primary_server, replicas, latest_commit_id):
        try:
            conn = sqlite3.connect('dfs.db')
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO metadata (file_path, primary_server, replicas, latest_commit_id)
                VALUES (?, ?, ?, ?)
            ''', (file_path, primary_server, json.dumps(replicas), latest_commit_id))
            conn.commit()
            conn.close()
            return {'status': 'success'}
        except Exception as e:
            print(f"Error updating metadata: {e}")
            return {'status': 'error', 'message': str(e)}

    def create_file(self, file_path, master_server: MasterServer, conn=None):
        try:
            metadata = self.get_metadata(file_path)

            if metadata is None:
                # File doesn't exist, proceed to create
                if conn is not None:
                    primary_server = str(conn[1])
                else:
                    primary_server, replicas = master_server.select_servers()

                # Add metadata to the database
                self.update_metadata(file_path, primary_server, replicas, '0')

                return {
                    'status': 'success',
                    'message': 'File created successfully',
                    'file_info': {
                        'file_path': file_path,
                        'primary_server': primary_server,
                        'replicas': replicas,
                        'latest_commit_id': '0'
                    }
                }
            else:
                # File already exists, return an error
                return {'status': 'error', 'message': 'File already exists'}

        except Exception as e:
            print(f"Error creating file: {e}")
            return {'status': 'error', 'message': str(e)}


def handle_client(conn: socket, addr, data_service: DataService, master_server):
    client_message = conn.recv(1024)
    client_message = client_message.decode()

    print(f'Client message - {client_message}')
    client_message = json.loads(client_message)

    file_path = client_message.get('file_path', '')
    operation = client_message.get('operation', '')
    content = client_message.get('content', {})
    server_id = addr

    print(f'File path: {file_path}, Operation: {operation}, Server id: {server_id}')

    response = None
    match operation:
        case 'create_file':
            response = data_service.create_file(file_path, master_server, addr)
        case 'get_metadata':
            response = data_service.get_metadata(file_path)
        case 'update_metadata':
            primary_server = content.get('primary_server', '')
            replicas = content.get('replicas', [])
            latest_commit_id = content.get('latest_commit_id', '')
            response = data_service.update_metadata(file_path, primary_server, replicas, latest_commit_id)
        case _:
            print('Invalid operation. Please try again !!')
            response = {'status': 'error', 'message': 'Invalid operation'}

    print(f'## {response}')
    conn.send(json.dumps(response).encode())
    conn.close()


def main():
    # sleeping for maximum lease time for avoiding inconsistencies after a crash
    # time.sleep(120)
    # Start Server

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
    conn.close()
    
    master_server = MasterServer()
    data_service = DataService()
    data_service.update_metadata('sks.txt', '11235',[], None)

    start_server_listener(data_service, master_server, '12345')


if __name__ == '__main__':
    main()
