import json
from socket import *
import sqlite3

'''

    Its the main server
    Every client contacts this server
    It stores metadata about all the file servers
    Client can request info about files

    {
        file_name : 'file_name',
        primary_server : 'primary_server',
        replicas : 'replicas',
        latest_commit_id : 'commit_id',
    }

'''


def start_server():
    HOST = 'localhost'
    PORT = 12345  
    server_socket = socket(AF_INET,SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(10)
    print ('File server is up...')
    return server_socket

def get_metadata():
    connection = sqlite3.connect("metadata.db")
    cur = connection.cursor()
    res = cur.execute("SELECT name FROM sqlite_master WHERE name='spam'")
    pass

def update_metadata():
    try:
        connection = sqlite3.connect("metadata.db")
        cur = connection.cursor()
        # Store metadata in the database
        cur.execute('''
            INSERT OR REPLACE INTO metadata (file_path, primary_server, replicas)
            VALUES (?, ?, ?)
        ''', (file_path, primary_server, replicas))

        # Commit the changes to the database
        connection.commit()
        return True
    except:
        return False

def main():
    conn = sqlite3.connect('dfs.db')
    cursor = conn.cursor()

    # Create a table to store metadata if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            file_path TEXT PRIMARY KEY,
            primary_server TEXT,
            replicas TEXT,
            latest_commit_id TEXT,
        )
    ''')

    server = start_server()
    conn, addr = server.accept()
    while(True):
        response = ''
        if(conn == None):
            conn, addr = server.accept()
        print(f'connected to {conn} {addr}...')

        client_message = conn.recv(1024)
        client_message = client_message.decode()

        print(f'client message - {client_message}')
        client_message = json.loads(client_message)

        file_name =  client_message.split("|")[0]
        operation =  client_message.split("|")[1]
        message =  client_message.split("|")[2]

        print(f'file name {file_name} , operation {operation}, message {message}')

        match operation:
            case 'get_metadata':
                response = get_metadata(file_name)
            case 'update_metadata':
                response = update_metadata(file_name, message)
            case _:
                print('Invalid operation.Please try again !!')
        
        conn.send(response[1].encode())

if __name__ == '__main__':
    main()
 