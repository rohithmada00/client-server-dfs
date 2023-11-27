from socket import *
import os

def start_server():
    HOST = 'localhost'
    PORT = 12345  
    server_socket = socket(AF_INET,SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(10)
    print ('File server is up...')
    return server_socket

def read_file(file_name):
    try:
        with open(f'files/{file_name}', 'r') as file:
            content = file.read()
            print('file read')
            return ('SUCCESS', content)
    except IOError:
        print(f'{file_name} doesnot exist')
        return ('FAILURE', 'File doesnot exist in system...')

def write_file(file_name, message):
    with open(f'files/{file_name}', 'w') as file:
            file.write(message)
    replicate(file_name)
    return ('SUCCESS', 'File written successfully...')

def message_to_server(ip_addr, port, message):
	print("Replicating to fileserver {ip_addr} {port}")
	server_socket = socket(AF_INET, SOCK_STREAM)
	server_socket.connect((ip_addr,port))
	server_socket.send(message.encode())
	server_socket.close()

def replicate(file_name):
    file = open(f'files/{file_name}', 'r')
    content = file.read()
    file.close()

    message = file_name + '|' + 'rep' + '|' + content

    # replicate to slave server 1
    message_to_server('localhost', 12346, message)

    # replicate to slave server 2
    message_to_server('localhost', 12347, message)

def main():
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

        file_name =  client_message.split("|")[0]
        operation =  client_message.split("|")[1]
        message =  client_message.split("|")[2]

        print(f'file name {file_name} , operation {operation}, message {message}')

        match operation:
            case 'r':
                response = read_file(file_name)
            case 'w':
                response = write_file(file_name, message)
            case _:
                print('Invalid operation.Please try again !!')
            
        
        conn.send(response[1].encode())

if __name__ == '__main__':
    main()
 