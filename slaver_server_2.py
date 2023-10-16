from socket import *

PATH = 'replica_2/'

def start_server():
    HOST = 'localhost'
    PORT = 12347  
    server_socket = socket(AF_INET,SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(10)
    print ('File server is up...')
    return server_socket

def read_file(file_name):
    try:
        with open(f'{PATH}{file_name}', 'r') as file:
            content = file.read()
            print('file read')
            return ('SUCCESS', content)
    except IOError:
        print(f'{file_name} doesnot exist')
        return ('FAILURE', 'File doesnot exist in system...')

def write_file(file_name, message):
    with open(f'{PATH}{file_name}', 'w') as file:
            file.write(message)
    return ('SUCCESS', 'File written successfully...')

def replicate(file_name,  content):
    file = open(file_name, 'w')
    file.write(content)
    file.close()
    print(file_name + " successfully replicated...\n")

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
            case 'rep':
                response = replicate(file_name, message)
            case _:
                print('Invalid operation.Please try again !!')
        
        conn.send(response[1].encode())

if __name__ == '__main__':
    main()
 