from socket import *
from collections import defaultdict

HOST = "127.0.0.1" 
PORT = 12367 

def check_status_file(filename, filename_locked_status:dict):
    if(filename not in filename_locked_status.keys()):
        filename_locked_status['filename'] = 'unlocked'
    return filename_locked_status.get(filename, "unlocked") == "unlocked"

def main():
    s = socket(AF_INET, SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(10)
    print('Locking Service is ready to receive...')

    filename_locked_status = {}
    filename_clients_status = defaultdict(list)
    client_timeout_status = defaultdict(int)

    while True:
        connection_socket, address = s.accept()
        try:
            msg_received = connection_socket.recv(1024).decode()
            client_id, command, filename =  msg_received.split('|')

            if command == "LOCK":
                unlocked = check_status_file(filename, filename_locked_status)
                if unlocked:
                    if not filename_clients_status[filename]:  
                        filename_locked_status[filename] = "locked"
                        allow_message = "file granted"
                        connection_socket.send(allow_message.encode())
                    else:
                        if filename_clients_status[filename][0] == client_id:
                            filename_clients_status[filename].pop(0)  
                            filename_locked_status[filename] = "locked"
                            allow_message = "file granted"
                            connection_socket.send(allow_message.encode())
                else:
                    if client_id not in filename_clients_status[filename]:
                        filename_clients_status[filename].append(client_id)
                    client_timeout_status[client_id] += 1
                    if client_timeout_status[client_id] >= 100:
                        filename_clients_status[filename].remove(client_id)
                        del client_timeout_status[client_id]
                        message_timeout = "TIMEOUT"
                        connection_socket.send(message_timeout.encode())
                    else:
                        allow_message = "file not granted"
                        connection_socket.send(allow_message.encode())

            elif command == "UNLOCK":
                if filename_locked_status.get(filename) == "locked":
                    filename_locked_status[filename] = "unlocked"
                    if filename_clients_status[filename]:
                        new_client = filename_clients_status[filename].pop(0)
                        client_timeout_status[new_client] = 0  
                    unlock_message = "file unlocked"
                    connection_socket.send(unlock_message.encode())

        except Exception as e:
            print(f"An exception occurred: {e}")
        finally:
            connection_socket.close()

if __name__ == "__main__":
    main()
