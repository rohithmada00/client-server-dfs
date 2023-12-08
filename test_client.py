from socket import *
import json
import random
import time
import matplotlib.pyplot as plt
import threading

HOST = "127.0.0.1"
PORTS = ['11234', '11235', '11236', '11237', '11238']
MAX_RETRIES = 3  
RETRY_INTERVAL = 2

def read_file(file_name):
    client_socket = contact_random_server()
    if client_socket is None:
        return {
            'status': 'error',
            'message': 'Could not connect to any server'
        }
    
    # ask for read
    message = {
        'file_name': file_name,
        'operation': 'r',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    
    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    client_socket.close()
    return response

def contact_random_server():
    for _ in range(MAX_RETRIES):
        port = random.choice(PORTS)
        print(f'Connecting to random server on port {port}...')
        client_socket = socket(AF_INET, SOCK_STREAM)

        try:
            client_socket.connect((HOST, int(port)))
            return client_socket  # Successful connection, return the socket
        except Exception as e:
            print(f"Error connecting to server on port {port}: {e}")
            time.sleep(RETRY_INTERVAL)  # Wait before retrying

    print(f"Failed to connect to any server after {MAX_RETRIES} attempts.")
    return None  # Return None to indicate failure

def contact_server(port = '11234'):
    for _ in range(MAX_RETRIES):
        print(f'Connecting to random server on port {port}...')
        client_socket = socket(AF_INET, SOCK_STREAM)

        try:
            client_socket.connect((HOST, int(port)))
            return client_socket  # Successful connection, return the socket
        except Exception as e:
            print(f"Error connecting to server on port {port}: {e}")
            time.sleep(RETRY_INTERVAL)  # Wait before retrying

    print(f"Failed to connect to any server after {MAX_RETRIES} attempts.")
    return None

def write_file(file_name, content=None):
    client_socket = contact_random_server()

    if client_socket is None:
        return {
            'status': 'error',
            'message': 'Could not connect to any server'
        }
    
    # ask for write
    message = {
        'file_name': file_name,
        'operation': 'w',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    
    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    status = response.get('status', 'error')
    message = response.get('message')

    # if pending wait for another message
    if status == 'pending':
        print(f'Received response from server...')
        print(message)
        
        # Receive another message
        response = client_socket.recv(1024).decode()
        response = json.loads(response)
        status = response.get('status', 'error')
        message = response.get('message')

    # if success take input and send
    if status == 'success':
        print(f'Received response from server...')
        print(message)
        
        print('Sending file contents to server...')
        if content is None:
            content = input('Please enter the content to write: ')

        message = {
            'content': content
        }
        data = json.dumps(message).encode()
        client_socket.send(data)

        response = client_socket.recv(1024).decode()
        response = json.loads(response)

    print(f'Received response from server...')
    print(response)
    client_socket.close()
    return response

def create_file(file_name, content=None):
    client_socket = contact_random_server()
    if client_socket is None:
        return {
            'status': 'error',
            'message': 'Could not connect to any server'
        }
    
    # ask for write
    message = {
        'file_name': file_name,
        'operation': 'c',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    
    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    status = response.get('status', 'error')
    message = response.get('message')
    if status == 'error':
        return response
    
    if content is None:
        content = input('Please enter the content to write: ')
    message = {
        'content': content
    }
    data = json.dumps(message).encode()
    client_socket.send(data)

    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    return response

def delete_file(file_name):
    client_socket = contact_random_server()
    if client_socket is None:
        return {
            'status': 'error',
            'message': 'Could not connect to any server'
        }
    
    # ask for read
    message = {
        'file_name': file_name,
        'operation': 'delete_globally',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    
    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    print(f'Received response from server...')
    status = response.get('status', 'error')
    client_socket.close()

    return response

def seek_file(file_name, seek_index):
    client_socket = contact_random_server()
    if client_socket is None:
        return {
            'status': 'error',
            'message': 'Could not connect to any server'
        }
    
    # ask for seek
    message = {
        'file_name': file_name,
        'operation': 'seek_files',
        'content' :{
            'seek_index' : seek_index
        }
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    
    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    print(f'Received response from server...')
    status = response.get('status', 'error')
    client_socket.close()

    return response


def fail_server(port = None):
    if port is None:
        client_socket = contact_random_server()
    else:
        client_socket = contact_server(port)

    if client_socket is None:
        return {
            'status': 'error',
            'message': 'Could not connect to any server'
        }
    
    message = {
        'operation': 'fail_server',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)

    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    return response

def list_files():
    client_socket = contact_random_server()
    if client_socket is None:
        return {
            'status': 'error',
            'message': 'Could not connect to any server'
        }

    # ask for read
    message = {
        'operation': 'list_files',
    }
    data = json.dumps(message).encode()
    client_socket.send(data)
    response = client_socket.recv(1024).decode()
    response = json.loads(response)
    print(f'Received response from server...')
    status = response.get('status', 'error')

    if status == 'success':
        content = response.get('content', {})
        files_list = content.get('files_list', [])
        print(f'Files list:\n{files_list}')

    client_socket.close()
    return response

#####################################################

def test_create_read_delete():
    create_response = create_file('test_file_2.txt', 'testing hello world !!!')
    print(f'Create response: {create_response}')
    assert create_response['status'] == 'success'
    time.sleep(1)
    read_response = read_file('test_file_2.txt')
    print(f'Read response: {read_response}')
    assert read_response['content'] == 'testing hello world !!!'
    time.sleep(1)
    delete_response = delete_file('test_file_2.txt')
    print(f'Delete response: {delete_response}')
    assert delete_response['status'] == 'success'
    time.sleep(1)
    read_response = read_file('test_file_2.txt')
    assert read_response['status'] == 'error' 

def test_file_seek():
    create_response = create_file('test_file_1.txt', 'testing hello world !!!')
    print(f'Create response: {create_response}')
    assert create_response['status'] == 'success'
    time.sleep(1)
    seek_response = seek_file('test_file_1.txt', 8)
    print(f'Seek response: {seek_response}')
    assert seek_response['content'] == 'hello world !!!'
    time.sleep(1)
    delete_response = delete_file('test_file_1.txt')
    assert delete_response['status'] == 'success'
    print(f'Delete response: {delete_response}')
    time.sleep(1)
    read_response = read_file('test_file_1.txt')
    assert read_response['status'] == 'error' 

def test_file_write():
    create_response = create_file('test_file_1.txt', 'testing hello world !!!')
    print(f'Create response: {create_response}')
    assert create_response['status'] == 'success'
    write_response = write_file('test_file_1.txt', 'write testing !!!')
    print(f'write response: {write_response}')
    assert write_response['content'] == 'write testing !!!'
    delete_response = delete_file('test_file_1.txt')
    assert delete_response['status'] == 'success'
    print(f'Delete response: {delete_response}')
    read_response = read_file('test_file_1.txt')
    assert read_response['status'] == 'error'

def test_server_disconnection():
    port = random.choice(PORTS)
    fail_response = fail_server(port)
    assert fail_response['status'] == 'success'
    server_socket = contact_server(port)
    assert server_socket == None

def perform_test_and_plot():
    timings = {}

    # Test: Create File
    start_time = time.time()
    create_response = create_file('test_file.txt', 'Sample content')
    end_time = time.time()
    timings['Create'] = end_time - start_time

    # Test: Read File
    start_time = time.time()
    read_response = read_file('test_file.txt')
    end_time = time.time()
    timings['Read'] = end_time - start_time

    # Test: Write File
    start_time = time.time()
    write_response = write_file('test_file.txt', 'Updated content')
    end_time = time.time()
    timings['Write'] = end_time - start_time

    # Test: Delete File
    start_time = time.time()
    delete_response = delete_file('test_file.txt')
    end_time = time.time()
    timings['Delete'] = end_time - start_time

    # Plotting the results
    operations = list(timings.keys())
    times = [timings[op] for op in operations]

    plt.figure(figsize=(10, 5))
    plt.bar(operations, times, color='blue')
    plt.xlabel('Operation')
    plt.ylabel('Time (seconds)')
    plt.title('Response Times for File Operations')
    plt.show()

    return timings

def serial_write_test(file_name, content, num_writes):
    start_time = time.time()
    for _ in range(num_writes):
        write_file(file_name, content)  
    end_time = time.time()
    return end_time-start_time

def parallel_write_test(file_name, content, num_writes):
    start_time = time.time()
    threads = []
    for i in range(num_writes):
        thread = threading.Thread(target=write_file, args=(f"{file_name}_{i}", content))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    end_time = time.time()
    return end_time-start_time

def plot_same_file_multi_writes():
    # Initialize lists to store the timings
    serial_timings = []
    parallel_timings = []
    write_counts = [1, 2, 3, 4,]  # The counts of writes you want to test
    create_response = create_file("test_file.txt")
    assert create_response['status']=='success'
    for count in write_counts:
        # Run Serial Test
        serial_time = serial_write_test("test_file.txt", "Sample Content", count)
        serial_timings.append(serial_time)

        # Run Parallel Test
        parallel_time = parallel_write_test("test_file.txt", "Sample Content", count)
        parallel_timings.append(parallel_time)

    # Plotting the results
    plt.figure(figsize=(10, 5))
    plt.plot(write_counts, serial_timings, label='Serial Writing', marker='o')
    plt.plot(write_counts, parallel_timings, label='Parallel Writing', marker='o')
    plt.xlabel('Number of Writes')
    plt.ylabel('Time (seconds)')
    plt.title('Serial vs Parallel Writing Performance')
    plt.legend()
    plt.grid(True)
    plt.show()

# TODO: optimistic replication
def test_file_consistency():
    pass


if __name__ == "__main__":
    print('starting test client')
    test_create_read_delete()
    time.sleep(1)
    test_file_seek()
    # plot_same_file_multi_writes()