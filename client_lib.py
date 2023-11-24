import os

def cache(file_name, file_data, operation):
    path = f'./cache/{file_name}'

    os.makedirs(os.path.dirname(path), exist_ok=True)

    if(operation == 'w'):
        file = open(path, operation)
        file.write(file_data)
    else:
        file = open(path, 'r')
        print(f'content from cache for file {file_name}')
        print(file.read())

def in_cache(file_path):
    path = f'./cache/{file_path}'
    return os.path.exists(path)


