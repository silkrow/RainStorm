import requests
import random
from concurrent.futures import ThreadPoolExecutor

HTTP_PORT = "4444"
FILE_PATH_PREFIX = "../files/client/"

# List of server addresses to check
server_addresses = ["http://fa24-cs425-6801.cs.illinois.edu", 
                    "http://fa24-cs425-6802.cs.illinois.edu",
                    "http://fa24-cs425-6803.cs.illinois.edu", 
                    "http://fa24-cs425-6804.cs.illinois.edu",
                    "http://fa24-cs425-6805.cs.illinois.edu", 
                    "http://fa24-cs425-6806.cs.illinois.edu",
                    "http://fa24-cs425-6807.cs.illinois.edu", 
                    "http://fa24-cs425-6808.cs.illinois.edu",
                    "http://fa24-cs425-6809.cs.illinois.edu", 
                    "http://fa24-cs425-6810.cs.illinois.edu"]


def find_live_server():
    random_number = random.randint(1, 10)
    for i in range(10):
        address = server_addresses[(random_number + i)%10]
        url = f"{address}:" + HTTP_PORT
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                print(f"Connected to live server at {url}")
                return url
        except requests.RequestException as e:
            print(f"Searching alive server, could not connect to {url}")
    return None

def handle_user_input(user_input):
    parts = user_input.split()

    if len(parts) == 0:
        return True

    if parts[0] == 'exit':
        return False

    if parts[0] == "list_mem_ids" and len(parts) == 2:
        try:
            server_id = int(parts[1])
            if server_id > 10 or server_id < 1:
                print("Invalid server id!")
                return True
            address = server_addresses[server_id - 1] + ":" + HTTP_PORT
            response = requests.get(address, timeout=2)
            if response.status_code == 200:
                print(f"Connected to live server at {address}")
                response = requests.get(f"{address}/membership")
                print(f"membership received from {server_id}: {response.text}")

        except requests.RequestException as e:
            print(f"Could not connect to {address}: {e}")
        return True
    
    if parts[0] == 'online' and len(parts) == 2:
        try:
            server_id = int(parts[1])
            if server_id > 10 or server_id < 1:
                print("Invalid server id!")
                return True
            address = server_addresses[server_id - 1] + ":" + HTTP_PORT
            response = requests.get(address, timeout=2)
            if response.status_code == 200:
                print(f"Connected to live server at {address}")
                response = requests.get(f"{address}/online")
                print(f"Is server {server_id} online? {response.text}")

        except requests.RequestException as e:
            print(f"Could not connect to {address}: {e}")
        return True

    if parts[0] == 'store' and len(parts) == 1:
        live_server = find_live_server()
        if live_server:
            try:
                response = requests.get(live_server, timeout=2)
                if response.status_code == 200:
                    response = requests.get(f"{live_server}/store")
                    print(response.text)
            except requests.RequestException as e:
                print(f"Could not connect to {live_server}: {e}")
        else:
            print("No live servers available")
        return True


    if parts[0] == "create" and len(parts) == 3:  # dd if=/dev/urandom of=largefile.txt bs=1M count=100
                                                # Above is a good way of generating a large text file with random text.
        local, hydfs = parts[1], parts[2]
        
        live_server = find_live_server()
        if live_server:
            try:
                # Step 1: Request authorization to create the file
                data = {"local": local, "hydfs": hydfs}
                response = requests.post(f"{live_server}/create", json=data)
                
                if response.ok:
                    print("Authorization received from server")
                    
                    # Step 2: Send the actual file content
                    with open(FILE_PATH_PREFIX + local, 'rb') as f:
                        upload_response = requests.put(f"{live_server}/create?filename={hydfs}", data=f)
                    
                    if upload_response.ok:
                        print("File upload complete")
                    else:
                        print("File upload failed:", upload_response.text)
                else:
                    print("Authorization failed:", response.text)

            except requests.RequestException as e:
                print("Request to server failed:", e)
        else:
            print("No live servers available")
        return True

    if parts[0] == "append" and len(parts) == 3: 
        local, hydfs = parts[1], parts[2]
        
        live_server = find_live_server()
        if live_server:
            try:
                # Step 1: Request authorization to append the file
                data = {"local": local, "hydfs": hydfs}
                response = requests.post(f"{live_server}/append", json=data)
                
                if response.ok:
                    print("Authorization received from server")
                    
                    # Step 2: Send the actual file content
                    with open(FILE_PATH_PREFIX + local, 'rb') as f:
                        upload_response = requests.put(f"{live_server}/append?filename={hydfs}&num=0", data=f)
                    
                    if upload_response.ok:
                        print("File upload complete")
                    else:
                        print("File upload failed:", upload_response.text)
                else:
                    print("Authorization failed:", response.text)

            except requests.RequestException as e:
                print("Request to server failed:", e)
        else:
            print("No live servers available")
        return True

    if parts[0] == "multiappend" and len(parts) == 6:
        local_files = parts[1:5]
        hydfs = parts[5]

        live_server = find_live_server()
        if live_server:
            try:
                # Step 1: Request authorization for each file to append
                with ThreadPoolExecutor(max_workers=4) as executor:
                    auth_futures = {executor.submit(requests.post, f"{live_server}/append", json={"local": local, "hydfs": hydfs}): local for local in local_files}
                    
                    # Check each authorization response
                    for future in auth_futures:
                        local = auth_futures[future]
                        response = future.result()
                        if response.ok:
                            print(f"Authorization for {local} received from server")
                        else:
                            print(f"Authorization failed for {local}:", response.text)
                    
                    # Step 2: Upload the actual file contents concurrently
                    upload_futures = []
                    for i, local in enumerate(local_files):
                        with open(FILE_PATH_PREFIX + local, 'rb') as f:
                            upload_futures.append((local, requests.put(f"{live_server}/append?filename={hydfs}&num={i}", data=f)))

                    for local, upload_response in upload_futures:
                        if upload_response.ok:
                            print(f"File {local} upload complete")
                        else:
                            print(f"File {local} upload failed:", upload_response.text)

            except requests.RequestException as e:
                print("Request to server failed:", e)
        else:
            print("No live servers available")
        return True

    if parts[0] == "get" and len(parts) == 3:
        hydfs, local = parts[1], parts[2]
        
        live_server = find_live_server()
        if live_server:
            try:
                # Step 1: Request authorization to create the file
                data = {"local": local, "hydfs": hydfs}
                response = requests.get(f"{live_server}/get", json=data)

                if response.ok:
                    # Ensure correct encoding
                    response.encoding = 'utf-8'  
                    data = response.text
                    
                    # Step 2: Write content to the file with proper encoding
                    with open(FILE_PATH_PREFIX + local, 'w', encoding='utf-8') as f:
                        f.write(data)
                    print("File get successfully!")
                else:
                    print("Get file failed:", response.text)

            except requests.RequestException as e:
                print("Request to server failed:", e)
        else:
            print("No live servers available")
        return True


    if parts[0] == "getfromreplica" and len(parts) == 4:  
        vm_id, hydfs, local = parts[1], parts[2], parts[3]
        
        live_server = find_live_server()
        if live_server:
            try:
                # Step 1: Request authorization to create the file
                data = {"local": local, "hydfs": hydfs, "vm_add": server_addresses[int(vm_id)-1]}
                response = requests.get(f"{live_server}/getfromreplica", json=data)
                
                if response.ok:
                    # Step 2: Send the actual file content
                    with open(FILE_PATH_PREFIX + local, 'w') as f:
                        f.write(response.text)
                    print("File get successfully!")
                else:
                    print("Get file failed:", response.text)

            except requests.RequestException as e:
                print("Request to server failed:", e)
        else:
            print("No live servers available")
        return True

    if parts[0] == "ls" and len(parts) == 2:  
        hydfs = parts[1]
        
        live_server = find_live_server()
        if live_server:
            try:
                # Step 1: Request authorization to create the file
                response = requests.get(f"{live_server}/ls?filename={hydfs}")
                
                if response.ok:
                    # Step 2: Send the actual file content
                    print(response.text)
                else:
                    print("Ls file failed:", response.text)

            except requests.RequestException as e:
                print("Request to server failed:", e)
        else:
            print("No live servers available")
        return True

    if parts[0] == "merge" and len(parts) == 2:  
        filename = parts[1]
        
        live_server = find_live_server()
        if live_server:
            try:
                response = requests.get(f"{live_server}/merge?filename={filename}")
                
                if response.ok:
                    print("File merged successfully!")
                else:
                    print("Merge file failed:", response.text)

            except requests.RequestException as e:
                print("Request to server failed:", e)
        else:
            print("No live servers available")
        return True


    print("Not a valid command")
    # ...
    return True

def main():
    print("Enter 'exit' to quit.")
    while True:
        user_input = input("Enter command: ")
        if not handle_user_input(user_input):
            break

if __name__ == "__main__":
    main()
