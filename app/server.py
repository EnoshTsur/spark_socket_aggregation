import socket
import time
import json
import random
import csv

def batch_data():
    with open("data/Global Health Statistics.csv", mode="r") as file:
        reader = csv.DictReader(file)
        data = list(reader)
        return [data[i:i+5] for i in range(0, len(data), 5)]

def create_server():
    
    # Create server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('localhost', 9999))
    server_socket.listen(1)
    
    print("Waiting for Spark to connect...")
    
    try:
        while True:
            client_socket, address = server_socket.accept()
            print(f"Spark connected from {address}")

            data = batch_data()
            
            try:
                for batch in data:  # Continuously send data
                    
                    # Convert to JSON string and add newline
                    json_data = json.dumps(batch) + '\n'
                    print(f"Sending: {json_data.strip()}")
                    
                    # Send the data
                    client_socket.send(json_data.encode('utf-8'))
                    time.sleep(1)  # Wait 1 second between messages
                    
            except Exception as e:
                print(f"Error: {e}")
                client_socket.close()
                
    except KeyboardInterrupt:
        print("Shutting down server...")
    finally:
        server_socket.close()

if __name__ == "__main__":
    create_server()