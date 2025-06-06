import socket

def main():
    HOST = '127.0.0.1'
    PORT = 12345

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        client.connect((HOST, PORT))
        print(f"Connected to {HOST}:{PORT}")

        while True:
            message = input("Enter message (or 'quit' to exit): ")
            if message.lower() == 'quit':
                break

            client.send(message.encode())
            
            data = client.recv(1024)
            print(f"Received from server: {data.decode()}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()
        print("Connection closed")

if __name__ == "__main__":
    main()