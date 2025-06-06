import socket
import threading

def handle_client(client_socket, address):
    print(f"New connection from {address}")
    while True:
        try:
            data = client_socket.recv(1024)
            if not data:
                break
            print(f"Received from {address}: {data.decode()}")
            client_socket.send(data)
        except:
            break
    print(f"Connection closed from {address}")
    client_socket.close()

def main():
    HOST = '0.0.0.0'
    PORT = 12345

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"Server listening on {HOST}:{PORT}")

    while True:
        try:
            client_socket, address = server.accept()
            client_handler = threading.Thread(
                target=handle_client,
                args=(client_socket, address)
            )
            client_handler.start()
        except KeyboardInterrupt:
            print("\nShutting down server...")
            break

    server.close()

if __name__ == "__main__":
    main()