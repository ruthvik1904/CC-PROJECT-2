import socket
import time
import random
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def start_server(host='localhost', port=9999):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.bind((host, port))
        server_socket.listen(1)
        logging.info(f"Server started and listening on {host}:{port}")

        conn, address = server_socket.accept()
        logging.info(f"Connected to {address}")

        while True:
            # Generate a random transaction
            product_id = random.randint(1, 100)
            quantity = random.randint(1, 20)    # Random quantity
            price = round(random.uniform(5.0, 100.0), 2)  # Random price
            timestamp = int(time.time())
            transaction_record = f"{product_id},{quantity},{price},{timestamp}\n"

            # Send the transaction to the consumer
            conn.sendall(transaction_record.encode())
            time.sleep(1)  # Send one transaction per second
    except KeyboardInterrupt:
        logging.info("Transaction data stream stopped by user")
    finally:
        conn.close()
        server_socket.close()

if __name__ == "__main__":
    start_server()
