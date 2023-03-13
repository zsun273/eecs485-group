"""Common usage.

This file is for code shared by the Manager and the Worker.
"""
import json
import socket


def send_tcp_message(host, port, message_dict):
    """Send customized tcp message."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        message = json.dumps(message_dict)
        sock.sendall(message.encode('utf-8'))


def recv_tcp_message(clientsocket):
    """Receive tcp message from client."""
    # Socket recv() will block for a maximum of 1 second.
    # If you omit this, it blocks indefinitely.
    # waiting for packets.
    clientsocket.settimeout(1)

    # Receive data, one chunk at a time.  If recv() times out
    # before we can read a chunk, then go back to the top of
    # the loop and try again.  When the client closes the
    # connection, recv() returns empty data, which breaks out
    # of the loop.  We make a simplifying assumption that the
    # client will always cleanly close the connection.
    with clientsocket:
        message_chunks = []
        while True:
            try:
                data = clientsocket.recv(4096)
            except socket.timeout:
                continue
            if not data:
                break
            message_chunks.append(data)

    # Decode list-of-byte-strings to UTF8 and parse JSON data
    message_bytes = b''.join(message_chunks)
    message_str = message_bytes.decode("utf-8")

    return json.loads(message_str)
