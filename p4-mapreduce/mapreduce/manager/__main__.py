"""MapReduce framework Manager node."""
import os
import socket
import tempfile
import logging
import json
import threading
import time
import click
import mapreduce.utils

# Configure logging
LOGGER = logging.getLogger(__name__)


def server_tcp(signals, host, port):
    """Wait on a message from a socket OR a shutdown signal."""
    LOGGER.info("Start TCP server thread")

    # Create an INET, STREAMing socket, this is TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        workers = []

        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        LOGGER.debug("TCP bind %s:%s",
                     host, port, )
        sock.listen()

        # Socket accept() will block for a maximum of 1 second.  If you
        # omit this, it blocks indefinitely, waiting for a connection.
        sock.settimeout(1)

        while not signals["shutdown"]:
            # print("TCP waiting ...")

            # Wait for a connection for 1s.  The socket library avoids
            # consuming CPU while waiting for a connection.
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            print("Connection from", address[0])

            # Socket recv() will block for a maximum of 1 second.  If you omit
            # this, it blocks indefinitely, waiting for packets.
            clientsocket.settimeout(1)

            # Receive data, one chunk at a time.  If recv() times out before
            # we can read a chunk, then go back to the top of the loop and try
            # again.  When the client closes the connection, recv() returns
            # empty data, which breaks out of the loop.  We make a simplifying
            # assumption that the client will always cleanly close the
            # connection.
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

            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue
            print("TCP msg: ", message_dict)

            # shutdown when receive special shutdown message
            if message_dict.get('message_type', "") == "shutdown":
                # forward msg to all workers
                shut_thread = threading.Thread(target=shut_workers(workers))
                shut_thread.start()
                shut_thread.join()
                time.sleep(1)  # make sure worker shuts
                print("========== WORKERS ALL SHUTDOWN ===============")

                signals['shutdown'] = True
            elif message_dict.get('message_type', "") == "register":
                LOGGER.debug("TCP recv \n%s",
                             json.dumps(message_dict, indent=2), )
                new_worker = {'worker_host': message_dict['worker_host'],
                              'worker_port': message_dict['worker_port'],
                              'state': "ready"}
                workers.append(new_worker)

                # send back ACK
                ack_thread = threading.Thread(
                    target=ack,
                    args=(message_dict['worker_host'],
                          message_dict['worker_port']))
                ack_thread.start()
                ack_thread.join()
                print("================= ACK SENT ===============")

    print("server TCP shutting down")


def server_udp(signals, host, port):
    """Wait on a message from a socket OR a shutdown signal."""
    LOGGER.info("Start UDP server thread")
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Bind the UDP socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        LOGGER.debug("UDP bind %s:%s",
                     host, port,)
        sock.settimeout(1)

        # Receive incoming UDP messages
        while not signals["shutdown"]:
            # print("UDP waiting ...")
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue

            message_str = message_bytes.decode("utf-8")
            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue

            LOGGER.debug("UDP recv \n%s",
                         json.dumps(message_dict, indent=2), )

    print("server UDP shutting down")


def ack(host, port):
    """Send ACK message back to workers."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # connect to the worker
        sock.connect((host, port))

        message_dict = {
            "message_type": "register_ack",
            "worker_host": host,
            "worker_port": port,
        }
        # send a message
        message = json.dumps(message_dict)
        sock.sendall(message.encode('utf-8'))
        LOGGER.debug("TCP send to %s:%s \n%s",
                     host, port, json.dumps(message_dict, indent=2), )


def shut_workers(workers):
    """Shut down workers."""
    for worker in workers:
        if worker['state'] != "dead":
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                host, port = worker['worker_host'], worker['worker_port']
                # connect to the worker
                sock.connect((host, port))

                message_dict = {"message_type": "shutdown"}
                # send shutdown message
                message = json.dumps(message_dict)
                sock.sendall(message.encode('utf-8'))


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s",
            host, port,
        )

        LOGGER.info(
            "PWD %s",
            os.getcwd(),
        )

        # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        signals = {"shutdown": False}
        udp_thread = threading.Thread(target=server_udp,
                                      args=(signals, host, port))
        udp_thread.start()
        time.sleep(1)
        server_tcp(signals, host, port)

        udp_thread.join()  # Wait for server thread to shut down


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)
