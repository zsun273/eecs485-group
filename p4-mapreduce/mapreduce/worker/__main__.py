"""MapReduce framework Worker node."""
import os
import logging
import json
import socket
import threading
from threading import Lock
import click
# import mapreduce.utils

# Configure logging
LOGGER = logging.getLogger(__name__)
L = Lock()


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )
        self.host, self.port = host, port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.signals = {"shutdown": False}

        self.worker_tcp()
        # registration(host, port, manager_host, manager_port)

    def worker_tcp(self):
        """Wait on a message from a socket OR a shutdown signal."""
        LOGGER.info("Start TCP server thread")
        registered = False
        udp_running = False

        # Create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the worker
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            LOGGER.debug("TCP bind %s:%s",
                         self.host, self.port, )
            sock.settimeout(1)

            while not self.signals["shutdown"]:
                # print("worker TCP waiting ...")

                L.acquire()
                if not registered:
                    self.registration()
                    registered = True
                L.release()

                # Wait for a connection for 1s.  The socket library avoids
                # consuming CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])

                clientsocket.settimeout(1)

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
                # print("worker TCP msg: ", message_dict)

                # shutdown when receive special shutdown message
                if message_dict.get('message_type', "") == "shutdown":
                    self.signals['shutdown'] = True
                elif message_dict.get('message_type', "") == "register_ack":
                    LOGGER.debug("TCP recv \n%s",
                                 json.dumps(message_dict, indent=2), )
                    udp_thread = threading.Thread(
                        target=self.worker_udp)
                    udp_thread.start()
                    udp_running = True

        if udp_running:
            udp_thread.join()

        print("worker TCP shutting down")

    def registration(self):
        """Send registration message to Manager."""
        # Create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((self.manager_host, self.manager_port))

            # send a message
            message_dict = {
                "message_type": "register",
                "worker_host": self.host,
                "worker_port": self.port,
            }
            message = json.dumps(message_dict)
            sock.sendall(message.encode('utf-8'))
            LOGGER.debug("TCP send to %s:%s \n%s",
                         self.manager_host, self.manager_port,
                         json.dumps(message_dict, indent=2), )
            LOGGER.info(
                "Sent connection request to Manager %s:%s",
                self.manager_host, self.manager_port,
            )

    def worker_udp(self):
        """Send heartbeat or wait for a shutdown signal."""
        # while not self.signals["shutdown"]:
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to the UDP socket on server
            sock.connect((self.manager_host, self.manager_port))

            # Send a message
            message = json.dumps({"message_type": "heartbeat"})
            sock.sendall(message.encode('utf-8'))
            # time.sleep(10)

        print("worker UDP shutting down")


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)
