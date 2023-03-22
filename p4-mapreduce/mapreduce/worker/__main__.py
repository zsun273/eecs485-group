"""MapReduce framework Worker node."""
import heapq
import hashlib
import os
import logging
import json
import pathlib
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from threading import Lock
from contextlib import ExitStack
import click
from mapreduce import utils


# Configure logging
LOGGER = logging.getLogger(__name__)
L = Lock()


def worker_map(executable, input_path, num_partitions,
               output, task_id):
    """Map job."""
    with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
        LOGGER.info("Created tmpdir %s", tmpdir)
        output_files = [pathlib.PurePath(
                            tmpdir,
                            f"maptask{task_id:05d}-part"
                            f"{partition_number:05d}")
                        for partition_number in range(num_partitions)]
        with ExitStack() as stack:
            files = [stack.enter_context(open(filename, 'a', encoding="utf-8"))
                     for filename in output_files]
            for filename in input_path:
                with open(filename, encoding="utf-8") as infile:
                    with subprocess.Popen(
                            [executable],
                            stdin=infile,
                            stdout=subprocess.PIPE,
                            text=True,
                    ) as map_process:
                        LOGGER.info("Executed %s", executable)
                        for line in map_process.stdout:
                            # Add line to correct partition output file
                            files[(int(hashlib.
                                       md5(line.split("\t")[0].
                                           encode("utf-8")).
                                       hexdigest(),
                                       base=16) % num_partitions)].write(line)
        # sort lines and
        # move files to managers tmp folder
        for filename in os.listdir(pathlib.Path(tmpdir)):
            with open(pathlib.Path(tmpdir, filename), 'r',
                      encoding="utf-8") as file:
                lines = sorted(file)
            with open(pathlib.Path(tmpdir, filename), 'w',
                      encoding="utf-8") as file:
                for line in lines:
                    file.write(line)
            LOGGER.info("Sorted %s", filename)
            shutil.move(pathlib.Path(tmpdir, filename),
                        pathlib.Path(output, filename))
            LOGGER.info("Moved %s", filename)


def worker_reduce(executable, input_path, output, task_id):
    """Reduce job."""
    with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
        LOGGER.info("Created tmpdir %s", tmpdir)
        with ExitStack() as stack:
            files = [stack.enter_context(open(fname, encoding="utf-8"))
                     for fname in input_path]
            instream = heapq.merge(*files)
            filename = pathlib.PurePath(tmpdir, f"part-{task_id:05d}")
            with open(filename, 'a', encoding="utf-8") as outfile:
                with subprocess.Popen(
                    [executable],
                    text=True,
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                ) as reduce_process:
                    LOGGER.info("Executed %s", executable)
                    # Pipe input to reduce_process
                    for line in instream:
                        reduce_process.stdin.write(line)

        # move file to output folder
        for filename in os.listdir(pathlib.Path(tmpdir)):
            shutil.move(pathlib.Path(tmpdir, filename),
                        pathlib.Path(output, filename))
            LOGGER.info("Moved %s", filename)


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

        self.udp_thread = threading.Thread(
            target=self.worker_udp)
        self.worker_tcp()

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
                # LOGGER.info("worker TCP waiting ...")
                time.sleep(0.1)
                with L:
                    if not registered:
                        self.registration()
                        registered = True

                # Wait for a connection for 1s.  The socket library avoids
                # consuming CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                LOGGER.info("Connection from: %s", address[0])

                try:
                    message_dict = utils.recv_tcp_message(clientsocket)
                except json.JSONDecodeError:
                    continue
                LOGGER.debug("Worker TCP recv \n%s",
                             json.dumps(message_dict, indent=2), )

                if message_dict.get('message_type', "") == "register_ack":
                    self.udp_thread.start()
                    udp_running = True
                elif message_dict.get('message_type', "") == "shutdown":
                    self.signals['shutdown'] = True
                elif message_dict.get('message_type', "") == "new_map_task":
                    task_id = message_dict["task_id"]
                    # do mapping here
                    map_thread = threading.Thread(
                        target=worker_map,
                        args=(message_dict["executable"],
                              message_dict["input_paths"],
                              message_dict["num_partitions"],
                              message_dict["output_directory"],
                              task_id))
                    map_thread.start()

                    map_thread.join()
                    utils.send_tcp_message(self.manager_host,
                                           self.manager_port,
                                           {"message_type": "finished",
                                            "task_id": message_dict["task_id"],
                                            "worker_host": self.host,
                                            "worker_port": self.port})
                elif message_dict.get('message_type', "") == "new_reduce_task":
                    task_id = message_dict["task_id"]
                    # do reducing here
                    reduce_thread = threading.Thread(
                        target=worker_reduce,
                        args=(message_dict["executable"],
                              message_dict["input_paths"],
                              message_dict["output_directory"],
                              task_id))
                    reduce_thread.start()

                    reduce_thread.join()
                    utils.send_tcp_message(self.manager_host,
                                           self.manager_port,
                                           {"message_type": "finished",
                                            "task_id": message_dict["task_id"],
                                            "worker_host": self.host,
                                            "worker_port": self.port})

        if udp_running:
            self.udp_thread.join()

        LOGGER.info("worker TCP shutting down")

    def registration(self):
        """Send registration message to Manager."""
        message_dict = {
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port,
        }
        utils.send_tcp_message(self.manager_host,
                               self.manager_port, message_dict)
        LOGGER.debug("TCP send to %s:%s \n%s",
                     self.manager_host, self.manager_port,
                     json.dumps(message_dict, indent=2), )
        LOGGER.info(
            "Sent connection request to Manager %s:%s",
            self.manager_host, self.manager_port,
        )

    def worker_udp(self):
        """Send heartbeat every 2 sec or wait for a shutdown signal."""
        while not self.signals["shutdown"]:
            # Create an INET, DGRAM socket, this is UDP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                # Connect to the UDP socket on server
                sock.connect((self.manager_host, self.manager_port))

                # Send a message
                message = json.dumps({"message_type": "heartbeat",
                                      "worker_host": self.host,
                                      "worker_port": self.port})
                sock.sendall(message.encode('utf-8'))
                LOGGER.debug("UDP send heartbeat to %s:%s",
                             self.manager_host, self.manager_port, )
                time.sleep(2)

        LOGGER.info("worker UDP shutting down")


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
