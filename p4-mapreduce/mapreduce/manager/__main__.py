"""MapReduce framework Manager node."""
import heapq
import os
import shutil
import socket
import tempfile
import logging
import json
import threading
import time
from collections import deque
import pathlib
import click

from mapreduce import utils

# Configure logging
LOGGER = logging.getLogger(__name__)


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

        self.host, self.port = host, port
        self.workers = {}  # state: ready=0, busy=1, dead=2
        self.register_order = []  # (state, order, host, port)
        self.job_queue = deque()
        # self.job_id = 0
        self.signals = {"shutdown": False, "job_id": 0, "job_done": True,
                        "finished_task": []}

        self.threads = {"udp_thread": threading.Thread(target=self.server_udp),
                        "job_thread": threading.Thread(target=self.run_job),
                        "fault_fix": threading.Thread(
                            target=self.check_heartbeat)}

        self.threads["udp_thread"].start()
        self.threads["job_thread"].start()
        self.threads["fault_fix"].start()
        self.server_tcp()

        self.threads["udp_thread"].join()  # for shutdown test

    def server_tcp(self):
        """Wait on a message from a socket OR a shutdown signal."""
        LOGGER.info("Start TCP server thread")

        # Create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            LOGGER.debug("TCP bind %s:%s",
                         self.host, self.port, )
            sock.listen()

            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)

            while not self.signals["shutdown"]:
                # print("TCP waiting ...")
                try:
                    clientsocket, _ = sock.accept()
                except socket.timeout:
                    continue

                try:
                    message_dict = utils.recv_tcp_message(clientsocket)
                except json.JSONDecodeError:
                    continue

                LOGGER.debug("Manager TCP recv \n%s",
                             json.dumps(message_dict, indent=2), )

                # shutdown when receive special shutdown message
                if message_dict.get('message_type', "") == "shutdown":
                    # forward msg to all workers
                    shut_thread = threading.Thread(target=self.shut_workers)
                    shut_thread.start()
                    shut_thread.join()
                    print("========== WORKERS ALL SHUTDOWN ===============")
                    self.signals['shutdown'] = True
                elif message_dict.get('message_type', "") == "register":
                    self.workers[(message_dict['worker_host'],
                                  message_dict['worker_port'])] \
                        = {'state': 0, 'missed_heartbeat': 0}
                    heapq.heappush(self.register_order,
                                   [0, len(self.register_order),
                                    message_dict['worker_host'],
                                    message_dict['worker_port']])
                    # send back ACK
                    ack_thread = threading.Thread(
                        target=self.ack,
                        args=(message_dict['worker_host'],
                              message_dict['worker_port']))
                    ack_thread.start()
                    ack_thread.join()
                    print("================= ACK SENT ===============")
                elif message_dict.get('message_type', "") == "new_manager_job":
                    message_dict["job_id"] = self.signals["job_id"]
                    self.signals["job_id"] += 1
                    self.job_queue.append(message_dict)
                    # for job in self.job_queue:
                    #     LOGGER.debug("Job ID %s \n%s", job["job_id"],
                    #                  json.dumps(job, indent=2), )
                    time.sleep(1)
                elif message_dict.get('message_type', "") == "finished":
                    num_workers = len(self.register_order)
                    for i in range(num_workers):
                        if self.register_order[i][2] == \
                                message_dict["worker_host"] \
                                and self.register_order[i][3] == \
                                message_dict["worker_port"]:
                            self.register_order[i][0] = 0  # busy -> ready
                            self.workers[
                                (message_dict["worker_host"],
                                 message_dict["worker_port"])]["state"] = 0
                            heapq.heapify(self.register_order)
                            break
                    self.signals["finished_task"].append(
                        message_dict["task_id"])

        print("server TCP shutting down")

    def server_udp(self):
        """Wait on a message from a socket OR a shutdown signal."""
        LOGGER.info("Start UDP server thread")
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            LOGGER.debug("UDP bind %s:%s",
                         self.host, self.port, )
            sock.settimeout(1)

            # Receive incoming UDP messages
            while not self.signals["shutdown"]:
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

                if message_dict.get('message_type', "") == "heartbeat":
                    # recv a heartbeat, update worker
                    host, port = message_dict['worker_host'], \
                                 message_dict['worker_port']
                    if (host, port) in self.workers:
                        # ignore heartbeat before worker registration
                        self.workers[(host, port)]['missed_heartbeat'] = 0
                LOGGER.debug("UDP recv \n%s",
                             json.dumps(message_dict, indent=2), )

        print("server UDP shutting down")

    def shut_workers(self):
        """Shut down workers."""
        for host, port in self.workers:
            # if self.workers[(host, port)]['state'] != 2:
            message_dict = {"message_type": "shutdown"}
            utils.send_tcp_message(host, port, message_dict)
            LOGGER.debug("TCP send to %s:%s \n%s",
                         host, port, json.dumps(message_dict, indent=2), )

    def ack(self, host, port):
        """Send ACK message back to workers."""
        message_dict = {
            "message_type": "register_ack",
            "worker_host": host,
            "worker_port": port,
        }
        utils.send_tcp_message(host, port, message_dict)
        LOGGER.debug("TCP send to %s:%s \n%s",
                     host, port, json.dumps(message_dict, indent=2), )

    def run_job(self):
        """Handle job running."""
        LOGGER.info("Start job thread")
        while not self.signals["shutdown"]:
            if self.job_queue:
                # have new job to run
                print("Detect new job.")
                job = self.job_queue.popleft()
                job_id = job["job_id"]

                prefix = f"mapreduce-shared-job{job_id:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)

                    output_dir = pathlib.Path(job["output_directory"])
                    if pathlib.Path.exists(output_dir):
                        # remove existing output dir
                        shutil.rmtree(output_dir)

                    output_dir.mkdir()
                    LOGGER.info("Created output_dir %s", output_dir)

                    # partition
                    input_dir = pathlib.Path(job["input_directory"])
                    files = []
                    for filename in input_dir.iterdir():
                        files.append(str(filename))
                    print(files)

                    tasks = {}
                    for i, filename in enumerate(files):
                        task_id = i % job["num_mappers"]
                        if task_id not in tasks:
                            tasks[task_id] = [filename]
                        else:
                            tasks[task_id].append(filename)

                    task_id = 0
                    self.signals["job_done"] = False
                    while not self.signals["shutdown"] \
                            and task_id < len(tasks):
                        LOGGER.info("Current task id %s", task_id)
                        LOGGER.info("Current workers %s", self.register_order)
                        time.sleep(1)
                        # allocate tasks to workers
                        if not self.register_order:
                            continue
                        if self.register_order[0][0] == 0:
                            host = self.register_order[0][2]
                            port = self.register_order[0][3]
                            message_dict = {
                                "message_type": "new_map_task",
                                "task_id": task_id,
                                "input_paths": tasks[task_id],
                                "executable": job["mapper_executable"],
                                "output_directory": str(tmpdir),
                                "num_partitions": job["num_reducers"],
                                "worker_host": host,
                                "worker_port": port
                            }
                            self.register_order[0][0] = 1  # ready -> busy
                            heapq.heapify(self.register_order)  # reorder
                            utils.send_tcp_message(host, port,
                                                   message_dict)

                            task_id += 1
                    LOGGER.info("Task Allocation Done")
                    # check job done
                    while not self.signals["shutdown"] and \
                            len(self.signals["finished_task"]) != len(tasks):
                        time.sleep(0.2)  # wait for all tasks to be finished

                LOGGER.info("Current job done. Move to next job.")
                self.signals["finished_task"].clear()
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)

    def check_heartbeat(self):
        """Check heartbeat and do fault tolerance."""
        LOGGER.info("Fault tolerance thread starts.")
        while not self.signals['shutdown']:
            for host, port in self.workers:
                self.workers[(host, port)]['missed_heartbeat'] += 1
                if self.workers[(host, port)]['missed_heartbeat'] == 5:
                    self.workers[(host, port)]['state'] = 2
                    print(f"Worker {host}:{port} died")
            # check status every two seconds
            time.sleep(2)


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
