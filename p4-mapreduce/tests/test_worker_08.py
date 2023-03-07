"""See unit test function docstring."""

import json
import tempfile
import threading
import mapreduce
import utils
from utils import TESTDATA_DIR


def manager_message_generator(mock_sendall, tmp_path):
    """Fake Manager messages."""
    # Worker register
    #
    # Transfer control back to solution under test in between each check for
    # the register message to simulate the Worker calling recv() when there's
    # nothing to receive.
    for _ in utils.wait_for_register_messages(mock_sendall):
        yield None

    yield json.dumps({
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": 6001,
    }).encode("utf-8")
    yield None

    # Map task 1
    yield json.dumps({
        "message_type": "new_map_task",
        "task_id": 0,
        "executable": TESTDATA_DIR/"exec/wc_map.sh",
        "input_paths": [
            TESTDATA_DIR/"input/file01",
        ],
        "output_directory": tmp_path,
        "num_partitions": 2,
        "worker_host": "localhost",
        "worker_port": 6001,
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for Worker to finish map job
    #
    # Transfer control back to solution under test in between each check for
    # the finished message to simulate the Worker calling recv() when there's
    # nothing to receive.
    for _ in utils.wait_for_status_finished_messages(mock_sendall):
        yield None

    # Map task 2
    yield json.dumps({
        "message_type": "new_map_task",
        "task_id": 1,
        "executable": TESTDATA_DIR/"exec/wc_map.sh",
        "input_paths": [
            TESTDATA_DIR/"input/file02",
        ],
        "output_directory": tmp_path,
        "num_partitions": 2,
        "worker_host": "localhost",
        "worker_port": 6001,
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for Worker to finish the second map task. There should now be two
    # status=finished messages in total, One from each map task.
    #
    # Transfer control back to solution under test in between each check for
    # the finished message to simulate the Worker calling recv() when there's
    # nothing to receive.
    for _ in utils.wait_for_status_finished_messages(mock_sendall, num=2):
        yield None

    # Reduce task 1
    yield json.dumps({
        "message_type": "new_reduce_task",
        "task_id": 0,
        "executable": TESTDATA_DIR/"exec/wc_reduce.sh",
        "input_paths": [
            f"{tmp_path}/maptask00000-part00000",
            f"{tmp_path}/maptask00001-part00000",
        ],
        "output_directory": tmp_path,
        "worker_host": "localhost",
        "worker_port": 6001,
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for Worker to finish reduce task. There should now be three
    # finished messages in total: two from the map tasks and one
    # from this reduce task.
    #
    # Transfer control back to solution under test in between each check for
    # the finished message to simulate the Worker calling recv() when there's
    # nothing to receive.
    for _ in utils.wait_for_status_finished_messages(mock_sendall, num=3):
        yield None

    # Reduce task 2
    yield json.dumps({
        "message_type": "new_reduce_task",
        "task_id": 1,
        "executable": TESTDATA_DIR/"exec/wc_reduce.sh",
        "input_paths": [
            f"{tmp_path}/maptask00000-part00001",
            f"{tmp_path}/maptask00001-part00001",
        ],
        "output_directory": tmp_path,
        "worker_host": "localhost",
        "worker_port": 6001,
    }, cls=utils.PathJSONEncoder).encode("utf-8")
    yield None

    # Wait for Worker to finish final reduce task.
    #
    # Transfer control back to solution under test in between each check for
    # the finished message to simulate the Worker calling recv() when there's
    # nothing to receive.
    for _ in utils.wait_for_status_finished_messages(mock_sendall, num=4):
        yield None

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode("utf-8")
    yield None


def test_map_reduce(mocker, tmp_path):
    """Verify Worker can map and reduce.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.

    Note: 'tmp_path' is a fixture provided by the pytest-mock package.
    This fixture creates a temporary directory for use within this test.

    See https://docs.pytest.org/en/6.2.x/tmpdir.html for more info.
    """
    # We're using more variables for easier reading and debugging
    # pylint: disable=too-many-locals

    # Mock the socket library socket class
    mock_socket = mocker.patch("socket.socket")

    # sendall() records messages
    mock_sendall = mock_socket.return_value.__enter__.return_value.sendall

    # accept() returns a mock client socket
    mock_clientsocket = mocker.MagicMock()
    mock_accept = mock_socket.return_value.__enter__.return_value.accept
    mock_accept.return_value = (mock_clientsocket, ("127.0.0.1", 10000))

    # recv() returns values generated by manager_message_generator()
    mock_recv = mock_clientsocket.recv
    mock_recv.side_effect = manager_message_generator(mock_sendall, tmp_path)

    # Spy on tempfile.TemporaryDirectory so that we can ensure that it has
    # the correct number of calls.
    mock_tmpdir = mocker.spy(tempfile.TemporaryDirectory, "__init__")

    # Run student Worker code.  When student Worker calls recv(), it will
    # return the faked responses configured above.  When the student code calls
    # sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    try:
        mapreduce.worker.Worker(
            host="localhost",
            port=6001,
            manager_host="localhost",
            manager_port=6000,
        )
        assert threading.active_count() == 1, "Failed to shutdown threads"
    except SystemExit as error:
        assert error.code == 0

    # Verify calls to tempfile.TemporaryDirectory()
    assert mock_tmpdir.call_count == 4, (
        "Expected 4 calls to tempfile.TemporaryDirectory(...), received "
        f"{mock_tmpdir.call_count}"
    )
    assert mock_tmpdir.call_args_list == [
        mocker.call(mocker.ANY, prefix="mapreduce-local-task00000-"),
        mocker.call(mocker.ANY, prefix="mapreduce-local-task00001-"),
        mocker.call(mocker.ANY, prefix="mapreduce-local-task00000-"),
        mocker.call(mocker.ANY, prefix="mapreduce-local-task00001-"),
    ], "Incorrect calls to tempfile.TemporaryDirectory"

    # Verify messages sent by the Worker
    #
    # Pro-tip: show log messages and detailed diffs with
    #   $ pytest -vvs --log-cli-level=info tests/test_worker_X.py
    messages = utils.filter_not_heartbeat_messages(
        utils.get_messages(mock_sendall)
    )
    assert messages == [
        {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        },
        {
            "message_type": "finished",
            "task_id": 0,
            "worker_host": "localhost",
            "worker_port": 6001,
        },
        {
            "message_type": "finished",
            "task_id": 1,
            "worker_host": "localhost",
            "worker_port": 6001,
        },
        {
            "message_type": "finished",
            "task_id": 0,
            "worker_host": "localhost",
            "worker_port": 6001,
        },
        {
            "message_type": "finished",
            "task_id": 1,
            "worker_host": "localhost",
            "worker_port": 6001,
        },
    ]

    # Verify Map Stage output
    maptask00 = (tmp_path/"maptask00000-part00000").read_text().splitlines()
    maptask01 = (tmp_path/"maptask00000-part00001").read_text().splitlines()
    maptask10 = (tmp_path/"maptask00001-part00000").read_text().splitlines()
    maptask11 = (tmp_path/"maptask00001-part00001").read_text().splitlines()
    assert maptask00 == [
        "\t1",
        "bye\t1",
        "hello\t1",
    ]
    assert maptask01 == [
        "world\t1",
        "world\t1",
    ]
    assert maptask10 == [
        "\t1",
        "hello\t1",
    ]
    assert maptask11 == [
        "goodbye\t1",
        "hadoop\t1",
        "hadoop\t1",
    ]

    # Verify Reduce Stage output
    part00000 = (tmp_path/"part-00000").read_text().splitlines()
    part00001 = (tmp_path/"part-00001").read_text().splitlines()
    assert part00000 == [
        "\t2",
        "bye\t1",
        "hello\t2",
    ]
    assert part00001 == [
        "goodbye\t1",
        "hadoop\t2",
        "world\t2",
    ]
