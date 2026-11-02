"""Real io_uring smoke tests; skip explicitly when the host denies io_uring."""
import select
import signal
import socket
import struct
import subprocess
import sys
import threading
import time

binary = sys.argv[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def start(port):
    process = subprocess.Popen([binary, "--ip", "127.0.0.1", "--port", str(port),
                                "--threads", "3"], stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if select.select([process.stdout], [], [], 0.1)[0]:
            line = process.stdout.readline()
            if "RingKV ready" in line:
                return process
        if process.poll() is not None:
            output, error = process.communicate()
            if "io_uring setup" in error and any(text in error for text in
                    ["Operation not permitted", "Function not implemented", "Invalid argument"]):
                print("SKIP: host does not provide the required io_uring capabilities: " + error.strip())
                sys.exit(77)
            raise AssertionError((process.returncode, output, error))
    process.kill()
    raise AssertionError(("startup timed out", process.communicate()))


def command(*args):
    return (f"*{len(args)}\r\n" + "".join(
        f"${len(arg)}\r\n{arg}\r\n" for arg in args)).encode()


def stop(process, signum):
    process.send_signal(signum)
    output, error = process.communicate(timeout=8)
    assert process.returncode == 0, (process.returncode, output, error)
    assert "RingKV stopped" in output, (output, error)


port = free_port()
process = start(port)
try:
    # A second process cannot bind our non-SO_REUSEPORT listener.
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        failed = subprocess.run([binary, "--port", str(occupied.getsockname()[1]),
                                 "--threads", "3"], capture_output=True, text=True, timeout=8)
        assert failed.returncode == 1 and "bind" in failed.stderr, failed

    for i in range(12):
        with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
            client.sendall(command("SET", f"key-{i}", "value"))
            assert client.recv(1024) == b"+OK\r\n"
            client.sendall(command("GET", f"key-{i}"))
            assert client.recv(1024) == b"$5\r\nvalue\r\n"

    # A peer can reset the connection while a routed request/reply is in flight.
    for i in range(24):
        with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
            client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            client.sendall(command("GET", f"key-{i % 12}"))

    # Idle clients retain reads in flight during shutdown.
    idle = [socket.create_connection(("127.0.0.1", port), timeout=2) for _ in range(8)]
    stop(process, signal.SIGTERM)
    for client in idle:
        client.close()
finally:
    if process.poll() is None:
        process.kill()
        process.communicate()

# Port reuse and SIGINT exercise the same explicit cleanup path.
process = start(port)
try:
    stop(process, signal.SIGINT)
finally:
    if process.poll() is None:
        process.kill()
        process.communicate()

# Stop while replies and cross-worker routed requests are active.
process = start(port)
done = threading.Event()


def traffic():
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1) as client:
            payload = b"".join(command("GET", f"missing-{i}") for i in range(8))
            while not done.is_set():
                client.sendall(payload)
                if not client.recv(4096):
                    break
    except OSError:
        pass  # Closing connections during cancellation is intentional.


clients = [threading.Thread(target=traffic) for _ in range(4)]
try:
    for client in clients:
        client.start()
    time.sleep(0.15)
    stop(process, signal.SIGTERM)
finally:
    done.set()
    for client in clients:
        client.join(timeout=3)
    if process.poll() is None:
        process.kill()
        process.communicate()

print("server lifecycle tests passed")
