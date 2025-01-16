
import click
import zmq
import time
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager

# Function for push workers
def push_worker(push_address, worker_id, stop, interval=1):
    print(f"Push Worker {worker_id} started.")
    context = zmq.Context.instance()
    socket = context.socket(zmq.PUSH)
    socket.connect(push_address)
    print(f"Push Worker {worker_id} connected to {push_address}")

    try:
        while not stop.is_set():
            payload = {
                "worker_id": worker_id,
                "timestamp": datetime.now().isoformat()
            }
            socket.send_json(payload)
            print(f"Push Worker {worker_id} sent: {payload}")
            time.sleep(interval)
    finally:
        print(f"Push Worker {worker_id} stopping.")
        socket.close()
        context.term()
        print(f"Push Worker {worker_id} stopped.")

# Function for pull workers
def pull_worker(pull_address, worker_id, stop):
    try:
        print(f"Pull Worker {worker_id} started.")
        context = zmq.Context.instance()
        socket = context.socket(zmq.PULL)
        socket.bind(pull_address)
        print(f"Pull Worker {worker_id} connected to {pull_address}")

        while not stop.is_set():
            message = socket.recv_json()
            print(f"Pull Worker {worker_id} received: {message}")
    finally:
        print(f"Pull Worker {worker_id} stopping.")
        socket.close()
        context.term()
        print(f"Pull Worker {worker_id} stopped.")


# Main function to configure and run the process pools
@click.command()
@click.option("--push-workers", default=2, help="Number of push workers.", show_default=True)
def main(push_workers=2):
    zmq_addr = "tcp://127.0.0.1:5555"
    futures = []

    manager = Manager()
    stop_signal = manager.Event()
    executor = ProcessPoolExecutor(max_workers=1 + push_workers)
    try:
        # Using ProcessPoolExecutor for request and reply workers
        print("Starting push-pull example.")
        # Launch push workers
        for i in range(push_workers):
            futures.append(executor.submit(push_worker, zmq_addr, f"push_{i}", stop_signal, interval=0))

        # Launch pull worker
        futures.append(executor.submit(pull_worker, zmq_addr, f"pull_worker", stop_signal))

        # Keep the main process running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_signal.set()
        executor.shutdown()
        print("Shutting down...")

if __name__ == "__main__":
    main()
