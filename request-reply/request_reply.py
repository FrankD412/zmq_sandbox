
from uuid import UUID, uuid4
import click
import msgspec
import zmq
import time
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager


class Message(msgspec.Struct):
    requester_id: int
    request_id: UUID
    timestamp: str


class Response(msgspec.Struct):
    requester_id: int
    request_id: UUID
    response_id: UUID
    timestamp: str


# Function for push workers
def requester(backend_address, requester_id, stop, num_requests, interval=1):
    print(f"Push Worker {requester_id} started.")
    context = zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.connect(backend_address)
    print(f"Requester {requester_id} connected to {backend_address}")

    request_counter = 0
    decoder = msgspec.json.Decoder(Response)

    try:
        while not stop.is_set():
            request = Message(requester_id=requester_id, request_id=uuid4(), timestamp=datetime.now().isoformat())
            socket.send(msgspec.json.encode(request))
            print(f"Requester {requester_id} sent: {request}")
            response = decoder.decode(socket.recv())
            print(f"Requester {requester_id} received: {response}")
            request_counter += 1

            if num_requests and request_counter >= num_requests:
                break
            time.sleep(interval)
    finally:
        stop.set()
        print(f"Requester {requester_id} stopping.")
        socket.close()
        context.term()
        print(f"Requester {requester_id} stopped.")

# Function for pull workers
def replier(frontend_address, stop, num_requests):
    try:
        print("Replier process started.")
        context = zmq.Context.instance()
        socket = context.socket(zmq.REP)
        socket.bind(frontend_address)
        print(f"Replier process connected to {frontend_address}")

        encoder = msgspec.json.Encoder()
        decoder = msgspec.json.Decoder(Message)
        request_counter = 0

        while not stop.is_set():
            if num_requests and request_counter >= num_requests:
                break

            print("Replier waiting for message...\n")
            message = decoder.decode(socket.recv())
            print(f"Replier received: {message}")
            socket.send(encoder.encode(Response(requester_id=message.requester_id, request_id=message.request_id, response_id=uuid4(), timestamp=datetime.now().isoformat())))
            request_counter += 1
    except Exception as e:
        print(e)
    finally:
        print("Replier stopping.")
        socket.close()
        context.term()
        print("Replier stopped.")


# Main function to configure and run the process pools
@click.command()
@click.option("--requesters", default=1, help="Number of requesters.", show_default=True)
@click.option("--num_requests", default=10)
def main(requesters, num_requests):
    zmq_addr = "tcp://127.0.0.1:5555"
    futures = []

    manager = Manager()
    stop_signal = manager.Event()
    executor = ProcessPoolExecutor(max_workers=1 + requesters)
    try:
        # Using ProcessPoolExecutor for request and reply workers
        print("Starting push-pull example.")
        # Launch push workers
        for i in range(requesters):
            futures.append(executor.submit(requester, zmq_addr, i, stop_signal, num_requests, interval=0))

        # Launch pull worker
        futures.append(executor.submit(replier, zmq_addr, stop_signal, num_requests))

        # Keep the main process running
        while not stop_signal.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        stop_signal.set()
    finally:
        executor.shutdown()
        print("Shutting down...")

if __name__ == "__main__":
    main()
