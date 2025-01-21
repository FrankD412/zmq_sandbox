import random
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
    part: int
    final: bool
    replier_id: int
    request_id: UUID
    response_id: UUID
    timestamp: str


def proxy_process(frontend_address, backend_address):
    try:
        context = zmq.Context.instance()

        frontend = context.socket(zmq.ROUTER)
        frontend.bind(frontend_address)

        backend = context.socket(zmq.DEALER)
        backend.bind(backend_address)

        zmq.proxy(frontend, backend)
    finally:
        # We never get here...
        frontend.close()
        backend.close()
        context.term()


# Function for push workers
def requester(backend_address, requester_id, stop, num_requests, interval=1):
    print(f"Push Worker {requester_id} started.")
    context = zmq.Context.instance()
    socket = context.socket(zmq.DEALER)
    socket.connect(backend_address)
    print(f"Requester {requester_id} connected to {backend_address}")

    request_counter = 0
    decoder = msgspec.json.Decoder(Response)

    try:
        while not stop.is_set():
            request = Message(
                requester_id=requester_id,
                request_id=uuid4(),
                timestamp=datetime.now().isoformat(),
            )
            content = msgspec.json.encode(request)
            socket.send_multipart([request.request_id.bytes, b"", content])
            print(f"Requester {requester_id} Address: {request.request_id.bytes} sent: {request}")
            final = False
            while not final:
                message = socket.recv_multipart()
                response = decoder.decode(message[2])
                print(f"Requester {requester_id} received: {response}")
                final = response.final
            request_counter += 1

            if num_requests and request_counter >= num_requests:
                break
            time.sleep(interval)
    except Exception as e:
        print(e)
    finally:
        stop.set()
        print(f"Requester {requester_id} stopping.")
        socket.close()
        context.term()
        print(f"Requester {requester_id} stopped.")


# Function for pull workers
def replier(frontend_address, stop, num_requests, id, multipart):
    try:
        print("Replier process started.")
        context = zmq.Context.instance()
        socket = context.socket(zmq.DEALER)
        socket.connect(frontend_address)
        print(f"Replier process connected to {frontend_address}")

        encoder = msgspec.json.Encoder()
        decoder = msgspec.json.Decoder(Message)
        request_counter = 0

        while not stop.is_set():
            if num_requests and request_counter >= num_requests:
                break

            print("Replier waiting for message...\n")
            message = socket.recv_multipart()
            print(f"Replier received: {message}")
            parts = random.randint(1, multipart)
            for part in range(parts):
                client = message[0]
                contents = decoder.decode(message[3])
                socket.send_multipart(
                    [
                        client,
                        message[1],
                        b"",
                        encoder.encode(
                            Response(
                                requester_id=contents.requester_id,
                                replier_id=id,
                                request_id=contents.request_id,
                                response_id=uuid4(),
                                timestamp=datetime.now().isoformat(),
                                part=part,
                                final=bool(part == parts - 1),
                            )
                        ),
                    ]
                )
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
@click.option("--requesters", default=1, help="Number of requesters.")
@click.option("--repliers", default=1, help="Number of repliers.")
@click.option(
    "--num_requests", default=10, help="Number of requests for the requester to send."
)
@click.option(
    "--backend_port", default=5556, help="Port for backend proxy connections."
)
@click.option(
    "--frontend_port", default=5555, help="Port for frontend proxy connections."
)
@click.option(
    "--multipart", default=3, help="Maximum number of responses to send a requester."
)
def main(requesters, repliers, backend_port, frontend_port, num_requests, multipart):
    futures = []

    manager = Manager()
    stop_signal = manager.Event()
    executor = ProcessPoolExecutor(max_workers=1 + repliers + requesters)
    try:
        futures.append(
            executor.submit(
                proxy_process, f"tcp://*:{frontend_port}", f"tcp://*:{backend_port}"
            )
        )
        # Using ProcessPoolExecutor for request and reply workers
        print("Starting push-pull example.")
        # Launch push workers
        for i in range(requesters):
            futures.append(
                executor.submit(
                    requester,
                    f"tcp://127.0.0.1:{frontend_port}",
                    i,
                    stop_signal,
                    num_requests,
                    interval=0,
                )
            )

        # Launch pull worker
        for i in range(repliers):
            futures.append(
                executor.submit(
                    replier,
                    f"tcp://127.0.0.1:{backend_port}",
                    stop_signal,
                    num_requests,
                    i,
                    multipart,
                )
            )

        # Keep the main process running
        while not stop_signal.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        stop_signal.set()
    finally:
        futures[0]
        executor.shutdown()
        print("Shutting down...")


if __name__ == "__main__":
    main()
