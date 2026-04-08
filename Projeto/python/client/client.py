import json
import os
import queue
import random
import threading
import time
import uuid
from typing import List, Set

import msgpack
import zmq


CLIENT_NAME = os.getenv("CLIENT_NAME", "py_client_1")
USERNAME = os.getenv("USERNAME", "alice_py")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL", "backend")
BROKER_ENDPOINT = os.getenv("BROKER_ENDPOINT", "tcp://broker:5555")
PUBSUB_PROXY_OUT_ENDPOINT = os.getenv("PUBSUB_PROXY_OUT_ENDPOINT", "tcp://pubsub_proxy:5558")
STARTUP_DELAY_SECONDS = float(os.getenv("STARTUP_DELAY_SECONDS", "5"))
REQUEST_TIMEOUT_MS = int(os.getenv("REQUEST_TIMEOUT_MS", "8000"))
MINIMUM_CHANNELS = int(os.getenv("MINIMUM_CHANNELS", "5"))
MINIMUM_SUBSCRIPTIONS = int(os.getenv("MINIMUM_SUBSCRIPTIONS", "3"))
MESSAGES_PER_BATCH = int(os.getenv("MESSAGES_PER_BATCH", "10"))
MESSAGE_INTERVAL_SECONDS = float(os.getenv("MESSAGE_INTERVAL_SECONDS", "1"))
MAX_BATCHES = int(os.getenv("MAX_BATCHES", "0"))

MESSAGE_TEMPLATES = [
    "Atualização do canal {channel} enviada por {user}",
    "Mensagem automática #{counter} no canal {channel}",
    "Bot {user} reportando atividade distribuída em {channel}",
    "Evento sincronizado {counter} para o tópico {channel}",
    "Heartbeat do bot {user} no canal {channel}",
]


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def encode_message(message: dict) -> bytes:
    return msgpack.packb(message, use_bin_type=True)


def decode_message(payload: bytes) -> dict:
    return msgpack.unpackb(payload, raw=False)


def log_message(direction: str, message: dict) -> None:
    print(
        f"[{CLIENT_NAME}][{direction}] {json.dumps(message, ensure_ascii=False, sort_keys=True)}",
        flush=True,
    )


class SubscriptionReceiver(threading.Thread):
    def __init__(self, endpoint: str) -> None:
        super().__init__(daemon=True)
        self.endpoint = endpoint
        self.ready = threading.Event()
        self.commands: "queue.Queue[str]" = queue.Queue()
        self.subscribed_channels: Set[str] = set()

    def subscribe(self, channel: str) -> None:
        self.commands.put(channel)

    def run(self) -> None:
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect(self.endpoint)
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        self.ready.set()

        while True:
            while True:
                try:
                    channel = self.commands.get_nowait()
                except queue.Empty:
                    break

                if channel not in self.subscribed_channels:
                    socket.setsockopt(zmq.SUBSCRIBE, channel.encode())
                    self.subscribed_channels.add(channel)
                    print(
                        f"[{CLIENT_NAME}][SUBSCRIBE] Inscrito no canal '{channel}'",
                        flush=True,
                    )

            events = dict(poller.poll(250))
            if socket not in events:
                continue

            topic_bytes, payload = socket.recv_multipart()
            message = decode_message(payload)
            received_message = {
                "channel": topic_bytes.decode(),
                "message": message.get("message"),
                "sent_timestamp": message.get("timestamp"),
                "received_timestamp": now_iso(),
                "username": message.get("username"),
                "server_id": message.get("server_id"),
                "publication_id": message.get("publication_id"),
            }
            log_message("PUBSUB_RECV", received_message)


def send_request(socket: zmq.Socket, message: dict) -> dict:
    log_message("SEND", message)
    socket.send(encode_message(message))

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    events = dict(poller.poll(REQUEST_TIMEOUT_MS))
    if socket not in events:
        raise TimeoutError("Timeout aguardando resposta do broker.")

    payload = socket.recv()
    response = decode_message(payload)
    log_message("RECV", response)
    return response


def make_request(request_type: str, **extra) -> dict:
    return {
        "type": request_type,
        "request_id": str(uuid.uuid4()),
        "timestamp": now_iso(),
        "origin": CLIENT_NAME,
        **extra,
    }


def list_channels(socket: zmq.Socket, username: str) -> List[str]:
    response = send_request(socket, make_request("LIST_CHANNELS", username=username))
    if response.get("status") != "OK":
        raise RuntimeError(response.get("error", "Falha ao listar canais."))
    return list(response.get("channels", []))


def ensure_single_channel_creation(socket: zmq.Socket, username: str, channels: List[str]) -> List[str]:
    if len(channels) >= MINIMUM_CHANNELS:
        return channels

    create_response = send_request(
        socket,
        make_request("CREATE_CHANNEL", username=username, channel=TARGET_CHANNEL),
    )
    if create_response.get("status") != "OK":
        print(
            f"[{CLIENT_NAME}] Não foi possível criar canal '{TARGET_CHANNEL}': {create_response.get('error')}",
            flush=True,
        )
    return list_channels(socket, username)


def maybe_subscribe_more(receiver: SubscriptionReceiver, channels: List[str]) -> None:
    available_candidates = [channel for channel in channels if channel not in receiver.subscribed_channels]
    if len(receiver.subscribed_channels) >= MINIMUM_SUBSCRIPTIONS or not available_candidates:
        return

    next_channel = random.choice(available_candidates)
    receiver.subscribe(next_channel)
    time.sleep(0.2)


def random_message(channel: str, username: str, counter: int) -> str:
    template = random.choice(MESSAGE_TEMPLATES)
    return template.format(channel=channel, user=username, counter=counter)


def login(socket: zmq.Socket) -> str:
    active_username = USERNAME
    for attempt in range(1, 11):
        candidate = active_username if attempt == 1 else f"{USERNAME}_{attempt}"
        response = send_request(socket, make_request("LOGIN", username=candidate))
        if response.get("status") == "OK":
            return candidate

        print(f"[{CLIENT_NAME}] Falha no login com '{candidate}': {response.get('error')}", flush=True)
        time.sleep(1)

    raise RuntimeError(f"{CLIENT_NAME} não conseguiu efetuar login.")


def publish_batch(socket: zmq.Socket, username: str, channel: str, batch_number: int) -> None:
    for message_counter in range(1, MESSAGES_PER_BATCH + 1):
        message_text = random_message(channel, username, ((batch_number - 1) * MESSAGES_PER_BATCH) + message_counter)
        response = send_request(
            socket,
            make_request(
                "PUBLISH_MESSAGE",
                username=username,
                channel=channel,
                message=message_text,
            ),
        )
        if response.get("status") != "OK":
            print(
                f"[{CLIENT_NAME}] Falha ao publicar no canal '{channel}': {response.get('error')}",
                flush=True,
            )
        time.sleep(MESSAGE_INTERVAL_SECONDS)


def main() -> None:
    random.seed()
    time.sleep(STARTUP_DELAY_SECONDS)

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(BROKER_ENDPOINT)

    receiver = SubscriptionReceiver(PUBSUB_PROXY_OUT_ENDPOINT)
    receiver.start()
    receiver.ready.wait()

    active_username = login(socket)
    channels = list_channels(socket, active_username)
    channels = ensure_single_channel_creation(socket, active_username, channels)
    maybe_subscribe_more(receiver, channels)

    batch_number = 0
    while True:
        channels = list_channels(socket, active_username)
        maybe_subscribe_more(receiver, channels)

        if not channels:
            time.sleep(1)
            continue

        batch_number += 1
        chosen_channel = random.choice(channels)
        publish_batch(socket, active_username, chosen_channel, batch_number)

        if MAX_BATCHES > 0 and batch_number >= MAX_BATCHES:
            print(
                f"[{CLIENT_NAME}] Execução encerrada após {batch_number} lote(s).",
                flush=True,
            )
            break


if __name__ == "__main__":
    main()