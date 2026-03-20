import json
import os
import time
import uuid

import msgpack
import zmq


CLIENT_NAME = os.getenv("CLIENT_NAME", "py_client_1")
USERNAME = os.getenv("USERNAME", "alice_py")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL", "backend")
BROKER_ENDPOINT = os.getenv("BROKER_ENDPOINT", "tcp://broker:5555")
STARTUP_DELAY_SECONDS = float(os.getenv("STARTUP_DELAY_SECONDS", "5"))
REQUEST_TIMEOUT_MS = int(os.getenv("REQUEST_TIMEOUT_MS", "8000"))


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


def send_request(socket, message: dict) -> dict:
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


def main() -> None:
    time.sleep(STARTUP_DELAY_SECONDS)

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(BROKER_ENDPOINT)

    active_username = USERNAME
    logged_in = False

    for attempt in range(1, 11):
        candidate = active_username if attempt == 1 else f"{USERNAME}_{attempt}"
        response = send_request(socket, make_request("LOGIN", username=candidate))

        if response.get("status") == "OK":
            active_username = candidate
            logged_in = True
            break

        print(f"[{CLIENT_NAME}] Falha no login com '{candidate}': {response.get('error')}", flush=True)
        time.sleep(1)

    if not logged_in:
        raise RuntimeError(f"{CLIENT_NAME} não conseguiu efetuar login.")

    response = send_request(socket, make_request("LIST_CHANNELS", username=active_username))
    channels = response.get("channels", []) if response.get("status") == "OK" else []

    if TARGET_CHANNEL not in channels:
        create_response = send_request(
            socket,
            make_request("CREATE_CHANNEL", username=active_username, channel=TARGET_CHANNEL),
        )
        if create_response.get("status") != "OK":
            print(
                f"[{CLIENT_NAME}] Não foi possível criar canal '{TARGET_CHANNEL}': {create_response.get('error')}",
                flush=True,
            )

    final_list = send_request(socket, make_request("LIST_CHANNELS", username=active_username))
    print(
        f"[{CLIENT_NAME}] Fluxo finalizado. Usuário='{active_username}' | canais={final_list.get('channels', [])}",
        flush=True,
    )


if __name__ == "__main__":
    main()
