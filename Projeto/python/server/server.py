import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict

import msgpack
import zmq


SERVER_ID = os.getenv("SERVER_ID", "py_server_1")
BACKEND_ENDPOINT = os.getenv("BACKEND_ENDPOINT", "tcp://broker:5556")
DATA_FILE = os.getenv("DATA_FILE", "/data/state.json")

USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{3,20}$")
CHANNEL_PATTERN = re.compile(r"^[A-Za-z0-9_-]{3,30}$")


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def encode_message(message: dict) -> bytes:
    return msgpack.packb(message, use_bin_type=True)


def decode_message(payload: bytes) -> dict:
    return msgpack.unpackb(payload, raw=False)


def log_message(direction: str, message: dict) -> None:
    print(
        f"[{SERVER_ID}][{direction}] {json.dumps(message, ensure_ascii=False, sort_keys=True)}",
        flush=True,
    )


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def load_state() -> Dict[str, Any]:
    ensure_parent(DATA_FILE)
    if not os.path.exists(DATA_FILE):
        state = {
            "server_id": SERVER_ID,
            "users": [],
            "logins": [],
            "channels": ["geral"],
        }
        save_state(state)
        return state

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)

    state.setdefault("server_id", SERVER_ID)
    state.setdefault("users", [])
    state.setdefault("logins", [])
    state.setdefault("channels", ["geral"])
    if "geral" not in state["channels"]:
        state["channels"].append("geral")
    return state


def save_state(state: Dict[str, Any]) -> None:
    ensure_parent(DATA_FILE)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def ok_response(request: dict, **extra) -> dict:
    base = {
        "type": "SERVER_RESULT",
        "request_id": request.get("request_id"),
        "timestamp": now_iso(),
        "status": "OK",
        "server_id": SERVER_ID,
        "request_type": request.get("type"),
    }
    base.update(extra)
    return base


def error_response(request: dict, error: str) -> dict:
    return {
        "type": "SERVER_RESULT",
        "request_id": request.get("request_id"),
        "timestamp": now_iso(),
        "status": "ERROR",
        "server_id": SERVER_ID,
        "request_type": request.get("type"),
        "error": error,
    }


def handle_login(request: dict) -> dict:
    username = str(request.get("username", "")).strip()
    if not USERNAME_PATTERN.match(username):
        return error_response(
            request,
            "Nome de usuário inválido. Use 3 a 20 caracteres com letras, números, _ ou -.",
        )

    state = load_state()
    if username in state["users"]:
        return error_response(request, f"Usuário '{username}' já existe.")

    state["users"].append(username)
    state["users"] = sorted(set(state["users"]))
    state["logins"].append(
        {
            "username": username,
            "timestamp": request.get("timestamp", now_iso()),
            "server_processed_at": now_iso(),
        }
    )
    save_state(state)
    return ok_response(request, username=username)


def handle_list_channels(request: dict) -> dict:
    state = load_state()
    channels = sorted(set(state["channels"]))
    return ok_response(request, channels=channels)


def handle_create_channel(request: dict) -> dict:
    channel = str(request.get("channel", "")).strip()
    if not CHANNEL_PATTERN.match(channel):
        return error_response(
            request,
            "Nome de canal inválido. Use 3 a 30 caracteres com letras, números, _ ou -.",
        )

    state = load_state()
    if channel in state["channels"]:
        return error_response(request, f"Canal '{channel}' já existe.")

    state["channels"].append(channel)
    state["channels"] = sorted(set(state["channels"]))
    save_state(state)
    return ok_response(request, channel=channel)


def handle_request(request: dict) -> dict:
    request_type = request.get("type")
    if request_type == "LOGIN":
        return handle_login(request)
    if request_type == "LIST_CHANNELS":
        return handle_list_channels(request)
    if request_type == "CREATE_CHANNEL":
        return handle_create_channel(request)
    return error_response(request, f"Tipo de requisição inválido: {request_type}")


def main() -> None:
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, SERVER_ID.encode())
    socket.connect(BACKEND_ENDPOINT)

    register_message = {
        "type": "REGISTER_SERVER",
        "server_id": SERVER_ID,
        "timestamp": now_iso(),
    }
    log_message("SEND", register_message)
    socket.send(encode_message(register_message))

    while True:
        payload = socket.recv()
        request = decode_message(payload)
        log_message("RECV", request)

        response = handle_request(request)
        log_message("SEND", response)
        socket.send(encode_message(response))


if __name__ == "__main__":
    main()
