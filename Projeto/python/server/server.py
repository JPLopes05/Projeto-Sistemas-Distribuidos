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
PUBSUB_PROXY_IN_ENDPOINT = os.getenv("PUBSUB_PROXY_IN_ENDPOINT", "tcp://pubsub_proxy:5557")
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


def default_state() -> Dict[str, Any]:
    return {
        "server_id": SERVER_ID,
        "users": [],
        "logins": [],
        "channels": ["geral"],
        "requests": [],
        "publications": [],
    }


def save_state(state: Dict[str, Any]) -> None:
    ensure_parent(DATA_FILE)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_state() -> Dict[str, Any]:
    ensure_parent(DATA_FILE)
    if not os.path.exists(DATA_FILE):
        state = default_state()
        save_state(state)
        return state

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)

    state.setdefault("server_id", SERVER_ID)
    state.setdefault("users", [])
    state.setdefault("logins", [])
    state.setdefault("channels", ["geral"])
    state.setdefault("requests", [])
    state.setdefault("publications", [])
    if "geral" not in state["channels"]:
        state["channels"].append("geral")
    return state


def persist_request(state: Dict[str, Any], request: dict, processed_at: str) -> None:
    state["requests"].append(
        {
            "request_id": request.get("request_id"),
            "request_type": request.get("type"),
            "username": request.get("username"),
            "channel": request.get("channel"),
            "message": request.get("message"),
            "origin": request.get("origin"),
            "client_timestamp": request.get("timestamp"),
            "server_processed_at": processed_at,
        }
    )


def publication_exists(state: Dict[str, Any], publication_id: str) -> bool:
    return any(pub.get("publication_id") == publication_id for pub in state["publications"])


def persist_publication(state: Dict[str, Any], publication: dict) -> None:
    publication_id = publication.get("publication_id")
    if publication_id and publication_exists(state, publication_id):
        return
    state["publications"].append(publication)


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
    processed_at = now_iso()
    persist_request(state, request, processed_at)

    if username in state["users"]:
        save_state(state)
        return error_response(request, f"Usuário '{username}' já existe.")

    state["users"] = sorted(set([*state["users"], username]))
    state["logins"].append(
        {
            "username": username,
            "timestamp": request.get("timestamp", processed_at),
            "server_processed_at": processed_at,
        }
    )
    save_state(state)
    return ok_response(request, username=username)


def handle_list_channels(request: dict) -> dict:
    state = load_state()
    processed_at = now_iso()
    persist_request(state, request, processed_at)
    save_state(state)
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
    processed_at = now_iso()
    persist_request(state, request, processed_at)
    if channel in state["channels"]:
        save_state(state)
        return error_response(request, f"Canal '{channel}' já existe.")

    state["channels"] = sorted(set([*state["channels"], channel]))
    save_state(state)
    return ok_response(request, channel=channel)


def validate_publication_request(state: Dict[str, Any], request: dict) -> str:
    username = str(request.get("username", "")).strip()
    channel = str(request.get("channel", "")).strip()
    message_text = str(request.get("message", "")).strip()

    if username not in state["users"]:
        return f"Usuário '{username}' não está cadastrado no servidor."
    if channel not in state["channels"]:
        return f"Canal '{channel}' não existe."
    if not message_text:
        return "A mensagem não pode ser vazia."
    return ""


def build_publication(request: dict, published_at: str) -> dict:
    return {
        "type": "CHANNEL_MESSAGE",
        "publication_id": request.get("request_id"),
        "channel": request.get("channel"),
        "message": request.get("message"),
        "username": request.get("username"),
        "origin": request.get("origin"),
        "timestamp": published_at,
        "client_timestamp": request.get("timestamp"),
        "server_id": SERVER_ID,
    }


def handle_publish_message(request: dict, pub_socket: zmq.Socket) -> dict:
    state = load_state()
    processed_at = now_iso()
    persist_request(state, request, processed_at)

    validation_error = validate_publication_request(state, request)
    if validation_error:
        save_state(state)
        return error_response(request, validation_error)

    publication = build_publication(request, processed_at)
    persist_publication(state, publication)
    save_state(state)

    pub_socket.send_multipart([str(publication["channel"]).encode(), encode_message(publication)])
    log_message("PUB", publication)
    return ok_response(
        request,
        channel=publication["channel"],
        message=publication["message"],
        publication=publication,
    )


def handle_sync_publication(request: dict) -> dict:
    publication = request.get("publication")
    if not isinstance(publication, dict):
        return error_response(request, "Publicação inválida para sincronização.")

    state = load_state()
    processed_at = now_iso()
    persist_request(state, request, processed_at)
    persist_publication(state, publication)
    save_state(state)
    return ok_response(request, publication_id=publication.get("publication_id"))


def handle_request(request: dict, pub_socket: zmq.Socket) -> dict:
    request_type = request.get("type")
    if request_type == "LOGIN":
        return handle_login(request)
    if request_type == "LIST_CHANNELS":
        return handle_list_channels(request)
    if request_type == "CREATE_CHANNEL":
        return handle_create_channel(request)
    if request_type == "PUBLISH_MESSAGE":
        return handle_publish_message(request, pub_socket)
    if request_type == "SYNC_PUBLICATION":
        return handle_sync_publication(request)
    return error_response(request, f"Tipo de requisição inválido: {request_type}")


def main() -> None:
    context = zmq.Context()

    rpc_socket = context.socket(zmq.DEALER)
    rpc_socket.setsockopt(zmq.IDENTITY, SERVER_ID.encode())
    rpc_socket.connect(BACKEND_ENDPOINT)

    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(PUBSUB_PROXY_IN_ENDPOINT)

    register_message = {
        "type": "REGISTER_SERVER",
        "server_id": SERVER_ID,
        "timestamp": now_iso(),
    }
    log_message("SEND", register_message)
    rpc_socket.send(encode_message(register_message))

    while True:
        payload = rpc_socket.recv()
        request = decode_message(payload)
        log_message("RECV", request)

        response = handle_request(request, pub_socket)
        log_message("SEND", response)
        rpc_socket.send(encode_message(response))


if __name__ == "__main__":
    main()