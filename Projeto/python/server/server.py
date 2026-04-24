import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import msgpack
import zmq


SERVER_ID = os.getenv("SERVER_ID", "py_server_1")
BACKEND_ENDPOINT = os.getenv("BACKEND_ENDPOINT", "tcp://broker:5556")
PUBSUB_PROXY_IN_ENDPOINT = os.getenv("PUBSUB_PROXY_IN_ENDPOINT", "tcp://pubsub_proxy:5557")
REFERENCE_ENDPOINT = os.getenv("REFERENCE_ENDPOINT", "tcp://reference_service:5560")
DATA_FILE = os.getenv("DATA_FILE", "/data/state.json")

USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{3,20}$")
CHANNEL_PATTERN = re.compile(r"^[A-Za-z0-9_-]{3,30}$")

REFERENCE_TIMEOUT_MS = int(os.getenv("REFERENCE_TIMEOUT_MS", "5000"))
HEARTBEAT_EVERY_CLIENT_MESSAGES = int(os.getenv("HEARTBEAT_EVERY_CLIENT_MESSAGES", "10"))

CLIENT_REQUEST_TYPES = {"LOGIN", "LIST_CHANNELS", "CREATE_CHANNEL", "PUBLISH_MESSAGE"}


class LogicalClock:
    def __init__(self) -> None:
        self.value = 0

    def tick(self) -> int:
        self.value += 1
        return self.value

    def update_from_message(self, message: Optional[dict]) -> int:
        if not isinstance(message, dict):
            return self.value

        received = message.get("logical_clock")

        if isinstance(received, int):
            self.value = max(self.value, received)
        elif isinstance(received, str) and received.isdigit():
            self.value = max(self.value, int(received))

        return self.value


LOGICAL_CLOCK = LogicalClock()
PHYSICAL_CLOCK_OFFSET_SECONDS = 0.0
SERVER_RANK: Optional[int] = None
CLIENT_MESSAGE_COUNT = 0


def corrected_now_datetime() -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=PHYSICAL_CLOCK_OFFSET_SECONDS)


def corrected_now_iso() -> str:
    return corrected_now_datetime().strftime("%Y-%m-%dT%H:%M:%SZ")


def local_epoch_ms() -> int:
    return int(time.time() * 1000)


def update_physical_clock_from_reference(response: dict) -> None:
    global PHYSICAL_CLOCK_OFFSET_SECONDS

    reference_ms = response.get("reference_timestamp_epoch_ms")
    if not isinstance(reference_ms, int):
        return

    local_ms = local_epoch_ms()
    PHYSICAL_CLOCK_OFFSET_SECONDS = (reference_ms - local_ms) / 1000.0

    print(
        f"[{SERVER_ID}][CLOCK_SYNC] offset_fisico_segundos={PHYSICAL_CLOCK_OFFSET_SECONDS:.3f}",
        flush=True,
    )


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
        "server_rank": SERVER_RANK,
        "users": [],
        "logins": [],
        "channels": ["geral"],
        "requests": [],
        "publications": [],
        "heartbeats": [],
    }


def save_state(state: Dict[str, Any]) -> None:
    ensure_parent(DATA_FILE)
    state["server_id"] = SERVER_ID
    state["server_rank"] = SERVER_RANK

    temp_file = f"{DATA_FILE}.tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

    os.replace(temp_file, DATA_FILE)


def load_state() -> Dict[str, Any]:
    ensure_parent(DATA_FILE)

    if not os.path.exists(DATA_FILE):
        state = default_state()
        save_state(state)
        return state

    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except json.JSONDecodeError:
        corrupt_file = f"{DATA_FILE}.corrupt-{int(time.time())}"
        os.replace(DATA_FILE, corrupt_file)
        print(
            f"[{SERVER_ID}][STATE] state.json corrompido. Backup criado em {corrupt_file}. Novo estado iniciado.",
            flush=True,
        )
        state = default_state()
        save_state(state)
        return state

    state.setdefault("server_id", SERVER_ID)
    state.setdefault("server_rank", SERVER_RANK)
    state.setdefault("users", [])
    state.setdefault("logins", [])
    state.setdefault("channels", ["geral"])
    state.setdefault("requests", [])
    state.setdefault("publications", [])
    state.setdefault("heartbeats", [])

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
            "client_logical_clock": request.get("logical_clock"),
            "server_processed_at": processed_at,
            "server_logical_clock_after_receive": LOGICAL_CLOCK.value,
        }
    )


def publication_exists(state: Dict[str, Any], publication_id: str) -> bool:
    return any(pub.get("publication_id") == publication_id for pub in state["publications"])


def persist_publication(state: Dict[str, Any], publication: dict) -> None:
    publication_id = publication.get("publication_id")
    if publication_id and publication_exists(state, publication_id):
        return

    state["publications"].append(publication)


def reference_request(context: zmq.Context, request_type: str, **extra) -> dict:
    request_socket = context.socket(zmq.REQ)
    request_socket.connect(REFERENCE_ENDPOINT)

    request = {
        "type": request_type,
        "request_id": str(uuid.uuid4()),
        "server_id": SERVER_ID,
        "timestamp": corrected_now_iso(),
        "logical_clock": LOGICAL_CLOCK.tick(),
        **extra,
    }

    log_message("REFERENCE_SEND", request)
    request_socket.send(encode_message(request))

    poller = zmq.Poller()
    poller.register(request_socket, zmq.POLLIN)
    events = dict(poller.poll(REFERENCE_TIMEOUT_MS))

    if request_socket not in events:
        request_socket.close(0)
        raise TimeoutError(f"Timeout ao consultar reference_service para {request_type}.")

    response = decode_message(request_socket.recv())
    request_socket.close(0)

    LOGICAL_CLOCK.update_from_message(response)
    update_physical_clock_from_reference(response)
    log_message("REFERENCE_RECV", response)

    return response


def register_rank_with_reference(context: zmq.Context) -> int:
    global SERVER_RANK

    for attempt in range(1, 11):
        try:
            response = reference_request(context, "GET_RANK")
            if response.get("status") == "OK":
                SERVER_RANK = int(response["rank"])
                print(f"[{SERVER_ID}] Rank recebido do serviço de referência: {SERVER_RANK}", flush=True)
                return SERVER_RANK
        except Exception as exc:
            print(
                f"[{SERVER_ID}] Tentativa {attempt}/10 falhou ao obter rank: {exc}",
                flush=True,
            )
            time.sleep(1)

    raise RuntimeError("Não foi possível obter rank no serviço de referência.")


def send_heartbeat_if_needed(context: zmq.Context) -> None:
    global CLIENT_MESSAGE_COUNT

    if CLIENT_MESSAGE_COUNT == 0:
        return

    if CLIENT_MESSAGE_COUNT % HEARTBEAT_EVERY_CLIENT_MESSAGES != 0:
        return

    try:
        response = reference_request(
            context,
            "HEARTBEAT",
            processed_client_messages=CLIENT_MESSAGE_COUNT,
        )

        state = load_state()
        state["heartbeats"].append(
            {
                "timestamp": corrected_now_iso(),
                "logical_clock": LOGICAL_CLOCK.value,
                "processed_client_messages": CLIENT_MESSAGE_COUNT,
                "reference_status": response.get("status"),
                "active_servers": response.get("active_servers", []),
            }
        )
        save_state(state)

    except Exception as exc:
        print(f"[{SERVER_ID}][HEARTBEAT] Falha ao enviar heartbeat: {exc}", flush=True)


def ok_response(request: dict, **extra) -> dict:
    base = {
        "type": "SERVER_RESULT",
        "request_id": request.get("request_id"),
        "timestamp": corrected_now_iso(),
        "logical_clock": LOGICAL_CLOCK.tick(),
        "status": "OK",
        "server_id": SERVER_ID,
        "server_rank": SERVER_RANK,
        "request_type": request.get("type"),
    }
    base.update(extra)
    return base


def error_response(request: dict, error: str) -> dict:
    return {
        "type": "SERVER_RESULT",
        "request_id": request.get("request_id"),
        "timestamp": corrected_now_iso(),
        "logical_clock": LOGICAL_CLOCK.tick(),
        "status": "ERROR",
        "server_id": SERVER_ID,
        "server_rank": SERVER_RANK,
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
    processed_at = corrected_now_iso()
    persist_request(state, request, processed_at)

    if username in state["users"]:
        save_state(state)
        return error_response(request, f"Usuário '{username}' já existe.")

    state["users"] = sorted(set([*state["users"], username]))
    state["logins"].append(
        {
            "username": username,
            "timestamp": request.get("timestamp", processed_at),
            "client_logical_clock": request.get("logical_clock"),
            "server_processed_at": processed_at,
            "server_logical_clock": LOGICAL_CLOCK.value,
        }
    )
    save_state(state)
    return ok_response(request, username=username)


def handle_list_channels(request: dict) -> dict:
    state = load_state()
    processed_at = corrected_now_iso()
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
    processed_at = corrected_now_iso()
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
        "logical_clock": LOGICAL_CLOCK.tick(),
        "client_timestamp": request.get("timestamp"),
        "client_logical_clock": request.get("logical_clock"),
        "server_id": SERVER_ID,
        "server_rank": SERVER_RANK,
    }


def handle_publish_message(request: dict, pub_socket: zmq.Socket) -> dict:
    state = load_state()
    processed_at = corrected_now_iso()
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

    LOGICAL_CLOCK.update_from_message(publication)

    state = load_state()
    processed_at = corrected_now_iso()
    persist_request(state, request, processed_at)
    persist_publication(state, publication)
    save_state(state)

    return ok_response(request, publication_id=publication.get("publication_id"))


def handle_request(request: dict, pub_socket: zmq.Socket, reference_context: zmq.Context) -> dict:
    global CLIENT_MESSAGE_COUNT

    LOGICAL_CLOCK.update_from_message(request)

    request_type = request.get("type")

    if request_type in CLIENT_REQUEST_TYPES:
        CLIENT_MESSAGE_COUNT += 1
        send_heartbeat_if_needed(reference_context)

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

    register_rank_with_reference(context)

    rpc_socket = context.socket(zmq.DEALER)
    rpc_socket.setsockopt(zmq.IDENTITY, SERVER_ID.encode())
    rpc_socket.connect(BACKEND_ENDPOINT)

    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(PUBSUB_PROXY_IN_ENDPOINT)

    register_message = {
        "type": "REGISTER_SERVER",
        "server_id": SERVER_ID,
        "server_rank": SERVER_RANK,
        "timestamp": corrected_now_iso(),
        "logical_clock": LOGICAL_CLOCK.tick(),
    }
    log_message("SEND", register_message)
    rpc_socket.send(encode_message(register_message))

    while True:
        payload = rpc_socket.recv()
        request = decode_message(payload)
        log_message("RECV", request)

        response = handle_request(request, pub_socket, context)
        log_message("SEND", response)
        rpc_socket.send(encode_message(response))


if __name__ == "__main__":
    main()