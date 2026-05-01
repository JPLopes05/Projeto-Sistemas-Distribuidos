import json
import os
import re
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import msgpack
import zmq


SERVER_ID = os.getenv("SERVER_ID", "py_server_1")
BACKEND_ENDPOINT = os.getenv("BACKEND_ENDPOINT", "tcp://broker:5556")
PUBSUB_PROXY_IN_ENDPOINT = os.getenv("PUBSUB_PROXY_IN_ENDPOINT", "tcp://pubsub_proxy:5557")
PUBSUB_PROXY_OUT_ENDPOINT = os.getenv("PUBSUB_PROXY_OUT_ENDPOINT", "tcp://pubsub_proxy:5558")
REFERENCE_ENDPOINT = os.getenv("REFERENCE_ENDPOINT", "tcp://reference_service:5560")
DATA_FILE = os.getenv("DATA_FILE", "/data/state.json")

USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{3,20}$")
CHANNEL_PATTERN = re.compile(r"^[A-Za-z0-9_-]{3,30}$")

REFERENCE_TIMEOUT_MS = int(os.getenv("REFERENCE_TIMEOUT_MS", "5000"))
HEARTBEAT_EVERY_CLIENT_MESSAGES = int(os.getenv("HEARTBEAT_EVERY_CLIENT_MESSAGES", "10"))
CLOCK_SYNC_EVERY_MESSAGES = int(os.getenv("CLOCK_SYNC_EVERY_MESSAGES", "15"))
SERVER_RPC_TIMEOUT_MS = int(os.getenv("SERVER_RPC_TIMEOUT_MS", "2500"))
SERVER_RPC_PORT = int(os.getenv("SERVER_RPC_PORT", "5570"))

SERVER_ORDER = [
    item.strip()
    for item in os.getenv(
        "SERVER_ORDER",
        "js_server_1,js_server_2,py_server_1,py_server_2",
    ).split(",")
    if item.strip()
]

SERVER_ENDPOINTS = {
    server_id: f"tcp://{server_id}:{SERVER_RPC_PORT}"
    for server_id in SERVER_ORDER
}

CLIENT_REQUEST_TYPES = {"LOGIN", "LIST_CHANNELS", "CREATE_CHANNEL", "PUBLISH_MESSAGE"}
BROKER_REQUEST_TYPES = CLIENT_REQUEST_TYPES | {"SYNC_PUBLICATION"}
SERVERS_TOPIC = "servers"


class LogicalClock:
    def __init__(self) -> None:
        self.value = 0
        self.lock = threading.Lock()

    def tick(self) -> int:
        with self.lock:
            self.value += 1
            return self.value

    def update_from_message(self, message: Optional[dict]) -> int:
        if not isinstance(message, dict):
            with self.lock:
                return self.value

        received = message.get("logical_clock")

        with self.lock:
            if isinstance(received, int):
                self.value = max(self.value, received)
            elif isinstance(received, str) and received.isdigit():
                self.value = max(self.value, int(received))

            return self.value

    def current(self) -> int:
        with self.lock:
            return self.value


LOGICAL_CLOCK = LogicalClock()
PHYSICAL_CLOCK_OFFSET_SECONDS = 0.0
SERVER_RANK: Optional[int] = None
CLIENT_MESSAGE_COUNT = 0
EXCHANGED_MESSAGE_COUNT = 0
CURRENT_COORDINATOR_ID = SERVER_ORDER[0] if SERVER_ORDER else SERVER_ID
COORDINATOR_LOCK = threading.Lock()
OFFSET_LOCK = threading.Lock()


def corrected_now_datetime() -> datetime:
    with OFFSET_LOCK:
        offset = PHYSICAL_CLOCK_OFFSET_SECONDS
    return datetime.now(timezone.utc) + timedelta(seconds=offset)


def corrected_now_iso() -> str:
    return corrected_now_datetime().strftime("%Y-%m-%dT%H:%M:%SZ")


def corrected_epoch_ms() -> int:
    return int(corrected_now_datetime().timestamp() * 1000)


def local_epoch_ms() -> int:
    return int(time.time() * 1000)


def server_rank_for(server_id: str) -> int:
    if server_id in SERVER_ORDER:
        return SERVER_ORDER.index(server_id) + 1
    return 9999


def get_current_coordinator() -> str:
    with COORDINATOR_LOCK:
        return CURRENT_COORDINATOR_ID


def set_current_coordinator(coordinator_id: str, source: str) -> None:
    global CURRENT_COORDINATOR_ID

    coordinator_id = str(coordinator_id or "").strip()
    if not coordinator_id:
        return

    with COORDINATOR_LOCK:
        previous = CURRENT_COORDINATOR_ID
        CURRENT_COORDINATOR_ID = coordinator_id

    if previous != coordinator_id:
        print(
            f"[{SERVER_ID}][COORDINATOR_UPDATE] coordenador={coordinator_id} anterior={previous} origem={source}",
            flush=True,
        )


def update_physical_clock_from_coordinator(coordinator_epoch_ms: int, coordinator_id: str) -> None:
    global PHYSICAL_CLOCK_OFFSET_SECONDS

    if not isinstance(coordinator_epoch_ms, int):
        return

    local_ms = local_epoch_ms()
    new_offset = (coordinator_epoch_ms - local_ms) / 1000.0

    with OFFSET_LOCK:
        PHYSICAL_CLOCK_OFFSET_SECONDS = new_offset

    print(
        f"[{SERVER_ID}][CLOCK_SYNC_FROM_COORDINATOR] coordenador={coordinator_id} offset_fisico_segundos={new_offset:.3f}",
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
        "current_coordinator_id": get_current_coordinator(),
        "users": [],
        "logins": [],
        "channels": ["geral"],
        "requests": [],
        "publications": [],
        "heartbeats": [],
        "clock_syncs": [],
        "elections": [],
    }


def save_state(state: Dict[str, Any]) -> None:
    ensure_parent(DATA_FILE)
    state["server_id"] = SERVER_ID
    state["server_rank"] = SERVER_RANK
    state["current_coordinator_id"] = get_current_coordinator()

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
    state.setdefault("current_coordinator_id", get_current_coordinator())
    state.setdefault("users", [])
    state.setdefault("logins", [])
    state.setdefault("channels", ["geral"])
    state.setdefault("requests", [])
    state.setdefault("publications", [])
    state.setdefault("heartbeats", [])
    state.setdefault("clock_syncs", [])
    state.setdefault("elections", [])

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
            "server_logical_clock_after_receive": LOGICAL_CLOCK.current(),
            "current_coordinator_id": get_current_coordinator(),
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


def server_rpc_request(context: zmq.Context, target_server_id: str, request_type: str, **extra) -> dict:
    endpoint = SERVER_ENDPOINTS.get(target_server_id)
    if not endpoint:
        raise ValueError(f"Endpoint não configurado para servidor '{target_server_id}'.")

    request_socket = context.socket(zmq.REQ)
    request_socket.connect(endpoint)

    request = {
        "type": request_type,
        "request_id": str(uuid.uuid4()),
        "server_id": SERVER_ID,
        "server_rank": SERVER_RANK,
        "timestamp": corrected_now_iso(),
        "logical_clock": LOGICAL_CLOCK.tick(),
        **extra,
    }

    log_message(f"SERVER_RPC_SEND:{target_server_id}", request)
    request_socket.send(encode_message(request))

    poller = zmq.Poller()
    poller.register(request_socket, zmq.POLLIN)
    events = dict(poller.poll(SERVER_RPC_TIMEOUT_MS))

    if request_socket not in events:
        request_socket.close(0)
        raise TimeoutError(f"Timeout ao consultar servidor {target_server_id} para {request_type}.")

    response = decode_message(request_socket.recv())
    request_socket.close(0)

    LOGICAL_CLOCK.update_from_message(response)
    log_message(f"SERVER_RPC_RECV:{target_server_id}", response)
    return response


def publish_coordinator_announcement(pub_socket: zmq.Socket, coordinator_id: str, reason: str) -> None:
    announcement = {
        "type": "COORDINATOR_ANNOUNCEMENT",
        "coordinator_id": coordinator_id,
        "coordinator_rank": server_rank_for(coordinator_id),
        "announcer_id": SERVER_ID,
        "announcer_rank": SERVER_RANK,
        "reason": reason,
        "timestamp": corrected_now_iso(),
        "logical_clock": LOGICAL_CLOCK.tick(),
    }

    pub_socket.send_multipart([SERVERS_TOPIC.encode(), encode_message(announcement)])
    log_message("PUB:servers", announcement)


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
            current_coordinator_id=get_current_coordinator(),
        )

        state = load_state()
        state["heartbeats"].append(
            {
                "timestamp": corrected_now_iso(),
                "logical_clock": LOGICAL_CLOCK.current(),
                "processed_client_messages": CLIENT_MESSAGE_COUNT,
                "reference_status": response.get("status"),
                "active_servers": response.get("active_servers", []),
                "current_coordinator_id": get_current_coordinator(),
            }
        )
        save_state(state)

    except Exception as exc:
        print(f"[{SERVER_ID}][HEARTBEAT] Falha ao enviar heartbeat: {exc}", flush=True)


def internal_ok_response(request: dict, **extra) -> dict:
    response = {
        "type": "SERVER_INTERNAL_REPLY",
        "request_id": request.get("request_id"),
        "request_type": request.get("type"),
        "status": "OK",
        "server_id": SERVER_ID,
        "server_rank": SERVER_RANK,
        "current_coordinator_id": get_current_coordinator(),
        "timestamp": corrected_now_iso(),
        "logical_clock": LOGICAL_CLOCK.tick(),
    }
    response.update(extra)
    return response


def internal_error_response(request: dict, error: str) -> dict:
    return {
        "type": "SERVER_INTERNAL_REPLY",
        "request_id": request.get("request_id"),
        "request_type": request.get("type"),
        "status": "ERROR",
        "server_id": SERVER_ID,
        "server_rank": SERVER_RANK,
        "current_coordinator_id": get_current_coordinator(),
        "timestamp": corrected_now_iso(),
        "logical_clock": LOGICAL_CLOCK.tick(),
        "error": error,
    }


def handle_internal_request(request: dict) -> dict:
    global EXCHANGED_MESSAGE_COUNT

    LOGICAL_CLOCK.update_from_message(request)
    EXCHANGED_MESSAGE_COUNT += 1

    request_type = request.get("type")

    if request_type == "CLOCK_REQUEST":
        if get_current_coordinator() != SERVER_ID:
            return internal_error_response(
                request,
                f"Servidor '{SERVER_ID}' não é o coordenador atual. Coordenador conhecido: {get_current_coordinator()}",
            )

        return internal_ok_response(
            request,
            coordinator_id=SERVER_ID,
            coordinator_epoch_ms=corrected_epoch_ms(),
        )

    if request_type == "ELECTION_REQUEST":
        return internal_ok_response(
            request,
            election_response="OK",
        )

    if request_type == "COORDINATOR_NOTIFICATION":
        coordinator_id = str(request.get("coordinator_id", "")).strip()
        if coordinator_id:
            set_current_coordinator(coordinator_id, f"direct_notification_from_{request.get('server_id')}")
        return internal_ok_response(request, coordinator_id=get_current_coordinator())

    return internal_error_response(request, f"Tipo de requisição interna inválido: {request_type}")


def internal_rpc_server_loop(context: zmq.Context) -> None:
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{SERVER_RPC_PORT}")

    print(f"[{SERVER_ID}][SERVER_RPC] Escutando em tcp://*:{SERVER_RPC_PORT}", flush=True)

    while True:
        payload = socket.recv()
        request = decode_message(payload)
        log_message("SERVER_RPC_RECV", request)

        response = handle_internal_request(request)
        log_message("SERVER_RPC_SEND", response)
        socket.send(encode_message(response))


def servers_subscription_loop(context: zmq.Context) -> None:
    socket = context.socket(zmq.SUB)
    socket.connect(PUBSUB_PROXY_OUT_ENDPOINT)
    socket.setsockopt(zmq.SUBSCRIBE, SERVERS_TOPIC.encode())

    print(f"[{SERVER_ID}][SUBSCRIBE] Inscrito no tópico '{SERVERS_TOPIC}'", flush=True)

    while True:
        topic_bytes, payload = socket.recv_multipart()
        message = decode_message(payload)
        LOGICAL_CLOCK.update_from_message(message)
        log_message("PUBSUB_RECV:servers", message)

        if message.get("type") == "COORDINATOR_ANNOUNCEMENT":
            coordinator_id = str(message.get("coordinator_id", "")).strip()
            if coordinator_id:
                set_current_coordinator(coordinator_id, f"pubsub_from_{message.get('announcer_id')}")


def start_background_threads(context: zmq.Context) -> None:
    threading.Thread(target=internal_rpc_server_loop, args=(context,), daemon=True).start()
    threading.Thread(target=servers_subscription_loop, args=(context,), daemon=True).start()


def elect_coordinator(context: zmq.Context, pub_socket: zmq.Socket, reason: str) -> str:
    candidates: List[Tuple[str, int]] = [(SERVER_ID, SERVER_RANK or server_rank_for(SERVER_ID))]

    print(f"[{SERVER_ID}][ELECTION_START] motivo={reason}", flush=True)

    for other_server_id in SERVER_ORDER:
        if other_server_id == SERVER_ID:
            continue

        try:
            response = server_rpc_request(
                context,
                other_server_id,
                "ELECTION_REQUEST",
                reason=reason,
                known_coordinator_id=get_current_coordinator(),
            )

            if response.get("status") == "OK":
                candidates.append(
                    (
                        str(response.get("server_id", other_server_id)),
                        int(response.get("server_rank") or server_rank_for(other_server_id)),
                    )
                )
        except Exception as exc:
            print(
                f"[{SERVER_ID}][ELECTION] servidor={other_server_id} indisponível motivo={exc}",
                flush=True,
            )

    elected_id, elected_rank = sorted(candidates, key=lambda item: item[1])[0]
    set_current_coordinator(elected_id, "election_result")
    publish_coordinator_announcement(pub_socket, elected_id, reason)

    state = load_state()
    state["elections"].append(
        {
            "timestamp": corrected_now_iso(),
            "logical_clock": LOGICAL_CLOCK.current(),
            "reason": reason,
            "candidates": [{"server_id": item[0], "rank": item[1]} for item in candidates],
            "elected_id": elected_id,
            "elected_rank": elected_rank,
        }
    )
    save_state(state)

    print(f"[{SERVER_ID}][ELECTION_RESULT] coordenador={elected_id} rank={elected_rank}", flush=True)
    return elected_id


def synchronize_clock_with_coordinator(context: zmq.Context, pub_socket: zmq.Socket, reason: str) -> None:
    coordinator_id = get_current_coordinator()

    if coordinator_id == SERVER_ID:
        print(
            f"[{SERVER_ID}][BERKELEY_COORDINATOR] servidor_atual_eh_coordenador mensagem={reason}",
            flush=True,
        )
        return

    try:
        response = server_rpc_request(
            context,
            coordinator_id,
            "CLOCK_REQUEST",
            reason=reason,
        )

        if response.get("status") != "OK":
            raise RuntimeError(response.get("error", "Resposta inválida do coordenador."))

        coordinator_epoch_ms = response.get("coordinator_epoch_ms")
        if not isinstance(coordinator_epoch_ms, int):
            raise RuntimeError("Coordenador não retornou coordinator_epoch_ms inteiro.")

        update_physical_clock_from_coordinator(coordinator_epoch_ms, coordinator_id)

        state = load_state()
        state["clock_syncs"].append(
            {
                "timestamp": corrected_now_iso(),
                "logical_clock": LOGICAL_CLOCK.current(),
                "reason": reason,
                "coordinator_id": coordinator_id,
                "coordinator_epoch_ms": coordinator_epoch_ms,
                "status": "OK",
            }
        )
        save_state(state)

        print(f"[{SERVER_ID}][BERKELEY_SYNC] coordenador={coordinator_id} status=OK", flush=True)

    except Exception as exc:
        print(
            f"[{SERVER_ID}][COORDINATOR_UNAVAILABLE] coordenador={coordinator_id} motivo={exc}",
            flush=True,
        )
        elected_id = elect_coordinator(context, pub_socket, f"coordenador_indisponivel:{coordinator_id}")

        if elected_id != SERVER_ID:
            try:
                response = server_rpc_request(
                    context,
                    elected_id,
                    "CLOCK_REQUEST",
                    reason=f"apos_eleicao:{reason}",
                )
                if response.get("status") == "OK" and isinstance(response.get("coordinator_epoch_ms"), int):
                    update_physical_clock_from_coordinator(response["coordinator_epoch_ms"], elected_id)
                    print(f"[{SERVER_ID}][BERKELEY_SYNC] coordenador={elected_id} status=OK", flush=True)
            except Exception as sync_exc:
                print(
                    f"[{SERVER_ID}][BERKELEY_SYNC] falha_apos_eleicao coordenador={elected_id} motivo={sync_exc}",
                    flush=True,
                )


def maybe_sync_clock(context: zmq.Context, pub_socket: zmq.Socket) -> None:
    if EXCHANGED_MESSAGE_COUNT == 0:
        return

    if EXCHANGED_MESSAGE_COUNT % CLOCK_SYNC_EVERY_MESSAGES != 0:
        return

    synchronize_clock_with_coordinator(
        context,
        pub_socket,
        f"mensagens_trocadas:{EXCHANGED_MESSAGE_COUNT}",
    )


def ok_response(request: dict, **extra) -> dict:
    base = {
        "type": "SERVER_RESULT",
        "request_id": request.get("request_id"),
        "timestamp": corrected_now_iso(),
        "logical_clock": LOGICAL_CLOCK.tick(),
        "status": "OK",
        "server_id": SERVER_ID,
        "server_rank": SERVER_RANK,
        "current_coordinator_id": get_current_coordinator(),
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
        "current_coordinator_id": get_current_coordinator(),
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
            "server_logical_clock": LOGICAL_CLOCK.current(),
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
        "current_coordinator_id": get_current_coordinator(),
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


def handle_request(request: dict, pub_socket: zmq.Socket, context: zmq.Context) -> dict:
    global CLIENT_MESSAGE_COUNT, EXCHANGED_MESSAGE_COUNT

    LOGICAL_CLOCK.update_from_message(request)

    request_type = request.get("type")

    if request_type in BROKER_REQUEST_TYPES:
        EXCHANGED_MESSAGE_COUNT += 1

    if request_type in CLIENT_REQUEST_TYPES:
        CLIENT_MESSAGE_COUNT += 1
        send_heartbeat_if_needed(context)

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
    start_background_threads(context)

    rpc_socket = context.socket(zmq.DEALER)
    rpc_socket.setsockopt(zmq.IDENTITY, SERVER_ID.encode())
    rpc_socket.connect(BACKEND_ENDPOINT)

    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(PUBSUB_PROXY_IN_ENDPOINT)

    time.sleep(0.5)

    if SERVER_RANK == 1:
        set_current_coordinator(SERVER_ID, "initial_rank_1")
        publish_coordinator_announcement(pub_socket, SERVER_ID, "initial_rank_1")

    register_message = {
        "type": "REGISTER_SERVER",
        "server_id": SERVER_ID,
        "server_rank": SERVER_RANK,
        "current_coordinator_id": get_current_coordinator(),
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

        maybe_sync_clock(context, pub_socket)


if __name__ == "__main__":
    main()