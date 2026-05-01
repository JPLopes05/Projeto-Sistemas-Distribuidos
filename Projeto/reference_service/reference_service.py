import json
import os
import time
from typing import Any, Dict

import msgpack
import zmq


REFERENCE_BIND = os.getenv("REFERENCE_BIND", "tcp://*:5560")
SERVER_ORDER = [
    item.strip()
    for item in os.getenv(
        "SERVER_ORDER",
        "js_server_1,js_server_2,py_server_1,py_server_2",
    ).split(",")
    if item.strip()
]
HEARTBEAT_TIMEOUT_SECONDS = int(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "35"))


class LogicalClock:
    def __init__(self) -> None:
        self.value = 0

    def tick(self) -> int:
        self.value += 1
        return self.value

    def update_from_message(self, message: dict) -> int:
        received = message.get("logical_clock")

        if isinstance(received, int):
            self.value = max(self.value, received)
        elif isinstance(received, str) and received.isdigit():
            self.value = max(self.value, int(received))

        return self.value


LOGICAL_CLOCK = LogicalClock()
SERVER_RANKS: Dict[str, int] = {}
ACTIVE_SERVERS: Dict[str, Dict[str, Any]] = {}


def now_epoch_ms() -> int:
    return int(time.time() * 1000)


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def encode_message(message: dict) -> bytes:
    return msgpack.packb(message, use_bin_type=True)


def decode_message(payload: bytes) -> dict:
    return msgpack.unpackb(payload, raw=False)


def log_message(direction: str, message: dict) -> None:
    print(
        f"[REFERENCE_SERVICE][{direction}] {json.dumps(message, ensure_ascii=False, sort_keys=True)}",
        flush=True,
    )


def cleanup_inactive_servers() -> None:
    now_ms = now_epoch_ms()
    timeout_ms = HEARTBEAT_TIMEOUT_SECONDS * 1000

    inactive = [
        server_id
        for server_id, data in ACTIVE_SERVERS.items()
        if now_ms - data.get("last_heartbeat_epoch_ms", 0) > timeout_ms
    ]

    for server_id in inactive:
        print(
            f"[REFERENCE_SERVICE] Servidor removido por falta de heartbeat: {server_id}",
            flush=True,
        )
        del ACTIVE_SERVERS[server_id]


def get_rank(server_id: str) -> int:
    if server_id in SERVER_RANKS:
        return SERVER_RANKS[server_id]

    if server_id in SERVER_ORDER:
        rank = SERVER_ORDER.index(server_id) + 1
    else:
        rank = max(SERVER_RANKS.values(), default=0) + 1

    SERVER_RANKS[server_id] = rank
    return rank


def active_servers_list() -> list:
    cleanup_inactive_servers()

    return sorted(
        [
            {
                "server_id": server_id,
                "rank": data["rank"],
                "last_heartbeat": data["last_heartbeat"],
            }
            for server_id, data in ACTIVE_SERVERS.items()
        ],
        key=lambda item: item["rank"],
    )


def base_reply(request: dict, status: str, include_reference_time: bool = False, **extra) -> dict:
    response = {
        "type": "REFERENCE_REPLY",
        "request_id": request.get("request_id"),
        "request_type": request.get("type"),
        "status": status,
        "timestamp": now_iso(),
        "logical_clock": LOGICAL_CLOCK.tick(),
    }

    if include_reference_time:
        response["reference_timestamp_epoch_ms"] = now_epoch_ms()

    response.update(extra)
    return response


def handle_get_rank(request: dict) -> dict:
    server_id = str(request.get("server_id", "")).strip()

    if not server_id:
        return base_reply(request, "ERROR", error="server_id obrigatório.")

    rank = get_rank(server_id)
    heartbeat_time = now_iso()

    ACTIVE_SERVERS[server_id] = {
        "rank": rank,
        "last_heartbeat": heartbeat_time,
        "last_heartbeat_epoch_ms": now_epoch_ms(),
    }

    return base_reply(
        request,
        "OK",
        server_id=server_id,
        rank=rank,
        active_servers=active_servers_list(),
    )


def handle_list_servers(request: dict) -> dict:
    return base_reply(
        request,
        "OK",
        active_servers=active_servers_list(),
    )


def handle_heartbeat(request: dict) -> dict:
    server_id = str(request.get("server_id", "")).strip()

    if not server_id:
        return base_reply(request, "ERROR", error="server_id obrigatório.")

    rank = get_rank(server_id)
    heartbeat_time = now_iso()

    ACTIVE_SERVERS[server_id] = {
        "rank": rank,
        "last_heartbeat": heartbeat_time,
        "last_heartbeat_epoch_ms": now_epoch_ms(),
    }

    return base_reply(
        request,
        "OK",
        server_id=server_id,
        rank=rank,
        active_servers=active_servers_list(),
    )


def handle_request(request: dict) -> dict:
    LOGICAL_CLOCK.update_from_message(request)

    request_type = request.get("type")

    if request_type == "GET_RANK":
        return handle_get_rank(request)

    if request_type == "LIST_SERVERS":
        return handle_list_servers(request)

    if request_type == "HEARTBEAT":
        return handle_heartbeat(request)

    return base_reply(
        request,
        "ERROR",
        error=f"Tipo de requisição inválido: {request_type}",
    )


def main() -> None:
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(REFERENCE_BIND)

    print(f"[REFERENCE_SERVICE] Serviço de referência em {REFERENCE_BIND}", flush=True)

    while True:
        payload = socket.recv()
        request = decode_message(payload)
        log_message("RECV", request)

        response = handle_request(request)
        log_message("SEND", response)
        socket.send(encode_message(response))


if __name__ == "__main__":
    main()