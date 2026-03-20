import json
import os
import time
from typing import Dict, List, Optional, Tuple

import msgpack
import zmq


FRONTEND_BIND = os.getenv("FRONTEND_BIND", "tcp://*:5555")
BACKEND_BIND = os.getenv("BACKEND_BIND", "tcp://*:5556")
EXPECTED_SERVERS = int(os.getenv("EXPECTED_SERVERS", "4"))
SERVER_RPC_TIMEOUT_MS = int(os.getenv("SERVER_RPC_TIMEOUT_MS", "8000"))


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def encode_message(message: dict) -> bytes:
    return msgpack.packb(message, use_bin_type=True)


def decode_message(payload: bytes) -> dict:
    return msgpack.unpackb(payload, raw=False)


def log_message(direction: str, peer: str, message: dict) -> None:
    print(
        f"[BROKER][{direction}][{peer}] {json.dumps(message, ensure_ascii=False, sort_keys=True)}",
        flush=True,
    )


def client_reply(status: str, request: dict, **extra) -> dict:
    base = {
        "type": "BROKER_REPLY",
        "request_id": request.get("request_id"),
        "timestamp": now_iso(),
        "status": status,
        "request_type": request.get("type"),
    }
    base.update(extra)
    return base


class Broker:
    def __init__(self) -> None:
        self.context = zmq.Context()
        self.frontend = self.context.socket(zmq.ROUTER)
        self.backend = self.context.socket(zmq.ROUTER)

        self.frontend.bind(FRONTEND_BIND)
        self.backend.bind(BACKEND_BIND)

        self.registered_servers: List[str] = []
        self.rr_index = 0

        print(f"[BROKER] Frontend em {FRONTEND_BIND}", flush=True)
        print(f"[BROKER] Backend em {BACKEND_BIND}", flush=True)

    def ensure_registered(self, server_id: str) -> None:
        if server_id not in self.registered_servers:
            self.registered_servers.append(server_id)
            self.registered_servers.sort()
            print(
                f"[BROKER] Servidor registrado: {server_id} | total={len(self.registered_servers)}",
                flush=True,
            )

    def recv_backend_any(self, timeout_ms: int) -> Optional[Tuple[str, dict]]:
        poller = zmq.Poller()
        poller.register(self.backend, zmq.POLLIN)
        events = dict(poller.poll(timeout_ms))
        if self.backend not in events:
            return None

        frames = self.backend.recv_multipart()
        server_id = frames[0].decode()
        payload = frames[-1]
        message = decode_message(payload)
        log_message("RECV", server_id, message)

        if message.get("type") == "REGISTER_SERVER":
            self.ensure_registered(server_id)

        return server_id, message

    def drain_registrations(self) -> None:
        while True:
            result = self.recv_backend_any(0)
            if result is None:
                break

    def send_to_server(self, server_id: str, message: dict) -> None:
        log_message("SEND", server_id, message)
        self.backend.send_multipart([server_id.encode(), encode_message(message)])

    def choose_server(self) -> Optional[str]:
        if not self.registered_servers:
            return None
        server_id = self.registered_servers[self.rr_index % len(self.registered_servers)]
        self.rr_index += 1
        return server_id

    def request_single_server(self, request: dict, server_id: str) -> Optional[dict]:
        self.send_to_server(server_id, request)
        deadline = time.time() + (SERVER_RPC_TIMEOUT_MS / 1000)

        while time.time() < deadline:
            remaining_ms = max(1, int((deadline - time.time()) * 1000))
            result = self.recv_backend_any(remaining_ms)
            if result is None:
                break

            recv_server_id, message = result
            if message.get("type") == "REGISTER_SERVER":
                continue

            if recv_server_id == server_id and message.get("request_id") == request.get("request_id"):
                return message

        return None

    def request_all_servers(self, request: dict) -> Dict[str, dict]:
        expected = list(self.registered_servers)
        for server_id in expected:
            self.send_to_server(server_id, request)

        responses: Dict[str, dict] = {}
        deadline = time.time() + (SERVER_RPC_TIMEOUT_MS / 1000)

        while len(responses) < len(expected) and time.time() < deadline:
            remaining_ms = max(1, int((deadline - time.time()) * 1000))
            result = self.recv_backend_any(remaining_ms)
            if result is None:
                break

            server_id, message = result
            if message.get("type") == "REGISTER_SERVER":
                continue

            if server_id in expected and message.get("request_id") == request.get("request_id"):
                responses[server_id] = message

        return responses

    def process_request(self, request: dict) -> dict:
        if len(self.registered_servers) < EXPECTED_SERVERS:
            return client_reply(
                "ERROR",
                request,
                error=f"Broker ainda não está pronto. Servidores registrados: {len(self.registered_servers)}/{EXPECTED_SERVERS}",
            )

        request_type = request.get("type")

        if request_type == "LIST_CHANNELS":
            server_id = self.choose_server()
            if server_id is None:
                return client_reply("ERROR", request, error="Nenhum servidor disponível.")

            response = self.request_single_server(request, server_id)
            if response is None:
                return client_reply(
                    "ERROR",
                    request,
                    error=f"Timeout ao consultar o servidor {server_id}.",
                )

            if response.get("status") != "OK":
                return client_reply(
                    "ERROR",
                    request,
                    error=response.get("error", "Erro desconhecido no servidor."),
                    server_id=server_id,
                )

            return client_reply(
                "OK",
                request,
                channels=response.get("channels", []),
                server_id=server_id,
            )

        if request_type in {"LOGIN", "CREATE_CHANNEL"}:
            responses = self.request_all_servers(request)

            if len(responses) != len(self.registered_servers):
                missing = sorted(set(self.registered_servers) - set(responses.keys()))
                return client_reply(
                    "ERROR",
                    request,
                    error=f"Timeout aguardando respostas dos servidores: {', '.join(missing)}",
                )

            for server_id, response in responses.items():
                if response.get("status") != "OK":
                    return client_reply(
                        "ERROR",
                        request,
                        error=response.get("error", "Erro desconhecido no servidor."),
                        failed_server=server_id,
                    )

            return client_reply(
                "OK",
                request,
                replicated_servers=sorted(responses.keys()),
            )

        return client_reply("ERROR", request, error=f"Tipo de requisição inválido: {request_type}")

    def reply_to_client(self, client_frames: List[bytes], message: dict) -> None:
        client_id = client_frames[0].decode(errors="ignore")
        log_message("SEND", client_id, message)
        self.frontend.send_multipart([client_frames[0], b"", encode_message(message)])

    def run(self) -> None:
        poller = zmq.Poller()
        poller.register(self.frontend, zmq.POLLIN)
        poller.register(self.backend, zmq.POLLIN)

        while True:
            events = dict(poller.poll(500))

            if self.backend in events:
                result = self.recv_backend_any(0)
                if result is not None:
                    server_id, message = result
                    if message.get("type") != "REGISTER_SERVER":
                        print(
                            f"[BROKER] Resposta fora de contexto recebida de {server_id}. Ignorando.",
                            flush=True,
                        )

            if self.frontend in events:
                client_frames = self.frontend.recv_multipart()
                payload = client_frames[-1]
                request = decode_message(payload)
                client_id = client_frames[0].decode(errors="ignore")
                log_message("RECV", client_id, request)

                self.drain_registrations()
                response = self.process_request(request)
                self.reply_to_client(client_frames, response)


if __name__ == "__main__":
    Broker().run()
