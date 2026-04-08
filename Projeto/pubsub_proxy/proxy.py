import os

import zmq


XSUB_BIND = os.getenv("XSUB_BIND", "tcp://*:5557")
XPUB_BIND = os.getenv("XPUB_BIND", "tcp://*:5558")


def main() -> None:
    context = zmq.Context()
    xsub = context.socket(zmq.XSUB)
    xpub = context.socket(zmq.XPUB)

    xsub.bind(XSUB_BIND)
    xpub.bind(XPUB_BIND)

    print(f"[PUBSUB_PROXY] XSUB em {XSUB_BIND}", flush=True)
    print(f"[PUBSUB_PROXY] XPUB em {XPUB_BIND}", flush=True)

    try:
        zmq.proxy(xsub, xpub)
    finally:
        xsub.close(0)
        xpub.close(0)
        context.term()


if __name__ == "__main__":
    main()