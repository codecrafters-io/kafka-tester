import socket
from binascii import hexlify
from concurrent.futures import ThreadPoolExecutor
from struct import pack, unpack
from typing import NamedTuple


class Request(NamedTuple):
    length: int
    api_key: int
    api_version: int
    correlation_id: int

    @staticmethod
    def from_bytes(raw: bytes) -> "Request":
        length, api_key, api_version, correlation_id = unpack("!ihhi", raw[:12])
        return Request(length, api_key, api_version, correlation_id)


def main(port=9092):
    server = socket.create_server(("localhost", port), reuse_port=True)
    print(f"✌️ Listening on port {port}.")

    with ThreadPoolExecutor() as executor:
        while True:
            connection, _ = server.accept()
            executor.submit(handle_connection, connection)


def handle_connection(connection) -> None:
    with connection:
        data = connection.recv(1024)
        print("Received:", data, hexlify(data))

        request = Request.from_bytes(data)
        print(request)

        hardcoded_apiversions_response = pack(
            "!iihbhhhbib",
            19 - 4,  # msg len
            request.correlation_id,  # 4 bytes
            0,  # error code,          2 bytes
            2,  # varint,              1 byte
            18,  # api key,            2 bytes
            0,  # api min version,     2 bytes
            4,  # api max version,     2 bytes
            0,  # tag,                 1 bytes
            0,  # throttle,            4 bytes
            0,  # tag,                 1 bytes
        )

        print("Sending:", hardcoded_apiversions_response)
        connection.sendall(hardcoded_apiversions_response)


if __name__ == "__main__":
    main()
