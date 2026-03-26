import serial
from typing import List, Optional, Callable
from conn.comm_serial import CommSerial
from conn.comm_tcp import CommTcp

DEFAULT_CFG = {"port":"COM1","baudrate":1200,"bytesize":8,"parity":"N","stopbits":1}

# Modos de comunicação suportados
MODE_SERIAL = "serial"
MODE_TCP = "tcp"


class HrtComm:
    def __init__(self, port: Optional[str] = None, func_read: Optional[Callable[[str], None]] = None,
                 mode: str = MODE_SERIAL):
        self._port: Optional[str] = port
        self.func_read: Optional[Callable[[str], None]] = func_read
        self._mode: str = mode
        self._comm_serial = CommSerial()
        self._comm_tcp = CommTcp()
        # Parâmetros TCP
        self._tcp_host: str = "127.0.0.1"
        self._tcp_port: int = 5094
        self.connect(port, func_read)

    # ---------------------- MODE ----------------------
    @property
    def mode(self) -> str:
        return self._mode

    @mode.setter
    def mode(self, value: str):
        if value not in (MODE_SERIAL, MODE_TCP):
            raise ValueError(f"Modo inválido: {value}. Use '{MODE_SERIAL}' ou '{MODE_TCP}'.")
        self._mode = value

    # ---------------------- PORT (serial) ----------------------
    @property
    def port(self) -> str:
        return self._port if self._port is not None else ""

    @port.setter
    def port(self, value: str):
        self._port = value

    # ---------------------- TCP params ----------------------
    @property
    def tcp_host(self) -> str:
        return self._tcp_host

    @tcp_host.setter
    def tcp_host(self, value: str):
        self._tcp_host = value

    @property
    def tcp_port(self) -> int:
        return self._tcp_port

    @tcp_port.setter
    def tcp_port(self, value: int):
        self._tcp_port = value

    # ---------------------- LISTAGEM ----------------------
    @property
    def available_ports(self) -> List[str]:
        return self._comm_serial.available_ports

    # ---------------------- R/W ----------------------
    def read_frame(self) -> str:
        if self._mode == MODE_TCP:
            resp = self._comm_tcp.read_tcp()
        else:
            resp = self._comm_serial.read_serial()
        return "".join([format(e, '02x').upper() for e in resp])

    @property
    def is_connected(self) -> bool:
        if self._mode == MODE_TCP:
            return self._comm_tcp.is_listening
        return self._comm_serial.is_open

    def write_frame(self, data: str) -> bool:
        aux = [int(data[i:i + 2], 16) for i in range(0, len(data), 2)]
        resp = bytes(aux)
        if self._mode == MODE_TCP:
            return self._comm_tcp.write_tcp(resp)
        return self._comm_serial.write_serial(resp)

    # ---------------------- CONNECT / DISCONNECT ----------------------
    def connect(self, port: Optional[str] = None, func_read: Optional[Callable[[str], None]] = None,
                host: Optional[str] = None, tcp_port: Optional[int] = None) -> bool:
        func_read_aux = func_read if func_read is not None else self.func_read

        def _hex_callback(data):
            if func_read_aux:
                func_read_aux(
                    "".join([format(e, '02x').upper() for e in data])
                )

        if self._mode == MODE_TCP:
            h = host or self._tcp_host
            p = tcp_port or self._tcp_port
            self._tcp_host = h
            self._tcp_port = p
            return self._comm_tcp.open_tcp(
                host=h,
                port=p,
                func_read=_hex_callback
            )
        else:
            if (port or self._port) is not None:
                return self._comm_serial.open_serial(
                    port or self._port,
                    baudrate=1200,
                    bytesize=8,
                    parity=serial.PARITY_ODD,
                    stopbits=serial.STOPBITS_ONE,
                    func_read=_hex_callback
                )
            return False

    def disconnect(self) -> bool:
        if self._mode == MODE_TCP:
            return self._comm_tcp.close_tcp()
        return self._comm_serial.close_serial()

def handle_data(data):
    print(f"Received data: {data}")

# Example Usage:
if __name__ == '__main__':
    # Example usage of HrtComm (replace "COM1" with your actual serial port)
    hrt_comm = HrtComm(func_read=handle_data)

    if hrt_comm.available_ports:
        port = hrt_comm.available_ports[0]
        print(f"Trying to connect to {port}")
        if hrt_comm.connect(port=port):
            print("Connected to serial port")
            frame_to_write = "0102030405"  # Example frame data
            if hrt_comm.write_frame(frame_to_write):
                print(f"Wrote frame: {frame_to_write}")
            else:
                print("Failed to write frame")
            # Data will be printed in handle_data function when received
            import time
            time.sleep(2) # wait for data
            hrt_comm.disconnect()
            print("Disconnected from serial port")
        else:
            print("Failed to connect to serial port")
    else:
        print("No serial ports found.")
