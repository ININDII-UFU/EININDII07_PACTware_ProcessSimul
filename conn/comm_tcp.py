import socket
import threading
import time
from typing import Optional, Callable


class CommTcp:
    """
    Servidor TCP/IP com interface compatível com CommSerial.
    Escuta em host:port, aceita UMA conexão de cliente por vez
    e oferece leitura assíncrona via thread + callback.
    Ideal para simular um dispositivo HART-IP acessível pela rede.
    """

    def __init__(self):
        self._server_sock: Optional[socket.socket] = None
        self._client_sock: Optional[socket.socket] = None
        self._accept_thread: Optional[threading.Thread] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._reader_callback: Optional[Callable[[bytes], None]] = None
        self._stop_event = threading.Event()
        self._host: str = ""
        self._port: int = 0
        self._lock = threading.Lock()

    # ---------------------- PROPRIEDADES ----------------------
    @property
    def is_open(self) -> bool:
        """Retorna True se há um cliente conectado."""
        with self._lock:
            if self._client_sock is None:
                return False
            try:
                self._client_sock.getpeername()
                return True
            except (OSError, AttributeError):
                return False

    @property
    def is_listening(self) -> bool:
        """Retorna True se o servidor está escutando."""
        return self._server_sock is not None

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    # ---------------------- ACCEPT LOOP ----------------------
    def _accept_loop(self):
        """Loop que aceita conexões de clientes (uma por vez)."""
        while not self._stop_event.is_set():
            try:
                self._server_sock.settimeout(1.0)
                client, addr = self._server_sock.accept()
                print(f"TCP HART: cliente conectado de {addr}")
                client.settimeout(0.5)

                # fecha cliente anterior, se houver
                with self._lock:
                    if self._client_sock is not None:
                        try:
                            self._client_sock.close()
                        except OSError:
                            pass
                    self._client_sock = client

                # inicia leitura desse cliente
                self._start_reader()

            except socket.timeout:
                continue
            except OSError:
                if not self._stop_event.is_set():
                    break

    # ---------------------- LEITURA ASSÍNCRONA ----------------------
    def _start_reader(self):
        """Inicia/reinicia a thread de leitura para o cliente atual."""
        # se já há uma thread rodando, ela vai parar ao detectar o novo socket
        if self._reader_thread is not None and self._reader_thread.is_alive():
            return  # o loop já está rodando e vai pegar o novo _client_sock
        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()

    def _reader_loop(self):
        """Loop que lê dados do cliente e chama o callback."""
        while not self._stop_event.is_set():
            with self._lock:
                sock = self._client_sock
            if sock is None:
                time.sleep(0.05)
                continue
            try:
                data = sock.recv(4096)
                if data:
                    if self._reader_callback:
                        self._reader_callback(data)
                else:
                    # cliente desconectou
                    print("TCP HART: cliente desconectou.")
                    with self._lock:
                        try:
                            sock.close()
                        except OSError:
                            pass
                        self._client_sock = None
            except socket.timeout:
                continue
            except OSError:
                with self._lock:
                    self._client_sock = None

    # ---------------------- ABRIR/FECHAR/IO ----------------------
    def open_tcp(self, host: str, port: int,
                 func_read: Optional[Callable[[bytes], None]] = None) -> bool:
        """
        Inicia o servidor TCP escutando em host:port.
        Retorna True se o bind+listen teve sucesso.
        """
        try:
            self.close_tcp()

            self._host = host
            self._port = port
            self._stop_event.clear()

            self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server_sock.bind((host, port))
            self._server_sock.listen(1)

            if func_read is not None:
                self._reader_callback = func_read

            # thread que aceita conexões
            self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
            self._accept_thread.start()

            print(f"TCP HART: servidor escutando em {host}:{port}")
            return True
        except OSError as e:
            print(f"Erro ao iniciar servidor TCP {host}:{port} – {e}")
            self._server_sock = None
            return False

    def close_tcp(self) -> bool:
        """Fecha o servidor e qualquer cliente conectado."""
        self._stop_event.set()

        with self._lock:
            if self._client_sock is not None:
                try:
                    self._client_sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    self._client_sock.close()
                except OSError:
                    pass
                self._client_sock = None

        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except OSError:
                pass
            self._server_sock = None

        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2)
        if self._reader_thread is not None:
            self._reader_thread.join(timeout=1)

        return True

    def read_tcp(self) -> bytes:
        """Lê dados disponíveis do cliente (não bloqueante)."""
        with self._lock:
            sock = self._client_sock
        if sock is not None:
            try:
                sock.setblocking(False)
                data = sock.recv(4096)
                sock.setblocking(True)
                sock.settimeout(0.5)
                return data
            except BlockingIOError:
                sock.setblocking(True)
                sock.settimeout(0.5)
                return b''
            except OSError:
                return b''
        return b''

    def write_tcp(self, write_data: bytes) -> bool:
        """
        Envia bytes para o cliente conectado.
        Retorna True se todos os bytes foram enviados.
        """
        with self._lock:
            sock = self._client_sock
        if sock is not None and write_data:
            try:
                sock.sendall(write_data)
                return True
            except OSError as e:
                print(f"Erro ao escrever TCP: {e}")
        return False


# --- Exemplo de uso ---
if __name__ == "__main__":
    def callback(data: bytes):
        print("Recebido TCP:", data)

    comm = CommTcp()
    host = "127.0.0.1"
    port = 5094

    if comm.open_tcp(host=host, port=port, func_read=callback):
        print(f"Conectado a {host}:{port}")
        time.sleep(0.5)

        if comm.write_tcp(b'\xFF\xFF\x06\x00'):
            print("Dados enviados com sucesso.")
        else:
            print("Falha ao enviar dados.")

        time.sleep(2)
        comm.close_tcp()
        print("Conexão TCP fechada.")
    else:
        print(f"Falha ao conectar em {host}:{port}")
