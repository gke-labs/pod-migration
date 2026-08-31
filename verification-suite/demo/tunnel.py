import socket
import subprocess
import threading
import sys
import json

def resolve_pod():
    print("[*] Locating active JupyterLab pod...")
    cmd = ["kubectl", "get", "pods", "-l", "app=jupyterlab", "-o", "json"]
    try:
        output = subprocess.check_output(cmd)
        data = json.loads(output)
        for item in data.get("items", []):
            if item.get("status", {}).get("phase") == "Running" and "deletionTimestamp" not in item["metadata"]:
                name = item["metadata"]["name"]
                ip = item["status"]["podIP"]
                print(f"[*] Found active pod: {name} (IP: {ip})")
                return name, ip
    except Exception as e:
        print(f"[!] Error resolving pod: {e}")
    raise Exception("Could not find a running JupyterLab pod. Please check deployment status.")

def handle_client(client_socket, addr, pod_name, pod_ip):
    print(f"[+] Connection from {addr[0]}:{addr[1]}")
    cmd = [
        "kubectl", "exec", "-i", pod_name, "-c", "jupyterlab", "--",
        "python3", "-c",
        "import socket,os,threading; s=socket.socket(); s.connect(('127.0.0.1', 8888)); "
        "threading.Thread(target=lambda: [s.sendall(d) for d in iter(lambda: os.read(0, 4096), b'')], daemon=True).start(); "
        "[os.write(1, d) for d in iter(lambda: s.recv(4096), b'')]"
    ]
    
    import os
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    stdin_fd = proc.stdin.fileno()
    stdout_fd = proc.stdout.fileno()

    def pipe_in(src, dst_fd):
        try:
            while True:
                data = src(4096)
                if not data:
                    break
                os.write(dst_fd, data)
        except Exception as e:
            pass

    def pipe_out(src_fd, dst):
        try:
            while True:
                data = os.read(src_fd, 4096)
                if not data:
                    break
                dst(data)
        except Exception as e:
            pass

    t1 = threading.Thread(target=pipe_in, args=(client_socket.recv, stdin_fd))
    t2 = threading.Thread(target=pipe_out, args=(stdout_fd, client_socket.sendall))
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
    client_socket.close()
    print(f"[-] Closed connection from {addr[0]}:{addr[1]}")

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('127.0.0.1', 8888))
    server.listen(5)
    print("==========================================================")
    print("Local Tunnel Active! Listening on http://localhost:8888 ...")
    print("Press Ctrl+C to terminate.")
    print("==========================================================")
    try:
        while True:
            client, addr = server.accept()
            try:
                pod_name, pod_ip = resolve_pod()
                threading.Thread(target=handle_client, args=(client, addr, pod_name, pod_ip), daemon=True).start()
            except Exception as e:
                print(f"[!] Failed to resolve active pod for connection: {e}")
                client.close()
    except KeyboardInterrupt:
        print("\nShutting down tunnel...")

if __name__ == '__main__':
    main()
