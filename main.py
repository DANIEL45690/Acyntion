import os
import sys
import time
import threading
import queue
import json
import hashlib
import shutil
import subprocess
import socket
import pickle
import struct
import logging
import signal
import psutil
from datetime import datetime
from collections import deque
import random
import string
import re
import zipfile
import tempfile
import stat

class ProcessSync:
    def __init__(self):
        self.processes = {}
        self.sync_events = {}
        self.message_queue = queue.Queue()
        self.running = True
        self.sync_mode = "parallel"
        self.history = deque(maxlen=50)
        self.lock = threading.Lock()
        self.network_peers = []
        self.logs = []
        self.backup_path = tempfile.mkdtemp()
        self.perf_data = {}

    def display_header(self):
        os.system('cls' if os.name == 'nt' else 'clear')
        print("=" * 60)
        print()
        print("                                             d8   ,e,                   ")
        print("  /~~~8e   e88~~\\ Y88b  / 888-~88e  e88~~\\ _d88__  \"   e88~-_  888-~88e ")
        print("      88b d888     Y888/  888  888 d888     888   888 d888   i 888  888 ")
        print(" e88~-888 8888      Y8/   888  888 8888     888   888 8888   | 888  888 ")
        print("C888  888 Y888       Y    888  888 Y888     888   888 Y888   ' 888  888 ")
        print(" \"88_-888  \"88__/   /     888  888  \"88__/  \"88_/ 888  \"88_-~  888  888 ")
        print("                  _/")
        print()
        print("=" * 60)
        print("[1] ADD PROCESS")
        print("[2] START SYNC")
        print("[3] STOP SYNC")
        print("[4] LIST PROCESSES")
        print("[5] REMOVE PROCESS")
        print("[6] SYNC MODE")
        print("[7] PROCESS STATUS")
        print("[8] SUSPEND PROCESS")
        print("[9] RESUME PROCESS")
        print("[10] RESTART PROCESS")
        print("[11] HISTORY VIEW")
        print("[12] BACKUP STATE")
        print("[13] RESTORE STATE")
        print("[14] NETWORK SYNC")
        print("[15] PERF MONITOR")
        print("[16] LOG VIEWER")
        print("[17] CLEAN LOGS")
        print("[18] EXPORT DATA")
        print("[19] IMPORT DATA")
        print("[20] VALIDATE PROCS")
        print("[21] AUTO RECOVERY")
        print("[22] BATCH ADD")
        print("[23] RESOURCE LIMIT")
        print("[24] DEPENDENCY CHECK")
        print("[25] EXIT")
        print("=" * 60)

    def function_01_add_process(self):
        print("\n[!] ADD PROCESS")
        name = input("[?] Process name: ")
        command = input("[?] Command to execute: ")
        if name not in self.processes:
            self.processes[name] = {
                'command': command,
                'status': 'idle',
                'pid': None,
                'sync_group': 'default',
                'restart_count': 0,
                'cpu_usage': 0,
                'mem_usage': 0
            }
            self.sync_events[name] = threading.Event()
            self.message_queue.put(f"[1] Process '{name}' added")
        else:
            self.message_queue.put(f"[!] Process '{name}' exists")

    def function_02_start_sync(self):
        print("\n[2] START SYNC")
        group = input("[?] Sync group name (default): ") or "default"
        processes_to_start = [p for p, data in self.processes.items() if data['sync_group'] == group]
        if self.sync_mode == "parallel":
            threads = []
            for proc in processes_to_start:
                t = threading.Thread(target=self.start_process, args=(proc,))
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
        else:
            for proc in processes_to_start:
                self.start_process(proc)
                time.sleep(0.5)
        self.message_queue.put(f"[2] Started {len(processes_to_start)} processes in group '{group}'")

    def function_03_stop_sync(self):
        print("\n[3] STOP SYNC")
        group = input("[?] Sync group name (default): ") or "default"
        stopped = 0
        for name, data in self.processes.items():
            if data['sync_group'] == group and data['status'] == 'running':
                self.stop_process(name)
                stopped += 1
        self.message_queue.put(f"[3] Stopped {stopped} processes")

    def function_04_list_processes(self):
        print("\n[4] PROCESS LIST")
        print("-" * 50)
        for name, data in self.processes.items():
            print(f"Name: {name}")
            print(f"Status: {data['status']}")
            print(f"Group: {data['sync_group']}")
            print(f"CPU: {data['cpu_usage']}% MEM: {data['mem_usage']}MB")
            print("-" * 30)
        input("\n[!] Press Enter to continue")

    def function_05_remove_process(self):
        print("\n[5] REMOVE PROCESS")
        name = input("[?] Process name: ")
        if name in self.processes:
            if self.processes[name]['status'] == 'running':
                self.stop_process(name)
            del self.processes[name]
            del self.sync_events[name]
            self.message_queue.put(f"[5] Process '{name}' removed")
        else:
            self.message_queue.put(f"[!] Process '{name}' not found")

    def function_06_sync_mode(self):
        print("\n[6] SYNC MODE")
        print("[1] Parallel")
        print("[2] Sequential")
        choice = input("[?] Select mode: ")
        self.sync_mode = "parallel" if choice == "1" else "sequential"
        self.message_queue.put(f"[6] Sync mode set to {self.sync_mode}")

    def function_07_process_status(self):
        print("\n[7] PROCESS STATUS")
        name = input("[?] Process name: ")
        if name in self.processes:
            data = self.processes[name]
            print(f"Name: {name}")
            print(f"Status: {data['status']}")
            print(f"Command: {data['command']}")
            print(f"Group: {data['sync_group']}")
            print(f"Restarts: {data['restart_count']}")
            print(f"CPU: {data['cpu_usage']}%")
            print(f"Memory: {data['mem_usage']}MB")
        else:
            print("[!] Process not found")
        input("\n[!] Press Enter to continue")

    def function_08_suspend_process(self):
        print("\n[8] SUSPEND PROCESS")
        name = input("[?] Process name: ")
        if name in self.processes and self.processes[name]['status'] == 'running':
            self.processes[name]['status'] = 'suspended'
            self.message_queue.put(f"[8] Process '{name}' suspended")

    def function_09_resume_process(self):
        print("\n[9] RESUME PROCESS")
        name = input("[?] Process name: ")
        if name in self.processes and self.processes[name]['status'] == 'suspended':
            self.processes[name]['status'] = 'running'
            self.message_queue.put(f"[9] Process '{name}' resumed")

    def function_10_restart_process(self):
        print("\n[10] RESTART PROCESS")
        name = input("[?] Process name: ")
        if name in self.processes:
            if self.processes[name]['status'] == 'running':
                self.stop_process(name)
                time.sleep(1)
            self.start_process(name)
            self.processes[name]['restart_count'] += 1
            self.message_queue.put(f"[10] Process '{name}' restarted")

    def function_11_history_view(self):
        print("\n[11] HISTORY VIEW")
        print("-" * 50)
        for i, entry in enumerate(self.history, 1):
            print(f"{i}: {entry}")
        if not self.history:
            print("[!] No history available")
        input("\n[!] Press Enter to continue")

    def function_12_backup_state(self):
        print("\n[12] BACKUP STATE")
        backup_file = f"backup_{int(time.time())}.json"
        with open(backup_file, 'w') as f:
            json.dump(self.processes, f, default=str)
        shutil.copy(backup_file, self.backup_path)
        self.message_queue.put(f"[12] Backup saved as {backup_file}")

    def function_13_restore_state(self):
        print("\n[13] RESTORE STATE")
        backups = [f for f in os.listdir('.') if f.startswith('backup_')]
        if not backups:
            print("[!] No backups found")
            return
        for i, b in enumerate(backups, 1):
            print(f"[{i}] {b}")
        choice = input("[?] Select backup: ")
        try:
            with open(backups[int(choice)-1], 'r') as f:
                self.processes = json.load(f)
            self.message_queue.put("[13] State restored successfully")
        except:
            self.message_queue.put("[!] Restore failed")

    def function_14_network_sync(self):
        print("\n[14] NETWORK SYNC")
        print("[1] Start server")
        print("[2] Connect to peer")
        choice = input("[?] Select option: ")
        if choice == "1":
            self.start_server()
        else:
            host = input("[?] Peer IP: ")
            self.connect_to_peer(host)

    def function_15_perf_monitor(self):
        print("\n[15] PERFORMANCE MONITOR")
        for name, data in self.processes.items():
            if data['pid'] and psutil.pid_exists(data['pid']):
                proc = psutil.Process(data['pid'])
                data['cpu_usage'] = proc.cpu_percent(interval=0.1)
                data['mem_usage'] = proc.memory_info().rss / 1024 / 1024
        print("-" * 50)
        print(f"{'Process':<20} {'CPU%':<10} {'MEM(MB)':<10} {'Status':<10}")
        print("-" * 50)
        for name, data in self.processes.items():
            print(f"{name:<20} {data['cpu_usage']:<10.1f} {data['mem_usage']:<10.1f} {data['status']:<10}")
        input("\n[!] Press Enter to continue")

    def function_16_log_viewer(self):
        print("\n[16] LOG VIEWER")
        for log in self.logs[-20:]:
            print(log)
        if not self.logs:
            print("[!] No logs available")
        input("\n[!] Press Enter to continue")

    def function_17_clean_logs(self):
        print("\n[17] CLEAN LOGS")
        self.logs.clear()
        self.message_queue.put("[17] Logs cleaned")

    def function_18_export_data(self):
        print("\n[18] EXPORT DATA")
        filename = f"export_{int(time.time())}.json"
        export_data = {
            'processes': self.processes,
            'sync_mode': self.sync_mode,
            'timestamp': str(datetime.now())
        }
        with open(filename, 'w') as f:
            json.dump(export_data, f, default=str)
        self.message_queue.put(f"[18] Data exported to {filename}")

    def function_19_import_data(self):
        print("\n[19] IMPORT DATA")
        filename = input("[?] File to import: ")
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
            self.processes.update(data.get('processes', {}))
            self.sync_mode = data.get('sync_mode', 'parallel')
            self.message_queue.put("[19] Data imported successfully")
        except:
            self.message_queue.put("[!] Import failed")

    def function_20_validate_procs(self):
        print("\n[20] VALIDATE PROCESSES")
        valid_count = 0
        for name, data in self.processes.items():
            if data['command'] and data['command'].strip():
                valid_count += 1
            else:
                self.logs.append(f"Invalid command for {name}")
        self.message_queue.put(f"[20] Valid processes: {valid_count}/{len(self.processes)}")

    def function_21_auto_recovery(self):
        print("\n[21] AUTO RECOVERY")
        for name, data in self.processes.items():
            if data['status'] == 'running' and (data['pid'] is None or not psutil.pid_exists(data['pid'])):
                self.start_process(name)
                self.logs.append(f"Auto recovered {name}")
        self.message_queue.put("[21] Auto recovery completed")

    def function_22_batch_add(self):
        print("\n[22] BATCH ADD")
        print("[!] Enter process names and commands (name:command)")
        print("[!] Type 'done' to finish")
        while True:
            entry = input("[?] > ")
            if entry.lower() == 'done':
                break
            if ':' in entry:
                name, cmd = entry.split(':', 1)
                self.function_01_add_process()
            else:
                print("[!] Invalid format")

    def function_23_resource_limit(self):
        print("\n[23] RESOURCE LIMIT")
        name = input("[?] Process name: ")
        cpu_limit = input("[?] CPU limit (%): ")
        mem_limit = input("[?] Memory limit (MB): ")
        if name in self.processes:
            self.processes[name]['cpu_limit'] = float(cpu_limit) if cpu_limit else None
            self.processes[name]['mem_limit'] = float(mem_limit) if mem_limit else None
            self.message_queue.put(f"[23] Limits set for {name}")

    def function_24_dependency_check(self):
        print("\n[24] DEPENDENCY CHECK")
        for name, data in self.processes.items():
            deps = data.get('depends_on', [])
            missing = []
            for dep in deps:
                if dep not in self.processes:
                    missing.append(dep)
            if missing:
                self.logs.append(f"{name} missing deps: {missing}")
        self.message_queue.put("[24] Dependency check complete")

    def function_25_exit(self):
        print("\n[25] EXIT")
        for name in list(self.processes.keys()):
            if self.processes[name]['status'] == 'running':
                self.stop_process(name)
        self.running = False
        sys.exit(0)

    def start_process(self, name):
        if name in self.processes and self.processes[name]['status'] != 'running':
            self.processes[name]['status'] = 'running'
            self.processes[name]['start_time'] = datetime.now()
            self.history.append(f"Started {name} at {datetime.now()}")
            self.logs.append(f"[{datetime.now()}] Process {name} started")

    def stop_process(self, name):
        if name in self.processes:
            self.processes[name]['status'] = 'stopped'
            self.processes[name]['pid'] = None
            self.history.append(f"Stopped {name} at {datetime.now()}")
            self.logs.append(f"[{datetime.now()}] Process {name} stopped")

    def start_server(self):
        server_thread = threading.Thread(target=self.run_server)
        server_thread.daemon = True
        server_thread.start()
        self.message_queue.put("[14] Server started on port 9999")

    def run_server(self):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.bind(('0.0.0.0', 9999))
            server.listen(5)
            while self.running:
                client, addr = server.accept()
                data = pickle.loads(client.recv(4096))
                self.processes.update(data)
                client.close()
        except:
            pass

    def connect_to_peer(self, host):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((host, 9999))
            pickle.dump(self.processes, client)
            client.close()
            self.message_queue.put(f"[14] Synced with {host}")
        except:
            self.message_queue.put("[!] Connection failed")

    def run(self):
        while self.running:
            self.display_header()
            while not self.message_queue.empty():
                print(self.message_queue.get())
            choice = input("\n[?] Select option (1-25): ")
            functions = {
                '1': self.function_01_add_process,
                '2': self.function_02_start_sync,
                '3': self.function_03_stop_sync,
                '4': self.function_04_list_processes,
                '5': self.function_05_remove_process,
                '6': self.function_06_sync_mode,
                '7': self.function_07_process_status,
                '8': self.function_08_suspend_process,
                '9': self.function_09_resume_process,
                '10': self.function_10_restart_process,
                '11': self.function_11_history_view,
                '12': self.function_12_backup_state,
                '13': self.function_13_restore_state,
                '14': self.function_14_network_sync,
                '15': self.function_15_perf_monitor,
                '16': self.function_16_log_viewer,
                '17': self.function_17_clean_logs,
                '18': self.function_18_export_data,
                '19': self.function_19_import_data,
                '20': self.function_20_validate_procs,
                '21': self.function_21_auto_recovery,
                '22': self.function_22_batch_add,
                '23': self.function_23_resource_limit,
                '24': self.function_24_dependency_check,
                '25': self.function_25_exit
            }
            if choice in functions:
                functions[choice]()
            else:
                print("[!] Invalid option")
                time.sleep(1)

if __name__ == "__main__":
    app = ProcessSync()
    app.run()
