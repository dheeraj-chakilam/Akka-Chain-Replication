#!/usr/bin/env python
"""
The master program for CS5414 chain replication
"""

import os
import signal
import subprocess
import sys
import time
from socket import SOCK_STREAM, socket, AF_INET
from threading import Thread

leader = -1  # coordinator
address = 'localhost'
threads = {}
live_list = {}
crash_later = []


class ClientHandler(Thread):
    def __init__(self, index, address, port, process):
        Thread.__init__(self)
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((address, port))
        self.buffer = ""
        self.valid = True
        self.process = process

    def run(self):
        global threads
        while self.valid:
            if "\n" in self.buffer:
                (l, rest) = self.buffer.split("\n", 1)
                self.buffer = rest
                s = l.split()
                if len(s) < 2:
                    continue
                if s[0] == 'resp' or s[0] == 'snapshot':
                    sys.stdout.write(s[1] + '\n')
                    sys.stdout.flush()
                elif s[0] == 'ack':
                    pass
                else:
                    print s
            else:
                try:
                    data = self.sock.recv(1024)
                    # sys.stderr.write(data)
                    self.buffer += data
                except:
                    # print sys.exc_info()
                    self.valid = False
                    del threads[self.index]
                    self.sock.close()
                    break

    def kill(self):
        if self.valid:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
            except:
                pass
            self.close()

    def send(self, s):
        if self.valid:
            self.sock.send(str(s) + '\n')

    def close(self):
        try:
            self.valid = False
            self.sock.close()
        except:
            pass


def send(index, data):
    global live_list, threads
    pid = int(index)
    if pid not in threads:
        print 'Master or testcase error!'
        return
    threads[pid].send(data)
    return

def exit():
    global threads

    time.sleep(2)
    for k in threads:
        threads[k].kill()
    time.sleep(0.1)
    os._exit(0)


def timeout():
    time.sleep(120)
    exit()


def main(debug=False):
    global threads, crash_later
    timeout_thread = Thread(target=timeout, args=())
    timeout_thread.setDaemon(True)
    timeout_thread.start()
    gpid = 0

    while True:
        line = ''
        try:
            line = sys.stdin.readline()
        except:  # keyboard exception, such as Ctrl+C/D
            exit()
        if line == '':  # end of a file
            exit()
        line = line.strip()  # remove trailing '\n'

        sp = line.split(None, 1)
        cmd = sp[0]  # first field is the command
        if cmd == 'addReplica':
            pid = gpid
            port = int(sp[1])
            live_list[pid] = True

            if debug:
                process = subprocess.Popen(['./process', str(pid), str(port)], preexec_fn=os.setsid)
            else:
                process = subprocess.Popen(['./process', str(pid), str(port)], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'), preexec_fn=os.setsid)

            # sleep for a while to allow the process be ready
            time.sleep(2)

            # connect to the port of the pid
            handler = ClientHandler(pid, address, port, process)
            threads[pid] = handler
            handler.start()
            # sleep for a while to allow the coordinator inform the master
            time.sleep(0.1)
            # increase pid for the next command
            gpid += 1
        elif cmd == 'get':
            pid = sorted(live_list.keys())[0]
            send(pid, line)
        elif cmd == 'add' or cmd == 'delete' or cmd == 'snapshot':
            pid = sorted(live_list.keys())[-1]
            send(pid, line)
            for c in crash_later:
                live_list[c] = False
            crash_later = []
        elif cmd == 'crash':
            pid = sorted(live_list.keys(), reverse=True)[int(sp[1])]
            send(pid, sp[0])
            live_list[pid] = False
        elif cmd[:5] == 'crash':
            pid = sorted(live_list.keys(), reverse=True)[int(sp[1])]
            send(pid, sp[0])
            crash_later.append(pid)
        time.sleep(2)

if __name__ == '__main__':
    debug = False
    if len(sys.argv) > 1 and sys.argv[1] == 'debug':
        debug = True

    main(debug)
