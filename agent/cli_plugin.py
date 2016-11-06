import sys
import time
import select
import tempfile
import threading
import subprocess

if sys.version < (3, 0, 0):
    import Queue as queue
else:
    import queue


mod_name = "cli"
__version__ = (1, 0)


class Proc(object):
    "Background process class"

    STDOUT = 0
    STDERR = 1
    EXIT_CODE = 2
    term_timeout = 1
    kill_timeout = 1

    RUNNING = 0
    TERM_SEND = 1
    KILL_SEND = 2

    def __init__(self, cmd, timeout, input_data=None, merge_out=True):
        self.input_data = input_data
        self.cmd = cmd
        self.timeout = timeout
        self.merge_out = merge_out

        self.proc = None
        self.end_time = None
        self.input_file = None
        self.output_q = None
        self.state = None

    def spawn(self):
        if self.input_data:
            self.input_file = tempfile.TemporaryFile(prefix="inp_data")

        if self.merge_out:
            stderr = subprocess.STDOUT
        else:
            stderr = subprocess.PIPE

        self.proc = subprocess.Popen(self.cmd,
                                     shell=isinstance(self.cmd, str),
                                     stdout=subprocess.PIPE,
                                     stderr=stderr,
                                     stdin=self.input_file)
        self.state = self.RUNNING
        self.output_q = queue.Queue()

        if self.timeout:
            self.end_time = time.time() + self.timeout

        if self.input_data:
            self.input_file.write(self.input_data)
            self.input_file.close()

        watch_th = threading.Thread(target=self.watch_proc_th)
        watch_th.daemon = True
        watch_th.start()

    def on_timeout(self):
        if self.state == self.RUNNING:
            self.term()
            self.end_time = time.time() + self.term_timeout
        elif self.state == self.TERM_SEND:
            self.kill()
            self.end_time = time.time() + self.kill_timeout
        else:
            assert self.state == self.KILL_SEND
            raise RuntimeError("Can't kill process")

    def watch_proc_th(self):
        if self.merge_out:
            all_pipes = [self.proc.stdout]
        else:
            all_pipes = [self.proc.stdout, self.proc.stderr]

        # set non-blocking

        while all_pipes:
            if self.end_time is not None:
                timeout = self.end_time - time.time()
            else:
                timeout = None

            r, _, e = select.select(all_pipes, [], all_pipes, timeout)
            if e != []:
                pass

            if not r:
                self.on_timeout()

            for pipe in r:
                data = pipe.read(1024)
                if len(data) == 0:
                    all_pipes.remove(pipe)
                    pipe.close()
                code = self.STDOUT if pipe is self.proc.stdout else self.STDERR
                self.output_q.put((code, data))

        if self.end_time is not None:
            self.proc.wait()
        else:
            self.proc.poll()
            while self.proc.returncode is None:
                while time.time() < self.end_time:
                    if self.proc.poll() is not None:
                        break
                    time.sleep(0.1)
                self.timeout()

        self.output_q.put((self.EXIT_CODE, self.proc.returncode))

    def get_updates(self):
        stdout_data = ""
        stderr_data = ""
        code = None
        while not self.output_q.empty():
            code, data = self.output_q.get()
            if code == self.STDOUT:
                stdout_data += data
            elif code == self.STDERR:
                stderr_data += data
            else:
                code = data
        return code, stdout_data, stderr_data

    def term(self):
        self.proc.terminate()

    def kill(self):
        self.proc.kill()


procs_lock = threading.Lock()
proc_id = 0
procs = {}


def rpc_spawn(cmd, timeout=None, input_data=None):
    global proc_id

    proc = Proc(cmd, timeout, input_data)
    proc.spawn()

    with procs_lock:
        curr_id = proc_id
        proc_id += 1
        procs[curr_id] = proc

    return curr_id


def rpc_get_updates(proc_id):
    with procs_lock:
        proc = procs[proc_id]

    ecode, d_out, d_err = proc.get_updates()

    if ecode is not None:
        with procs_lock:
            del procs[proc_id]
    return ecode, d_out, d_err


