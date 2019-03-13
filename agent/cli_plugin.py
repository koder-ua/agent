from __future__ import print_function

import time
import zlib
import errno
import select
import signal
import logging
import tempfile
import threading
import subprocess


from agent_module import queue, noraise


mod_name = "cli"
__version__ = (1, 0)


logger = logging.getLogger("agent.cli")


class Proc(object):
    """Background process class"""

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

        if isinstance(cmd, unicode):
            cmd = cmd.encode("utf8")

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
        try:
            if self.state == self.RUNNING:
                self.term()
                self.end_time = time.time() + self.term_timeout
            elif self.state == self.TERM_SEND:
                self.kill()
                self.end_time = time.time() + self.kill_timeout
            else:
                assert self.state == self.KILL_SEND
                raise RuntimeError("Can't kill process")
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                pass

    def watch_proc_th(self):
        output_size = {self.STDOUT: 0, self.STDERR: 0}

        def pipe_thread(pipe, code):
            while True:
                data = pipe.read(1024)
                if not data:
                    return
                output_size[code] += len(data)
                self.output_q.put((code, data))

        ths = [threading.Thread(target=pipe_thread, args=(self.proc.stdout, self.STDOUT))]

        if not self.merge_out:
            ths.append(threading.Thread(target=pipe_thread, args=(self.proc.stderr, self.STDERR)))

        for th in ths:
            th.daemon = True
            th.start()

        while True:
            for th in ths:
                try:
                    th.join(timeout=self.end_time - time.time())
                except:
                    self.on_timeout()
                    break
            else:
                break

        if self.end_time is None:
            self.proc.wait()
        else:
            self.proc.poll()
            while self.proc.returncode is None:
                while time.time() < self.end_time:
                    if self.proc.poll() is not None:
                        break
                    time.sleep(0.1)
                self.on_timeout()

        self.output_q.put((self.EXIT_CODE, self.proc.returncode))
        logger.debug("Proc %r returns %s and provides %s bytes of output", self.cmd, self.proc.returncode, output_size)

    def get_updates(self):
        stdout_data = ""
        stderr_data = ""
        code = None
        while not self.output_q.empty():
            msg_code, data = self.output_q.get()
            if msg_code == self.STDOUT:
                assert code is None, "Data after exit_code"
                stdout_data += data
            elif msg_code == self.STDERR:
                assert code is None, "Data after exit_code"
                stderr_data += data
            elif msg_code == self.EXIT_CODE:
                assert code is None, "Exit code after exit_code"
                code = data
            else:
                assert False, "Unknown typecode {0}".format(msg_code)
        return code, stdout_data, stderr_data

    def term(self):
        self.proc.terminate()

    def kill(self, signal=signal.SIGKILL):
        self.proc.send_signal(signal)


procs_lock = threading.Lock()
proc_id = 0
procs = {}


@noraise
def rpc_spawn(cmd, timeout=None, input_data=None, merge_out=False):
    global proc_id

    logger.info("CMD start requested: %s", cmd)
    proc = Proc(cmd, timeout, input_data, merge_out=merge_out)
    proc.spawn()

    with procs_lock:
        curr_id = proc_id
        proc_id += 1
        procs[curr_id] = proc

    return curr_id


@noraise
def rpc_killall(signal=signal.SIGKILL):
    logger.info("Signal %s is requested for all procs", signal)
    with procs_lock:
        for proc in procs.values():
            try:
                proc.kill(signal)
            except:
                pass


@noraise
def rpc_kill(proc_id, signal=signal.SIGKILL):
    with procs_lock:
        proc = procs[proc_id]
    logger.info("Signal %s is requested for %s", signal, proc.cmd)
    proc.kill(signal)


@noraise
def rpc_get_updates(proc_id, compress_limit=None):
    with procs_lock:
        proc = procs[proc_id]

    ecode, d_out, d_err = proc.get_updates()

    if ecode is not None:
        with procs_lock:
            del procs[proc_id]

    if compress_limit is not None:
        cumulative_out = d_out + d_err
        if len(cumulative_out) > compress_limit:
            return ecode, True, zlib.compress(cumulative_out), len(d_out)
        return ecode, False, cumulative_out, len(d_out)
    return ecode, d_out, d_err
