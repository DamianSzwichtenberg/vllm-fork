###############################################################################
# Copyright (C) 2024 Habana Labs, Ltd. an Intel Company
###############################################################################

import json
import os
import queue
import threading
import time
from contextlib import contextmanager

class FileWriter(threading.Thread):

    def __init__(self, filename, event_queue):
        super().__init__()
        self.filename = filename
        self.event_queue = event_queue
        self.daemon = True
        self.timer_event = threading.Event()

    def _drain_event_queue(self):
        content = ''
        while True:
            try:
                element = self.event_queue.get_nowait()
                content += element
            except queue.Empty:
                break
        return content

    def run(self):
        # don't check the queue too often
        while not self.timer_event.wait(1):
            # Block and wait for the next item in the queue
            content = self.event_queue.get()
            # Collect any other items in the queue
            content += self._drain_event_queue()

            with open(self.filename, 'a') as outfile:
                outfile.write(content)


class Profiler:
    profiling_trace_events = queue.Queue()
    event_tid = {'counter': 1, 'external': 2, 'internal': 3}
    filename = 'server_events.json'

    def __init__(self):
        self.enabled = os.getenv('VLLM_PROFILER_ENABLED',
                                 'false').lower() == 'true' and int(
                                     os.getenv('RANK', '0')) == 0
        if self.enabled:
            # initialize the trace file (JSON Array Format)
            with open(self.filename, 'w') as outfile:
                outfile.write('[')
            file_writer = FileWriter(self.filename,
                                     self.profiling_trace_events)
            file_writer.start()

    def _s_to_us(self, time):
        return time * 1000000.0

    def _dump_with_sep(self, entry):
        entry = json.dumps(entry) + ','
        self.profiling_trace_events.put(entry)

    def _dump_counter(self, ts, counter):
        self._dump_with_sep({
            'pid': 1,
            'tid': self.event_tid['counter'],
            'ph': 'C',
            'name': 'utils',
            'ts': ts,
            'args': counter
        })

    @contextmanager
    def record_event(self, type, name, args=None):
        if self.enabled:
            start = self._s_to_us(time.time())
            if args is not None and 'counter' in args:
                self._dump_counter(start, args['counter'])
                del args['counter']

            event = {
                'pid': 1,
                'tid': self.event_tid[type],
                'ph': 'X',
                'name': name,
                'ts': start,
                'dur': None,
                'args': args
            }
            yield

            end = self._s_to_us(time.time())
            event['dur'] = end - start

            self._dump_with_sep(event)
        else:
            yield
