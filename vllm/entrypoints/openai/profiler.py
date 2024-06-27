###############################################################################
# Copyright (C) 2024 Habana Labs, Ltd. an Intel Company
###############################################################################

import asyncio
import aiofiles
import threading
import json
import os
import queue
import time
from contextlib import contextmanager

from vllm.logger import init_logger
from vllm.utils import get_vllm_instance_id

logger = init_logger(__name__)

class AsyncFileWriter:
    def __init__(self, filename, event_queue):
        self.filename = filename
        self.event_queue = event_queue

    async def _drain_event_queue(self):
        content = ''
        while True:
            try:
                element = self.event_queue.get_nowait()
                content += element
            except queue.Empty:
                break
        return content

    async def run(self):
        while True:
            content = await asyncio.get_event_loop().run_in_executor(None, self.event_queue.get)
            content += await self._drain_event_queue()
            async with aiofiles.open(self.filename, 'a') as outfile:
                await outfile.write(content)

class AsyncProfiler:
    profiling_trace_events = queue.Queue()
    event_tid = {'counter': 4, 'external': 5, 'internal': 6}
    vllm_instance_id = get_vllm_instance_id()
    filename = f'server_events_{vllm_instance_id}_engine.json'
    event_cache = []

    def __init__(self):
        self.enabled = os.getenv('VLLM_PROFILER_ENABLED', 'false').lower() == 'true' and int(os.getenv('RANK', '0')) == 0
        logger.info(f'Profiler enabled for: {self.vllm_instance_id}')
        if self.enabled:
            with open(self.filename, 'w') as outfile:
                outfile.write('[')
            self.file_writer = AsyncFileWriter(self.filename, self.profiling_trace_events)
            threading.Thread(target=self._start_file_writer_in_background, daemon=True).start()

    def _start_file_writer_in_background(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.file_writer.run())

    def _dump_with_sep(self, entry):
        entry = json.dumps(entry) + ','
        self.profiling_trace_events.put(entry)

    def get_timestamp_us(self):
        return time.time() * 1000000.0

    def start_sync(self, type, name, args=None):
        if self.enabled:
            ts = self.get_timestamp_us()
            if args is not None and 'counter' in args:
                self.record_counter_sync(ts, args['counter'])
                del args['counter']
            event = {
                'pid': 1,
                'tid': self.event_tid[type],
                'ph': 'X',
                'name': name,
                'ts': ts,
                'dur': None,
                'args': args
            }
            self.event_cache.append(event)

    def end_sync(self):
        if self.enabled:
            ts = self.get_timestamp_us()
            if not self.event_cache:
                logger.warning('Profiler: end() call does not have matching start() call. Disabling profiler.')
                self.enabled = False
                return
            event = self.event_cache.pop()
            event['dur'] = ts - event['ts']
            self._dump_with_sep(event)

    def record_counter_sync(self, ts, counter):
        if self.enabled:
            self._dump_with_sep({
                'pid': 1,
                'tid': self.event_tid['counter'],
                'ph': 'C',
                'name': 'engine',
                'ts': ts,
                'args': counter
            })

    # Async versions of the above methods
    async def start(self, type, name, args=None):
        await asyncio.to_thread(self.start_sync, type, name, args)

    async def end(self):
        await asyncio.to_thread(self.end_sync)

    async def record_counter(self, ts, counter):
        await asyncio.to_thread(self.record_counter_sync, ts, counter)

    @contextmanager
    async def record_event(self, type, name, args=None):
        if self.enabled:
            await self.start(type, name, args)
            yield
            await self.end()
        else:
            yield
