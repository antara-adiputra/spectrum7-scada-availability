import asyncio, gc, os, queue
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import managers, Manager
from typing import Any, Dict, List, Callable, Generator, Literal, TypeAlias, Union

from nicegui import ui

from .. import settings


async def run_cpu_bound(fn, *fnargs, **fnkwargs):
	try:
		loop = asyncio.get_running_loop()
	except RuntimeError:
		loop = asyncio.get_event_loop()
	n = min(os.cpu_count(), settings.MAX_CPU_USAGE)
	try:
		with ProcessPoolExecutor(n) as proc:
			obj = await loop.run_in_executor(proc, partial(fn, *fnargs, **fnkwargs))
		return obj
	except Exception as e:
		ui.notify('Error: Terjadi error pada "Multiprocessing".\r\n'+e.args[0], type='negative', position='bottom-left')
	finally:
		gc.collect()
