import asyncio, gc, os, queue
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import managers, Manager
from typing import Any, Dict, List, Callable, Generator, Literal, TypeAlias, Union

from nicegui import background_tasks, ui


CalcOutputGen: TypeAlias = Generator[float, dict, dict]		# (percentage, state, result)


class BackgroundWorker:
	_manager: managers.SyncManager
	_queue: queue.Queue
	_event_callbacks: Dict[str, Callable]
	progress: float
	is_running: bool
	state: Dict[str, Any]
	result: List
	refresh_rate: float

	def __init__(self, refresh_rate: float = 0.1) -> None:
		self.refresh_rate = refresh_rate
		self._initiate()

	def _initiate(self) -> None:
		self._event_callbacks = dict()
		self.progress = 0.0
		self.state = dict()
		self.result = list()
		self.is_running = False
		self._create_queue()

	def _create_queue(self) -> None:
		self._manager = Manager()
		self._queue = self._manager.Queue()

	async def _consume_queue(self) -> None:
		self.is_running = True
		self.progress = 0.0

		while self.progress<1.0:
			try:
				out = self._queue.get_nowait()
			except queue.Empty:
				await asyncio.sleep(self.refresh_rate)
				continue
			self.progress = out['progress']
			self.state.update(out['state'])
			self.result.append(out['result'])

		await asyncio.sleep(2)
		self.is_running = False

	@staticmethod
	def _run_generator(queue: queue.Queue, func: Callable[..., CalcOutputGen], *funcargs, **funckwargs) -> None:
		for progress in func(*funcargs, **funckwargs):
			queue.put({
				'progress': progress[0],
				'state': progress[1],
				'result': progress[2]
			})

	def restart(self) -> None:
		self._initiate()

	async def run(self, func: Callable[..., CalcOutputGen], *funcargs, **funckwargs) -> None:
		background_tasks.create(run_cpu_bound(self._run_generator, self._queue, func, *funcargs, **funckwargs))
		background_tasks.create(self._consume_queue())


async def run_cpu_bound(fn, *fnargs, **fnkwargs):
	try:
		loop = asyncio.get_running_loop()
	except RuntimeError:
		loop = asyncio.get_event_loop()
	try:
		with ProcessPoolExecutor(os.cpu_count()) as proc:
			obj = await loop.run_in_executor(proc, partial(fn, *fnargs, **fnkwargs))
		return obj
	except Exception as e:
		ui.notify('Error: Terjadi error pada "Multiprocessing".\r\n'+e.args[0], type='negative', position='bottom-left')
	finally:
		gc.collect()