import asyncio, os, time
import multiprocessing as mp
import threading
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from glob import glob
from io import BytesIO
from queue import Queue

import pandas as pd

from .base import BaseWithProgress, DataModel, DataTable, ExceptionMessage, SerializableException
from . import params
from ..lib import load_workbook, logprint, read_xml, toggle_attr
from ..types import *
from .. import settings


FilePaths: TypeAlias = List[str]
FileDict: TypeAlias = Dict[str, BytesIO]
FileInput: TypeAlias = Union[str, FilePaths, FileDict]
ErrorCount: TypeAlias = int
# (<filename/filepath>, <size>, <status>)
# 0	= Success
# 1	= Error
# ...
FileQueueInfo: TypeAlias = Tuple[str, Optional[int], int]
FileDFPair: TypeAlias = Tuple[str, pd.DataFrame]


ERROR_CODE = [
	'OK',
	'INVALID DATA',
	'EXCEL ENGINE ERROR',
	'FILE NOT FOUND',
	'INTERNAL',
	'_',
	'UNKNOWN'
]


class FileReader(BaseWithProgress):

	def __init__(self, *models: Type[DataModel], files: Optional[FileInput] = None, **kwargs):
		super().__init__(**kwargs)
		self._data_models = models
		self._file_buffer: FileDict = dict()
		self._duration: float = 0
		self.loaded: Union[bool, None] = None
		self.loading_file: bool = False
		self.message_template = 'Membuka file {file}... [{count}/{total}]'
		self.processed_files: List[FileQueueInfo] = list()
		if files is None:
			self.filenames = list()
			self.sources = ''
		else:
			self.filenames = self._setup(files)
			self.sources = ',\n'.join(self.filenames)

	def _setup(self, files: FileInput) -> FilePaths:
		"""Get list of exact filepaths from given input."""
		filenames = list()
		if isinstance(files, (str, list)):
			filepaths = files if isinstance(files, list) else files.split(',')
			for filepath in filepaths:
				path = filepath.strip()
				if '*' in path:
					# Files defined with pathname patterns, use glob
					paths = glob(path)
					if len(paths)>0:
						filenames += paths
					else:
						exc = ExceptionMessage(type_='LoadFileError', message=f'File yang menyerupai "{path}" tidak ditemukan.', data={'file': path})
						# self._errors.append(exc)
						logprint(exc.message, level='error')
				elif path:
					filenames.append(path)
		elif isinstance(files, dict):
			filenames = list(files.keys())
			self._file_buffer = files

		return filenames

	def change_data_models(self, *models: Type[DataModel]):
		self._data_models = models

	def set_file(self, files: FileInput):
		"""Set/change current file input."""
		self.reset()
		self.filenames = self._setup(files)
		self.sources: str = ',\n'.join(self.filenames)

	def reset(self):
		self._duration = 0
		self.sources = ''
		self.loaded = None
		self._file_buffer.clear()
		self.filenames.clear()
		self.processed_files.clear()
		self.set_progress(value=0.0, message='')

	def open_file(self, file: str, **kwargs) -> Optional[pd.DataFrame]:
		"""Open single file into dataframe.

		Args:
			file : file source to be opened
		"""
		def validate(rawdf: pd.DataFrame, sheet: str = None):
			nonlocal status, df
			for model in self._data_models:
				if model.validate_schema(rawdf):
					logprint(f'Using {model.__name__} to handle data in file "{file}"' + f'on sheet "{sheet}"' if sheet else '')
					df = model.validate_dataframe(rawdf, **kwargs)
					status = 0
					logprint(f'Open "{file}" SUCCESS', level='info')
					break

		df: pd.DataFrame = None
		sheet_name = kwargs.pop('sheet_name', None)
		fsize = None	# in Byte
		status = -1
		queue: Queue = kwargs.get('queue')

		try:
			if file in self._file_buffer:
				iobuffer = self._file_buffer[file]
				fsize = len(iobuffer.getvalue())
				wb = load_workbook(iobuffer)
			else:
				fsize = os.path.getsize(file)
				wb = pd.read_excel(file, sheet_name=None, engine='openpyxl')

			# Try to check
			if isinstance(sheet_name, str):
				if sheet_name in wb:
					validate(wb[sheet_name], sheet_name)

					if df is None:
						logprint(f'Data in sheet "{sheet_name}" does not have valid schema', level='warning')
				else:
					logprint(f'Sheet "{sheet_name}" is not found in file "{file}". Try to scan through all sheets', level='warning')

			# Continue to check all sheets if no files satisfied
			if df is None:
				for ws_name, ws_data in wb.items():
					validate(ws_data, ws_name)

				if df is None:
					status = 1
					logprint(f'None data in file "{file}" has valid schema', level='warning')
					logprint(f'Open "{file}" FAILED', level='error')

		except ValueError as err:
			logprint(', '.join(err.args), level='error')
			try:
				logprint(f'Attempting to open file "{file}" with xml format', level='info')
				# Attempting to open file with xml format
				dfxml = read_xml(file, **kwargs)
				validate(dfxml)
				if df is None:
					status = 1
					logprint(f'Open "{file}" FAILED', level='error')

			except (ValueError, ImportError):
				status = 2
				logprint(f'Open "{file}" FAILED', level='error')
		except FileNotFoundError:
			status = 3
			logprint(f'File "{file}" is not found!', level='error')
		except Exception as exc:
			status = 4
			logprint(f'Unexpected error occured upon opening file "{file}"')

		if queue is not None:
			queue.put((file, fsize, status))

		return df

	def _open_single(self, file: str, queue: Optional[Queue] = None, **kwargs) -> FileDFPair:
		data = self.open_file(file, queue=queue, **kwargs)
		result = self.post_open_file(data) if isinstance(data, pd.DataFrame) else data
		return file, result

	async def _open_multiple(self, files: FilePaths, **kwargs) -> List[FileDFPair]:
		self.loading_file = True
		loop = asyncio.get_event_loop()
		# Get the number of workers
		n = min(settings.MAX_CPU_USAGE, os.cpu_count())
		log_callback = kwargs.pop('log_callback', None)
		with mp.Manager() as manager:
			# Define queue for shared memory between processes and threads
			queue = manager.Queue()
			# Create new thread for consuming queue
			# tc = loop.create_task(asyncio.to_thread(self._consume_queue, queue))
			t1 = threading.Thread(target=self._consume_queue, args=(queue, log_callback))
			t1.start()

			with ProcessPoolExecutor(n) as exc:
				tasks = list()
				for i in range(0, len(files), 1):
					tasks.append(loop.run_in_executor(exc, partial(self._open_single, **kwargs), files[i], queue))

				results = await asyncio.gather(*tasks, return_exceptions=True)

		self.loading_file = False
		return results

	def _consume_queue(self, queue: Queue, callback: Optional[Callable]):
		logprint(f'Mulai membuka {self.count} file...', level='info', cli=False, ext_func=callback, **params.INFOLOG_KWARGS)
		logprint('Start consuming queue...')
		self.set_progress(
			value=0.0,
			message=self.message_template.format(file='', count=0, total=self.count)
		)
		while self.loading_file:
			try:
				if not queue.empty():
					value: FileQueueInfo = queue.get()
					self.processed_files.append(value)
					self.set_progress(
						value=self.processed_count/self.count,
						message=self.message_template.format(file=value[0], count=self.processed_count, total=self.count)
					)
					# Push message into GUI log
					if value[2]==0:
						logprint(f'Berhasil membuka file {value[0]}.', level='info', cli=False, ext_func=callback, **params.INFOLOG_KWARGS)
					else:
						logprint(f'Gagal membuka file {value[0]}. kode={value[2]} ({ERROR_CODE[value[2]]})', level='error', cli=False, ext_func=callback, **params.ERRORLOG_KWARGS)
			except Exception:
				break

			# Important to define delay / sleep for better performance
			time.sleep(0.01)

		errors = self.errors
		message = f'Selesai membuka {self.count} file. error={len(errors)}'
		self.set_progress(
			value=1.0,
			message=message
		)
		logprint('Consuming queue completed. exit=0')
		logprint(message, level='info', cli=False, ext_func=callback, **params.INFOLOG_KWARGS)

	def load(self, files: Optional[FileInput] = None, sheet_name: Optional[str] = None, **kwargs):
		return asyncio.run(self.async_load(files=files, sheet_name=sheet_name, **kwargs))

	@toggle_attr('loading_file', True, False)
	async def async_load(self, files: Optional[FileInput] = None, sheet_name: Optional[str] = None, **kwargs) -> Optional[pd.DataFrame]:
		"""Load each file in filepaths asynchronously & concurrently into dataframe."""
		if files is None:
			if self.count==0:
				return
		else:
			self.set_file(files)

		data_list = list()
		errors = dict()
		result = None
		time_start = time.perf_counter()

		datas = await self._open_multiple(self.filenames, sheet_name=sheet_name, **kwargs)
		for data in datas:
			if isinstance(data, tuple):
				# Valid result
				if isinstance(data[1], pd.DataFrame):
					data_list.append(data[1])
				else:
					errors[data[0]] = data[1]
			else:
				# Error result
				errors[f'error{len(errors)+1}'] = str(data)

		df_result = self._concat_data(data_list)
		loaded = isinstance(df_result, pd.DataFrame)

		delta_time = time.perf_counter() - time_start
		self._duration = delta_time
		self.loaded = loaded
		if loaded:
			result = self.post_load(df_result, **kwargs)

		logprint(f'Loading {self.count} file(s) completed in {delta_time:.3f}s. error={len(errors)}', level='info')
		return result

	def _concat_data(self, dflist: List[pd.DataFrame]) -> Union[pd.DataFrame, SerializableException, None]:
		""""""
		result = None
		try:
			if len(dflist)>0:
				# return pd.concat([df.dropna(axis=1, how='all') for df in dflist])\
				result = pd.concat(dflist)\
				.drop_duplicates(keep='last')\
				.reset_index(drop=True)
		except Exception as err:
			result = SerializableException(err)
		finally:
			return result

	def post_open_file(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Process right after file opened.
		
		Args:
			df : dataframe
		"""
		return df

	def post_load(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
		"""Executed right after all files loaded.

		Args:
			df : Dataframe
		"""
		return df

	@property
	def data_models(self) -> Tuple[Type[DataModel]]:
		return self._data_models

	@property
	def count(self) -> int:
		return len(self.filenames)

	@property
	def processed_count(self) -> int:
		return len(self.processed_files)

	@property
	def errors(self) -> List[FileQueueInfo]:
		return [msg for msg in self.processed_files if msg[2]>0]
