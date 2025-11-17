import asyncio, os, time
from concurrent.futures import ProcessPoolExecutor
from glob import glob
from io import BytesIO

import pandas as pd

from .base import BaseWithProgress, DataModel, DataTable, ExceptionMessage
from ..lib import load_workbook, logprint, read_xml, run_background
from ..types import *


FilePaths: TypeAlias = List[str]
FileDict: TypeAlias = Dict[str, BytesIO]
FileInput: TypeAlias = Union[str, FilePaths, FileDict]
ErrorCount: TypeAlias = int


class FileReader(BaseWithProgress):

	def __init__(self, *models: Type[DataModel], files: Optional[FileInput] = None, **kwargs):
		super().__init__(**kwargs)
		self._data_models = models
		self._file_buffer: FileDict = dict()
		self._duration: float = 0
		self._loaded: bool = None
		if files is not None:
			self.filenames = self._setup(files)
			self.sources = ',\n'.join(self.filenames)

		self.error_count: int = 0

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

	def _set_loaded(self, value: Optional[bool] = None):
		self._loaded = value
		self.set_wrapper_attr('loaded', value)

	def reset(self):
		self._file_buffer = dict()
		self._duration = 0
		self.filenames = list()
		self.sources = ''
		self.error_count = 0
		self._set_loaded()
		self.set_progress(value=0.0, message='')

	def open_file(self, file: str, **kwargs) -> Optional[pd.DataFrame]:
		"""Open single file into dataframe.

		Args:
			file : file source to be opened
		"""
		df: pd.DataFrame = None
		sheet_name = kwargs.pop('sheet_name', None)
		try:
			wb = load_workbook(self._file_buffer.get(file, file))
			# Try to check
			if isinstance(sheet_name, str):
				if sheet_name in wb:
					ws = wb[sheet_name]
					for model in self._data_models:
						if model.validate_schema(ws):
							logprint(f'Using {model.__name__} to handle data in file "{file}" on sheet "{sheet_name}"', 'info')
							df = model.validate_dataframe(ws, **kwargs)
							logprint(f'Open "{file}" SUCCESS', level='info')
							break

					if df is None:
						logprint(f'Data in sheet "{sheet_name}" does not have valid schema', level='warning')
				else:
					logprint(f'Sheet "{sheet_name}" is not found in file "{file}". Try to scan through all sheets', level='warning')

			# Continue to check all sheets if no files satisfied
			if df is None:
				for ws_name, ws_data in wb.items():
					# Loop through workbook sheets & match header
					for model in self._data_models:
						if model.validate_schema(ws_data):
							logprint(f'Using {model.__name__} to handle data in file "{file}" on sheet "{ws_name}"', 'info')
							df = model.validate_dataframe(ws_data, **kwargs)
							logprint(f'Open "{file}" SUCCESS', level='info')
							break

				if df is None:
					logprint(f'None data in file "{file}" has valid schema', level='warning')

		except ValueError as err:
			logprint(', '.join(err.args), level='error')
			try:
				logprint(f'Attempting to open file "{file}" with xml format', level='info')
				# Attempting to open file with xml format
				dfxml = read_xml(file, **kwargs)
				for model in self._data_models:
					if model.validate_schema(df):
						logprint(f'Using {model.__name__} to handle data in file "{file}"', 'info')
						df = model.validate_dataframe(dfxml, **kwargs)
						logprint(f'Open "{file}" SUCCESS', level='info')
						break
			except (ValueError, ImportError):
				logprint(f'Open "{file}" FAILED', level='error')
		except FileNotFoundError:
			logprint(f'File "{file}" is not found!', level='error')

		return df

	def _open_files(self, files: FilePaths, **kwargs) -> Tuple[FilePaths, List[pd.DataFrame], ErrorCount]:
		"""Open multiple files with Threads.

		Args:
			files : list of files
		"""
		file_list: FilePaths = list()
		data_list = list()
		error = 0

		for i, file in enumerate(files):
			table = self.open_file(file, **kwargs)
			file_list.append(files[i])
			if isinstance(table, pd.DataFrame):
				data_list.append(self.post_open_file(table))
			else:
				error += 1

		return file_list, data_list, error

	def _open_concurrently(self, files: FilePaths, **kwargs) -> Tuple[FilePaths, List[pd.DataFrame], ErrorCount]:
		"""Optimize file loading with ProcessPoolExecutor.

		Args:
			files : list of files
		"""
		def done(ftr: asyncio.Future):
			nonlocal file_list, data_list, error_count
			fpaths, table, errcount = ftr.result()
			# print(table, '-->', datatable)
			file_list.extend(fpaths)
			data_list.extend(table)
			error_count += errcount
			self.set_progress(
				value=len(file_list)/len(self.filenames),
				message=f'Membuka file... [{len(file_list)}/{len(self.filenames)}]'
			)

		file_list: FilePaths = list()
		data_list: List[pd.DataFrame] = list()
		error_count: int = 0
		n = os.cpu_count()
		chunksize = len(files)//n + 1
		self.set_progress(
			value=0.0,
			message=f'Membuka file... [{len(file_list)}/{len(self.filenames)}]'
		)

		with ProcessPoolExecutor(n) as ppe:
			futures = list()

			for i in range(0, len(files), chunksize):
				segment = files[i:(i+chunksize)]
				future = ppe.submit(self._open_files, segment, **kwargs)
				future.add_done_callback(done)
				futures.append(future)

		self.set_progress(
			value=1.0,
			message=f'Berhasil membuka {len(self.filenames)} file.'
		)
		return file_list, data_list, error_count

	async def _async_open_concurrently(self, files: FilePaths, **kwargs) -> Tuple[FilePaths, List[pd.DataFrame], ErrorCount]:
		"""Optimize file loading with ProcessPoolExecutor asynchronously.

		Args:
			files : list of files
		"""
		def done(ftr: asyncio.Future):
			nonlocal file_list, data_list, error_count
			fpaths, table, errcount = ftr.result()
			file_list.extend(fpaths)
			data_list.append(table)
			error_count += errcount
			self.set_progress(
				value=len(file_list)/len(self.filenames),
				message=f'Membuka file... [{len(file_list)}/{len(self.filenames)}]'
			)

		file_list: FilePaths = list()
		data_list: List[pd.DataFrame] = list()
		error_count: int = 0
		n = os.cpu_count()
		chunksize = len(files)//n + 1
		self.set_progress(
			value=0.0,
			message=f'Membuka file... [{len(file_list)}/{len(self.filenames)}]'
		)

		tasks: set = set()
		executor = ProcessPoolExecutor(n)
		for i in range(0, len(self.filenames), chunksize):
			segment = self.filenames[i:(i+chunksize)]
			task = asyncio.create_task(
				run_background(executor, self._open_files, segment, **kwargs)
			)
			task.add_done_callback(done)
			tasks.add(task)

		await asyncio.gather(*tasks)
		tasks.clear()
		executor.shutdown()
		self.set_progress(
			value=1.0,
			message=f'Berhasil membuka {len(self.filenames)} file.'
		)

		return file_list, data_list, error_count

	def _concat_data(self, dflist: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
		""""""
		if len(dflist)>0 and all(map(lambda x: isinstance(x, pd.DataFrame), dflist)):
			# return pd.concat([df.dropna(axis=1, how='all') for df in dflist])\
			return pd.concat(dflist)\
			.drop_duplicates(keep='last')\
			.reset_index(drop=True)
		else:
			return

	def load(self, sheet_name: Optional[str] = None, **kwargs) -> Optional[pd.DataFrame]:
		"""Load each file in filepaths into dataframe."""
		count = len(self.filenames)
		if count==0:
			logprint(f'Filenames undefined / not set')
			# self._errors.append(ExceptionMessage(type_='LoadFileError', message='Input file belum ditentukan!', data={'count': count}))
			return

		time_start = time.time()

		_, data_list, error_count = self._open_concurrently(self.filenames, sheet_name=sheet_name, **kwargs)
		df_result = self._concat_data(data_list)
		loaded = isinstance(df_result, pd.DataFrame)

		delta_time = time.time() - time_start
		self._duration = delta_time
		self._set_loaded(loaded)
		self.error_count = error_count

		logprint(f'Loading {count} file(s) completed in {delta_time:.3f}s. error={error_count}', level='info')
		if loaded:
			return self.post_load(df_result, **kwargs)
		else:
			return None

	async def async_load(self, sheet_name: Optional[str] = None, **kwargs) -> Optional[pd.DataFrame]:
		"""Load each file in filepaths asynchronously & concurrently into dataframe.
		
		Modified on 17-04-2025 to support file loading progress.
		"""
		count = len(self.filenames)
		if count==0:
			# self._errors.append(ExceptionMessage(type_='LoadFileError', message='Input file belum ditentukan!', data={'count': count}))
			return

		time_start = time.time()

		_, data_list, error_count = await self._async_open_concurrently(self.filenames, sheet_name=sheet_name, **kwargs)
		df_result = self._concat_data(data_list)
		loaded = isinstance(df_result, pd.DataFrame)

		delta_time = time.time() - time_start
		self._duration = delta_time
		self._set_loaded(loaded)
		self.error_count = error_count

		logprint(f'Loading {count} file(s) completed in {delta_time:.3f}s. error={error_count}', level='info')
		if loaded:
			return self.post_load(df_result, **kwargs)
		else:
			return None

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

	def set_file(self, files: FileInput) -> None:
		"""Set/change current file input."""
		self.reset()
		self.filenames = self._setup(files)
		self.sources: str = ',\n'.join(self.filenames)

	@property
	def loaded(self) -> bool:
		return self._loaded

