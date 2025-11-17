import asyncio, datetime, os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from functools import partial

import numpy as np
import pandas as pd

from .base import BaseWithProgress, CalculationState, Config, DataModel, DataTable, frozen_dataclass_set
from .filereader import FileReader
from .soe import SOEData, SOEModel, SurvalentSOEModel, SurvalentSPModel
from . import params
from ..lib import progress_bar, toggle_attr
from ..types import *


@dataclass(frozen=True)
class AvailabilityData:
	all: pd.DataFrame
	start_date: datetime.datetime = field(kw_only=True)
	end_date: datetime.datetime = field(kw_only=True)

	def __post_init__(self):
		pass


@dataclass(frozen=True, kw_only=True)
class AvailabilityResult:
	"""
	"""
	data: AvailabilityData
	date_min: datetime.datetime = field(init=False, default=None)
	date_max: datetime.datetime = field(init=False, default=None)
	total_periods: datetime.timedelta = field(init=False, default=None)

	def __post_init__(self):
		frozen_dataclass_set(
			self,
			date_min=self.data.start_date,
			date_max=self.data.end_date,
			total_periods=self.data.end_date - self.data.start_date + datetime.timedelta(microseconds=1),
		)


class AvailabilityCore(BaseWithProgress):
	topic: ClassVar[str] = ''
	subject: ClassVar[str] = ''
	model_class: Type[DataModel] = None
	result: DataTable = None

	def __init__(self, data: Optional[SOEData] = None, **kwargs):
		super().__init__(**kwargs)
		self._oridata: SOEData = None
		self.reset()
		self.set_data(data)

	def _set_analyzed(self, value: bool):
		self._analyzed = value
		self.set_wrapper_attr('analyzed', value)

	def reset(self):
		self._analyzed: bool = False
		self.key_items: List[AvKeys] = list()
		self.data_count: int = 0
		self.result = None
		if isinstance(self._oridata, SOEData):
			self.data = self._oridata.copy()
			if isinstance(self._oridata, pd.DataFrame):
				self.set_date_range(self._oridata.data['timestamp'].min(), self._oridata.data['timestamp'].max())

	def set_data(self, data: SOEData):
		"""Set data source to analyze."""
		if isinstance(data, SOEData):
			self._oridata = data.copy()
			self.data = data
			if isinstance(data.data, pd.DataFrame):
				self.set_date_range(data.data['timestamp'].min(), data.data['timestamp'].max())
		else:
			self._oridata = None
			self.data = None
			if not data is None:
				# Warn user about inappropriate data source
				logprint(f'Inappropriate data type "{type(data).__name__}", expected SOEData type. Data is set to None.', level='warning')

	def select_data(self) -> pd.DataFrame:
		return self.data.his

	def get_key_items(self, df: pd.DataFrame) -> List[AvKeys]:
		""""""
		return list()

	def get_data_count(self, df: pd.DataFrame) -> int:
		""""""
		return df.shape[0]

	def pre_analyze(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
		""""""
		start_date = kwargs.get('start_date')
		end_date = kwargs.get('end_date')
		self.set_date_range(start_date, end_date)

		df_pre = df[
			(df['timestamp']>=self.start_date) &
			(df['timestamp']<=self.end_date)
		]
		self.key_items = self.get_key_items(df_pre)
		self.data_count = self.get_data_count(df_pre)
		return df_pre

	def post_analyze(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
		""""""
		self._analyzed = True
		return df

	def main_func(self, df: pd.DataFrame, key: AvKeys, **kwargs) -> DataTable:
		""""""
		pass

	def _get_chunksize(self, cpu: int, limit: Union[int, None]) -> int:
		if limit is None or limit<1:
			# Dynamic limit
			return len(self.key_items)//cpu + 1
		else:
			# Defined
			return limit

	def _run_on_each_process(self, df: pd.DataFrame, keys: List[AvKeys], **kwargs) -> Dict[str, Any]:
		"""Analyze function executed on each process concurrently.

		Args:
			keys : list of key to be used on loop

		Result:
			Serialized type of dataclass
		"""
		result: DataTable = None
		for key in keys:
			curr_result = self.main_func(df, key, **kwargs)
			if result is None:
				result = curr_result
			else:
				result.merge(curr_result, inplace=True)

		# IMPORTANT NOTE: Data passed thorugh inter-processes must be serializable
		return result.dump()

	def _run_concurrently(
		self,
		df: pd.DataFrame,
		keys: List[AvKeys],
		nprocessor: int,
		limit_per_cpu: Union[int, None],
		**kwargs
	) -> DataTable:
		"""Run analyze with multiple Processes.

		Args:
			keys : list of key to be used on loop

		Result:
			DataTable
		"""
		result: DataTable = None
		progress_msg = kwargs.get('progress_message', '')
		callback = kwargs.get('callback')

		if callable(callback):
			cb = callback
		else:
			cb = progress_bar

		# ProcessPoolExecutor create new instance on different processes, so modifying instance in each process will not change instance in main process.
		# Value returned must be "serializable".
		chunksize = self._get_chunksize(cpu=nprocessor, limit=limit_per_cpu)
		with ProcessPoolExecutor(nprocessor) as ppe:
			futures = list()

			for i in range(0, len(keys), chunksize):
				key_segment = keys[i:(i+chunksize)]
				future = ppe.submit(self._run_on_each_process, df, key_segment)
				futures.append(future)

			for x, future in enumerate(as_completed(futures)):
				result_dict = future.result()
				result_ = self.model_class.validate(result_dict)
				if result is None:
					result = result_
				else:
					result.merge(result_, inplace=True)

				self.set_progress(value=result.count/self.data_count, message=progress_msg, show_percentage=True)
				# Call callback function
				cb(value=(x+1)/len(futures), name='analyze')

		return result

	def _run_synchronously(
		self,
		df: pd.DataFrame,
		keys: List[AvKeys],
		**kwargs
	) -> DataTable:
		"""Run analyze on Single Process.

		Args:
			keys : list of key to be used on loop

		Result:
			Serialized type of dataclass
		"""
		result: DataTable = None
		progress_msg = kwargs.get('progress_message', '')
		callback = kwargs.get('callback')

		if callable(callback):
			cb = callback
		else:
			cb = progress_bar

		for x, key in enumerate(keys):
			curr_result = self.main_func(df, key, **kwargs)
			if result is None:
				result = curr_result
			else:
				result.merge(curr_result, inplace=True)

			self.set_progress(value=result.count/self.data_count, message=progress_msg, show_percentage=True)
			# Call callback function
			cb(value=(x+1)/len(keys), name='analyze')

		return result

	def _convert_to_dataframe(self, output: DataTable, **kwargs) -> pd.DataFrame:
		self.result = output
		if isinstance(output, DataTable):
			# Create new DataFrame from list
			return output.to_dataframe()\
				.reset_index(drop=True)
		else:
			logprint(f'Expected "DataTable" type got {type(output)}.', level='error')
			return None

	def analyze(
		self,
		start_date: Optional[datetime.datetime] = None,
		end_date: Optional[datetime.datetime] = None,
		force: bool = False,
		**kwargs
	) -> Optional[pd.DataFrame]:
		"""Analyze function using native single Process.

		Result:
			Dataframe
		"""
		# Pre-analyze initialization
		if force:
			self.reset()

		df = self.pre_analyze(
			self.select_data(),
			start_date=start_date,
			end_date=end_date,
			**kwargs
		)
		progress_msg = f'\nMenganalisa {self.data_count} {self.topic}...'
		self.set_progress(value=0.0, message=progress_msg, show_percentage=True)
		print('\n' + progress_msg)

		# Execute given function
		result = self._run_synchronously(
			df=df,
			keys=self.key_items,
			progress_message=progress_msg,
			**kwargs
		)
		self.set_progress(value=1.0, message=f'Analisa {self.data_count} {self.topic} selesai.')
		df_result = self._convert_to_dataframe(result)
		return self.post_analyze(df_result, **kwargs)

	def fast_analyze(
		self,
		start_date: Optional[datetime.datetime] = None,
		end_date: Optional[datetime.datetime] = None,
		force: bool = False,
		nprocessor: int = os.cpu_count(),
		limit_per_cpu: Union[int, None] = None,
		**kwargs
	) -> pd.DataFrame:
		"""Optimized analyze function using multiple Processes.

		Result:
			Dataframe
		"""
		# Pre-analyze initialization
		if force:
			self.reset()

		df = self.pre_analyze(
			self.select_data(),
			start_date=start_date,
			end_date=end_date,
			**kwargs
		)
		progress_msg = f'\nMenganalisa {self.data_count} {self.topic}...'
		self.set_progress(value=0.0, message=progress_msg, show_percentage=True)
		print('\n' + progress_msg)

		# Execute given function
		result = self._run_concurrently(
			df=df,
			keys=self.key_items,
			nprocessor=nprocessor,
			limit_per_cpu=limit_per_cpu,
			progress_message=progress_msg,
			**kwargs
		)
		self.set_progress(value=1.0, message=f'Analisa {self.data_count} {self.topic} selesai.')
		df_result = self._convert_to_dataframe(result)
		return self.post_analyze(df_result, **kwargs)

	async def async_analyze(
		self,
		start_date: Optional[datetime.datetime] = None,
		end_date: Optional[datetime.datetime] = None,
		force: bool = False,
		nprocessor: int = os.cpu_count(),
		limit_per_cpu: Union[int, None] = None,
		**kwargs
	) -> pd.DataFrame:
		"""Asynchronous function using multiple Process to work concurrently.

		Result:
			Dataframe
		"""
		async def run_background(proc: ProcessPoolExecutor, fn: Callable, *fnargs, **fnkwargs):
			loop = asyncio.get_running_loop() or asyncio.get_event_loop()
			return await loop.run_in_executor(proc, partial(fn, *fnargs, **fnkwargs))

		def done(ftr: asyncio.Future):
			nonlocal result
			result_dict = ftr.result()
			result_ = self.model_class.validate(result_dict)
			if result is None:
				result = result_
			else:
				result.merge(result_, inplace=True)

			self.set_progress(value=result.count/self.data_count, message=progress_msg, show_percentage=True)

		# Pre-analyze initialization
		if force:
			self.reset()

		result: DataTable = None
		df = self.pre_analyze(
			self.select_data(),
			start_date=start_date,
			end_date=end_date,
			**kwargs
		)
		progress_msg = f'\nMenganalisa {self.data_count} {self.topic}...'
		self.set_progress(value=0.0, message=progress_msg, show_percentage=True)

		tasks: set = set()
		chunksize = self._get_chunksize(cpu=nprocessor, limit=limit_per_cpu)
		executor = ProcessPoolExecutor(nprocessor)
		for i in range(0, len(self.key_items), chunksize):
			key_segment = self.key_items[i:(i+chunksize)]
			task = asyncio.create_task(
				run_background(executor, self._run_on_each_process, df, key_segment, **kwargs)
			)
			task.add_done_callback(done)
			tasks.add(task)

		await asyncio.gather(*tasks)
		tasks.clear()
		executor.shutdown()

		self.set_progress(value=1.0, message=f'Analisa {self.data_count} {self.topic} selesai.')
		df_result = self._convert_to_dataframe(result)
		return self.post_analyze(df_result, **kwargs)

	@property
	def analyzed(self) -> bool:
		return self._analyzed



class AvailabilityNamespace:
	config: Config
	core: AvailabilityCore
	# filewriter: BaseFileWriter
	# statistics: Any
	state: CalculationState
	reports: Any

	def __init__(self):
		self.state = CalculationState()
		self.soe: SOEData = None

	def set_data(self, data: Union[SOEData, pd.DataFrame]):
		pass

	def _init_filereader(self, *, files: FileInput, master: SCDMasterType) -> FileReader:
		if master=='survalent':
			return FileReader(SurvalentSOEModel, SurvalentSPModel, files=files)
		elif master=='spectrum':
			return FileReader(SOEModel, files=files)

	def read_spectrum(self, files: FileInput, **kwargs) -> pd.DataFrame:
		reader = self._init_filereader(files=files, master='spectrum')
		return reader.load()

	def read_survalent(self, files: FileInput, **kwargs) -> pd.DataFrame:
		reader = self._init_filereader(files=files, master='survalent')
		return reader.load()

	def read(self, engine: Union[FileReader], *args, **kwargs) -> pd.DataFrame:
		pass

	def read_file(self, files: FileInput, **kwargs) -> pd.DataFrame:
		pass

	def read_database(self, **kwargs) -> pd.DataFrame:
		pass

	@toggle_attr('state.analyzing', True, False)
	def _do_analyze(self, *args, **kwargs):
		return self.core.fast_analyze(*args, **kwargs)
	
	@toggle_attr('state.analyzing', True, False)
	async def _do_async_analyze(self, *args, **kwargs):
		return await self.core.async_analyze(*args, **kwargs)

	@toggle_attr('state.calculating', True, False)
	def calculate(
		self,
	):
		self.state.analyzed = False
		result = self._do_analyze()
		self.state.analyzed = self.core.analyzed

	@toggle_attr('state.calculating', True, False)
	async def async_calculate(
		self,
	):
		self.state.analyzed = False
		result = await self._do_async_analyze()
		self.state.analyzed = self.core.analyzed