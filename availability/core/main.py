import asyncio, datetime, os, time
import multiprocessing as mp
import threading
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from functools import partial
from queue import Queue

import pandas as pd

from .base import BaseWithProgress, CalculationState, Config, DataModel, DataTable, SerializableException, frozen_dataclass_set
from .filereader import FileReader
from .filewriter import FileProperties, FileWriter, SheetWrapper
from .soe import SOEData, SOEModel, SurvalentSOEModel, SurvalentSPModel
from . import params
from ..lib import logprint, progress_bar, toggle_attr
from ..types import *
from .. import settings


@dataclass(frozen=True)
class QueueMessage:
	message: str = ''
	count: int = 1


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

	def __init__(self, config: Config, data: Optional[SOEData] = None, **kwargs):
		super().__init__(**kwargs)
		self.config = config
		self.reset()
		self.set_data(data)

	def reset(self):
		self._duration: float = 0.0
		self.analyzed: Union[bool, None] = None
		self.analyzing: bool = False
		self.key_items: List[AvKeys] = list()
		self.data_count: int = 0
		self.data: SOEData = None

	def set_data(self, data: SOEData):
		"""Set data source to analyze."""
		if isinstance(data, SOEData):
			self.data = data
		else:
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
		if not (isinstance(start_date, datetime.datetime) and isinstance(end_date, datetime.datetime)):
			raise ValueError(f'Invalid date parameter type of start_date ({type(start_date)}) or end_date ({type(end_date)})')
		# self.set_date_range(start_date, end_date)

		df_pre = df[
			(df['timestamp']>=start_date) &
			(df['timestamp']<=end_date)
		]
		self.key_items = self.get_key_items(df_pre)
		self.data_count = self.get_data_count(df_pre)
		return df_pre

	def post_analyze(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
		""""""
		self.analyzed = True
		return df

	def analyze_for_key(self, df: pd.DataFrame, key: AvKeys, **kwargs) -> List[Dict[str, Any]]:
		""""""
		pass

	def _get_chunksize(self, cpu: int, limit: Union[int, None]) -> int:
		if limit is None or limit<1:
			# Dynamic limit
			count = len(self.key_items)
			size = count // cpu
			if count%cpu>0 or size==0:
				size += 1
		else:
			# Defined
			size = limit
		
		return size

	def _analyze_keys(self, df: pd.DataFrame, keys: List[AvKeys], queue: Optional[Queue] = None, **kwargs) -> List[Dict[str, Any]]:
		"""Analyze function executed on each process concurrently.

		Args:
			keys : list of key to be used on loop

		Result:
			Serialized type of dataclass
		"""
		result: List[Dict[str, Any]] = list()
		for key in keys:
			try:
				data = self.analyze_for_key(df, key, queue=queue, **kwargs)
			except Exception as err:
				data = [SerializableException(err, extra={'key': key})]
			finally:
				result.extend(data)

			# if queue is not None:
			# 	queue.put(key)

		# IMPORTANT NOTE: Data passed thorugh inter-processes must be serializable
		return result

	def _run_concurrently(
		self,
		df: pd.DataFrame,
		keys: List[AvKeys],
		nprocessor: int,
		limit_per_cpu: Union[int, None],
		queue: Optional[Queue] = None,
		**kwargs
	) -> List[Dict[str, Any]]:
		"""Run analyze with multiple Processes.

		Args:
			keys : list of key to be used on loop

		Result:
			DataTable
		"""
		result: List[Dict[str, Any]] = list()
		progress_msg = kwargs.get('progress_message', '')
		callback = kwargs.get('callback')

		if callable(callback):
			callback(value=0.0, message=progress_msg, show_percentage=True)
		# ProcessPoolExecutor create new instance on different processes, so modifying instance in each process will not change instance in main process.
		# Value returned must be "serializable".
		chunksize = self._get_chunksize(cpu=nprocessor, limit=limit_per_cpu)
		with ProcessPoolExecutor(nprocessor) as ppe:
			futures = list()

			for i in range(0, len(keys), chunksize):
				key_segment = keys[i:(i+chunksize)]
				future = ppe.submit(self._analyze_keys, df, key_segment, queue)
				futures.append(future)

			for x, future in enumerate(as_completed(futures)):
				result_dict = future.result()
				result.extend(result_dict)

				# Call callback function
				progress_bar(value=(x+1)/len(futures), name='analyze')
				if callable(callback):
					callback(value=result.count/self.data_count, message=progress_msg, show_percentage=True)

		if callable(callback):
			callback(value=1.0, message=f'Analisa {self.data_count} {self.topic} selesai.')

		return result

	def _run_synchronously(
		self,
		df: pd.DataFrame,
		keys: List[AvKeys],
		**kwargs
	) -> List[Dict[str, Any]]:
		"""Run analyze on Single Process.

		Args:
			keys : list of key to be used on loop

		Result:
			Serialized type of dataclass
		"""
		result: List[Dict[str, Any]] = list()
		progress_msg = kwargs.get('progress_message', '')
		callback = kwargs.get('callback')

		if callable(callback):
			callback(value=0.0, message=progress_msg, show_percentage=True)

		for x, key in enumerate(keys):
			try:
				data = self.analyze_for_key(df, key, **kwargs)
			except Exception as err:
				data = [SerializableException(err, extra={'key': key})]
			finally:
				result.extend(data)

			# self.set_progress(value=result.count/self.data_count, message=progress_msg, show_percentage=True)
			# Call callback function
			progress_bar(value=(x+1)/len(keys), name='analyze')
			if callable(callback):
				callback(value=result.count/self.data_count, message=progress_msg, show_percentage=True)

		if callable(callback):
			callback(value=1.0, message=f'Analisa {self.data_count} {self.topic} selesai.')

		return result

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

		# Execute given function
		result = self._run_synchronously(
			df=df,
			keys=self.key_items,
			progress_message=progress_msg,
			**kwargs
		)
		df_result = pd.DataFrame(result).reset_index(drop=True)
		return self.post_analyze(df_result, **kwargs)

	def fast_analyze(
		self,
		start_date: Optional[datetime.datetime] = None,
		end_date: Optional[datetime.datetime] = None,
		limit_per_cpu: Union[int, None] = None,
		**kwargs
	) -> pd.DataFrame:
		return asyncio.run(self.async_analyze(start_date=start_date, end_date=end_date, limit_per_cpu=limit_per_cpu, **kwargs))

	@toggle_attr('analyzing', True, False)
	async def async_analyze(
		self,
		start_date: Optional[datetime.datetime] = None,
		end_date: Optional[datetime.datetime] = None,
		limit_per_cpu: Union[int, None] = None,
		**kwargs
	) -> pd.DataFrame:
		"""Asynchronous function using multiple Process to work concurrently.

		Result:
			Dataframe
		"""
		log_callback = kwargs.pop('log_callback', None)
		time_start = time.perf_counter()
		df = self.pre_analyze(
			self.select_data(),
			start_date=start_date,
			end_date=end_date,
			**kwargs
		)
		loop = asyncio.get_event_loop()
		# Get the number of workers
		n = min(settings.MAX_CPU_USAGE, os.cpu_count())
		chunksize = self._get_chunksize(cpu=n, limit=limit_per_cpu)
		# self.analyzing = True
		logprint(f'Analyzing {self.data_count} event(s)...', level='info')
		logprint(f'Mulai menganalisa {self.data_count} {self.topic}...', level='info', cli=False, ext_func=log_callback, **params.INFOLOG_KWARGS)

		with mp.Manager() as manager:
			queue = manager.Queue()
			t1 = threading.Thread(target=self._consume_queue, args=(queue, log_callback))
			t1.start()

			with ProcessPoolExecutor(n) as exc:
				tasks = list()
				for i in range(0, len(self.key_items), chunksize):
					key_segment = self.key_items[i:(i+chunksize)]
					tasks.append(loop.run_in_executor(exc, partial(self._analyze_keys, **kwargs), df, key_segment, queue, **kwargs))

				# Return List[List[serialized data]]
				results = await asyncio.gather(*tasks, return_exceptions=True)

		datalist = [data for group in results for data in group]
		df_result = pd.DataFrame(datalist).reset_index(drop=True)

		delta_time = time.perf_counter() - time_start
		self._duration = delta_time
		# self.analyzing = False
		logprint(f'Analyze completed in {delta_time:.3f}s.', level='info')
		logprint(f'Analisa {self.data_count} {self.topic} selesai. ({delta_time:.2f}s)', level='info', cli=False, ext_func=log_callback, **params.INFOLOG_KWARGS)
		return self.post_analyze(df_result, **kwargs)

	def _consume_queue(self, queue: Queue, callback: Optional[Callable]):
		processed = 0
		message_template = f'Menganalisa {self.data_count} {self.topic}...'
		logprint(f'Start consuming queue in {self.__class__.__name__}...')
		self.set_progress(
			value=0.0,
			message=message_template,
			show_percentage=True
		)
		while self.analyzing:
			try:
				if not queue.empty():
					qmsg: QueueMessage = queue.get()
					processed += qmsg.count
					progress = processed/self.data_count
					self.set_progress(
						value=progress,
						message=message_template,
						show_percentage=True
					)
			except Exception:
				break

			# Important to define delay / sleep for better performance
			time.sleep(0.001)

		self.set_progress(
			value=1.0,
			message=f'Analisa {self.data_count} {self.topic} selesai. ({self.duration:.3f}s)'
		)
		logprint('Consuming queue completed. exit=0')

	@property
	def duration(self) -> float:
		return self._duration


class Availability:

	def __init__(self, config: Config, **kwargs):
		self._duration: float = 0.0
		self.config = config
		self.core = self.init_core()
		self.reader: FileReader = FileReader(self.core.model_class)
		self.state: CalculationState = CalculationState()
		self.sources: str = None
		self.data: pd.DataFrame = None
		self.result: AvailabilityResult = None
		self._bind_state()

	def init_core(self) -> AvailabilityCore:
		return AvailabilityCore(self.config)

	def reset(self):
		self._duration = 0.0
		self.sources = None
		self.data = None
		self.result = None
		self.core.reset()
		self.reader.reset()
		self.state.reset()

	def _bind_state(self):
		self.core.bind_to(self.state)
		self.reader.bind_to(self.state)

	def _get_filename_prefix(self) -> str:
		return '_'.join(('Availability', self.__class__.__name__, self.config.master.title()))

	async def _read_files(self, files: FileInput, sheet: str, **kwargs):
		self.reader.set_file(files=files)
		return await self.reader.async_load(sheet_name=sheet, **kwargs)

	def read_file(self, files: FileInput, sheet: Optional[str] = None, **kwargs):
		return asyncio.run(self.async_read_file(files=files, sheet=sheet, **kwargs))

	async def async_read_file(self, files: FileInput, sheet: Optional[str] = None, **kwargs):
		self.reader.change_data_models(self.core.model_class)
		df = await self._read_files(files=files, sheet=sheet, **kwargs)
		if isinstance(df, pd.DataFrame):
			return self.post_read_file(df, **kwargs)
		else:
			return

	def read_soe_file(self, files: FileInput, sheet: Optional[str] = None, **kwargs):
		return asyncio.run(self.async_read_soe_file(files=files, sheet=sheet, **kwargs))

	async def async_read_soe_file(self, files: FileInput, sheet: Optional[str] = None, **kwargs):
		if self.config.master=='spectrum':
			self.reader.change_data_models(SOEModel)
		elif self.config.master=='survalent':
			self.reader.change_data_models(SurvalentSOEModel, SurvalentSPModel)
		else:
			raise ValueError(f'Invalid master selection "{self.config.master}"')

		return await self._read_files(files=files, sheet=sheet, **kwargs)

	def post_read_file(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
		self.sources = self.reader.sources
		return df

	def read_database(self, **kwargs):
		pass

	def analyze_soe(self, soe: SOEData, **kwargs):
		return asyncio.run(self.async_analyze_soe(soe=soe, **kwargs))

	def get_result(self, df: pd.DataFrame, start_date: Optional[DateSpec] = None, end_date: Optional[DateSpec] = None) -> AvailabilityData:
		pass

	async def async_analyze_soe(self, soe: SOEData, **kwargs):
		if not isinstance(soe, SOEData):
			logprint(f'Invalid {type(soe)} type data passed to be analyzed in {self.__class__.__name__}', level='error')
			return

		self.core.set_data(soe)
		df_av = await self.core.async_analyze(start_date=soe.date_min, end_date=soe.date_max, **kwargs)
		self.data = df_av
		self._duration = self.core.duration
		self.sources = soe.sources
		return df_av

	@toggle_attr('state.calculating', True, False)
	def calculate(self, start_date: Optional[datetime.datetime] = None, end_date: Optional[datetime.datetime] = None, **kwargs) -> Optional[AvailabilityResult]:
		log_callback = kwargs.pop('log_callback', None)
		self.state.progress.update(value=0.0, message=f'Menghitung {self.core.subject}...')
		time_start = time.perf_counter()

		if isinstance(self.data, pd.DataFrame):
			periods = '' if start_date is None else f' dalam periode {start_date.strftime("%d-%m-%Y")} s/d {end_date.strftime("%d-%m-%Y")}'
			message = f'Menghitung {self.core.subject} dari {self.data.shape[0]} {self.core.topic}{periods}...'
			logprint(message, level='info', cli=False, ext_func=log_callback, **params.INFOLOG_KWARGS)
			result = self.get_result(self.data, start_date=start_date, end_date=end_date)
		else:
			result = None
			logprint(f'Calculation can\'t be done due to invalid source data. {self.__class__.__name__}.data=None', level='warning')
			logprint(f'Perhitungan {self.core.subject} tidak dapat dilakukan karena data invalid.', level='warning', cli=False, ext_func=log_callback, **params.WARNINGLOG_KWARGS)

		delta_time = time.perf_counter() - time_start
		if self.core.analyzed:
			# Accumulate process duration
			self._duration += delta_time
			# Reset analyzed status
			self.core.analyzed = False
		else:
			self._duration = delta_time

		calculated = isinstance(result, AvailabilityResult)
		self.result = result
		self.state.calculated = calculated
		if calculated:
			message = f'Perhitungan {self.core.subject} selesai. ({self.duration:.2f}s)'
			logprint(f'Calculation completed in {delta_time:.3f}s.', level='info')
			logprint(message, level='info', cli=False, ext_func=log_callback, **params.SUCCESSLOG_KWARGS)
		else:
			message = f'Perhitungan {self.core.subject} error. ({self.duration:.2f}s)'
			logprint(f'Calculation completed with error. ({delta_time:.3f}s)', level='info')
			logprint(message, level='error', cli=False, ext_func=log_callback, **params.ERRORLOG_KWARGS)

		self.state.progress.update(value=1.0, message=message)
		return result

	def get_properties(
		self,
		title: Optional[str] = None,
		subject: Optional[str] = None,
		author: Optional[str] = None,
		manager: Optional[str] = None,
		company: Optional[str] = None,
		category: Optional[str] = None,
		comments: Optional[str] = None,
	) -> FileProperties:
		# Define file properties
		return FileProperties(
			title=title or 'Availability',
			subject=subject or 'Availability',
			author=author or 'SCADA',
			manager=manager or 'Fasop',
			company=company or 'PLN UP2B Sistem Makassar',
			category=category or 'Excel Automation',
			comments=comments or f'Dibuat otomatis menggunakan Python{settings.PY_VERSION} dan XlsxWriter'
		)

	def get_xlsheet(self, **infokwargs) -> Dict[str, SheetWrapper]:
		return dict()

	@toggle_attr('state.exporting', True, False)
	def write_file(self, filename: Optional[str] = None, as_iobuffer: bool = False, **kwargs):
		log_callback = kwargs.pop('log_callback', None)
		self.state.progress.update(value=0.0, message=f'Memulai proses generate file...')
		time_start = time.perf_counter()

		prefix = self._get_filename_prefix()
		# Create filename automatically if not defined
		if not filename:
			start_date = self.result.date_min.strftime("%Y%m%d")
			stop_date = self.result.date_max.strftime("%Y%m%d")
			date_specs = '{start_date}-{stop_date}'.format(start_date=start_date, stop_date=stop_date)
			filename = '_'.join((prefix, 'Output', date_specs))

		writer = FileWriter(
			filename_prefix=prefix,
			sheets=self.get_xlsheet(filename=filename),
			properties=self.get_properties(),
		)
		self.writer = writer
		result = writer.to_excel(filename=filename, as_iobuffer=as_iobuffer)
		self.state.exported = isinstance(result, (str, BytesIO))
		if isinstance(result, str):
			self.state.last_exported_file = result

		delta_time = time.perf_counter() - time_start
		self.state.progress.update(value=1.0, message=f'Proses generate file selesai. ({delta_time:.2f}s)')
		logprint(f'File availability {self.__class__.__name__} telah berhasil di-generate.', level='info', cli=False, ext_func=log_callback, **params.INFOLOG_KWARGS)
		return result

	@property
	def duration(self) -> float:
		return self._duration

