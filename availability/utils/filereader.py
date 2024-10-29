import os, re, time
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from glob import glob
from io import BytesIO
from types import MappingProxyType
from typing import Any, Dict, List, Callable, Literal, Optional, Tuple, TypeAlias, Union

import pandas as pd

from .worker import run_cpu_bound
from ..globals import RCD_COLUMNS, RTU_COLUMNS, SOE_COLUMNS, SOE_COLUMNS_DTYPE
from ..lib import CONSOLE_WIDTH, ProcessError, calc_time, immutable_dict, join_datetime, load_cpoint, load_workbook, read_xml, test_datetime_format, truncate


FilePaths: TypeAlias = List[str]
FileDict: TypeAlias = Dict[str, BytesIO]
DtypeMapping: TypeAlias = MappingProxyType[str, Dict[str, Any]]


class _FileReader:
	"""Base class for reading files (xls, xlsx, xml) into dataframe.

	Args:
		files : path of file(s)

	Accepted kwargs:
		switching_element : elements to be monitored
		date_start : oldest date limit of data
		date_stop : newest date limit of data
	"""
	__slot__: List[str] = ['switching_element']
	_errors: List[Any]
	_warnings: List[Any]
	filenames: FilePaths
	iobuffers: FileDict
	exception_prefix: str = 'LoadFileError'
	column_list: List[str] = list()
	keep_duplicate: str = 'last'
	sheet_name: str = 'Sheet1'
	time_series_column: str = 'Timestamp'

	def __init__(self, files: Union[str, FilePaths, FileDict, None] = None, **kwargs):
		self._date_range = (None, None)
		self._loaded = False
		self._errors = list()
		self._warnings = list()
		self._files = files
		self.cached_file = dict()
		self.filenames = list()
		self.iobuffers = dict()
		self.switching_element = kwargs['switching_element'] if 'switching_element' in kwargs else ['CB']
		# Need this for cooperative multiple-inheritance
		super().__init__(**kwargs)

	def initialize(self) -> None:
		self._date_range = (None, None)
		self._loaded = False
		self._errors = list()
		self._warnings = list()
		self.cached_file = dict()
		self.filenames = list()
		self.iobuffers = dict()
		try:
			# Chaining initialize method as Mixin Class
			super().initialize()
		except AttributeError:
			pass

	def file_setup(self) -> FilePaths:
		"""Get list of exact filepaths from given input."""
		filenames = list()
		if isinstance(self._files, str):
			for f in self._files.split(','):
				if '*' in f:
					g = glob(f.strip())
					if len(g)>0:
						filenames += g
					else:
						self._errors.append(ProcessError(self.exception_prefix, f'File yang menyerupai "{f}" tidak ditemukan.'))
						print(f'Warning: File yang menyerupai "{f}" tidak ditemukan.')
				elif f.strip():
					filenames.append(f.strip())
		elif isinstance(self._files, list):
			filenames = self._files
		elif isinstance(self._files, dict):
			filenames = list(self._files.keys())
			self.iobuffers = self._files
		return filenames

	def open_multiple_files(self, files: FilePaths, *args, **kwargs):
		"""Open multiple files with Threads.

		Args:
			files : list of files
		"""
		fpath_list: FilePaths = list()
		data_list: List[pd.DataFrame] = list()

		for i, file in enumerate(files):
			data = self.open_file(file, sheet_name=self.sheet_name, table_header=self.column_list, base_column=self.time_series_column)
			fpath_list.append(files[i])
			data_list.append(data)

		return fpath_list, data_list

	def open_files_multiprocess(self, files: FilePaths, *args, **kwargs):
		"""Optimize file loading with ProcessPoolExecutor.

		Args:
			files : list of files
		"""
		fpath_list: FilePaths = list()
		data_list: List[pd.DataFrame] = list()
		n = os.cpu_count()
		chunksize = len(files)//n + 1

		with ProcessPoolExecutor(n) as ppe:
			futures = list()

			for i in range(0, len(files), chunksize):
				_files = files[i:(i+chunksize)]
				future = ppe.submit(self.open_multiple_files, _files)
				futures.append(future)

			for x, future in enumerate(as_completed(futures)):
				fpaths, datas = future.result()
				fpath_list.extend(fpaths)
				data_list.extend(datas)

		return fpath_list, data_list

	def _load(self, **kwargs):
		errors = list()
		count = len(self.filenames)
		df_result = None

		if count==0:
			err = ProcessError(self.exception_prefix, 'Input file belum ditentukan!')
			errors.append(err)
		if count==1:
			result = self.open_file(
				file=self.filenames[0],
				sheet_name=self.sheet_name,
				table_header=self.column_list,
				base_column=self.time_series_column
			)
			if isinstance(result, pd.DataFrame):
				result = self.post_open_file(result)
				df_result = result.drop_duplicates(keep=self.keep_duplicate)
			else:
				errors.append(result)
		else:
			valid_df = list()
			fpath_list, data_list = self.open_files_multiprocess(files=self.filenames, callback=kwargs.get('callback'))

			for result in data_list:
				if isinstance(result, pd.DataFrame):
					df = self.post_open_file(result)
					valid_df.append(df)
				else:
					errors.append(result)

			if len(valid_df)==0:
				err = ProcessError(self.exception_prefix, f'Semua data tidak valid. (Error={len(errors)})')
				errors.append(err)
			else:
				# Combine each Dataframe in data list into one and eleminate duplicated rows
				df_result = pd.concat(valid_df).drop_duplicates(keep=self.keep_duplicate)

		return df_result, errors

	def load(self, **kwargs) -> Union[pd.DataFrame, None]:
		"""Load each file in filepaths into dataframe."""
		self.initialize(**kwargs)
		self.filenames = self.file_setup()
		print(f'\nTotal {len(self.filenames)} file...')
		t0 = time.time()
		df, errors = self._load(**kwargs)
		delta_time = time.time() - t0
		self.errors.extend(errors)
		self._duration = delta_time
		print(f'(durasi={delta_time:.2f}s, error={len(self.errors)})')

		if errors:
			# If any error occured, do something here
			pass

		if isinstance(df, pd.DataFrame):
			return self.post_load(df)
		else:
			return df
		
	async def async_load(self, **kwargs) -> Union[pd.DataFrame, None]:
		"""Load each file in filepaths asynchronously & concurrently into dataframe."""
		self.initialize(**kwargs)
		self.filenames = self.file_setup()
		print(f'\nTotal {len(self.filenames)} file...')
		time_start = time.time()
		# Execute CPU Bound process in different processes
		df, errors = await run_cpu_bound(self._load, **kwargs)
		delta_time = time.time() - time_start
		self.errors.extend(errors)
		self._duration = delta_time
		print(f'(durasi={delta_time:.2f}s, error={len(self.errors)})')

		if errors:
			# If any error occured, do something here
			pass

		if isinstance(df, pd.DataFrame):
			return self.post_load(df)
		else:
			return df

	def post_load(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Executed method right after all files loaded.

		Args:
			df : Dataframe
		"""
		self.set_date_range(
			date_start=df[self.time_series_column].min(),
			date_stop=df[self.time_series_column].max()
		)
		self.sources = ',\n'.join(self.filenames)
		self._loaded = True
		return df

	def open_file(
			self,
			file: str,
			sheet_name: Optional[str] = None,
			table_header: Union[List, Tuple] = None,
			base_column: str = None,
			*args,
			**kwargs
		) -> Union[pd.DataFrame, Exception]:
		"""Open single file into dataframe.

		Args:
			file : file source to be opened
			sheet_name : defined sheet name
			table_header : defined header for validation
			base_column : base column for sorting & filtering
		"""
		wb = dict()
		df = None
		first_sheet = False
		txt_stat_len = 5
		txt_info_len = CONSOLE_WIDTH - txt_stat_len
		txt_prefix = f'Membuka file'

		if sheet_name is None and table_header is None: first_sheet = True		# Sheet name & header not defined, load first sheet

		if len(txt_prefix)+len(file)+4>txt_info_len:
			txt_path = f'"{truncate(text=file, max_length=txt_info_len-len(txt_prefix)-4, on="left")}"'
		else:
			txt_path = f'"{file}"'.ljust(txt_info_len-len(txt_prefix)-2)

		try:
			wb = load_workbook(self.iobuffers.get(file, file))

			if first_sheet:
				# Get first sheet name in workbook
				sheet_name = tuple(wb.keys())[0]

			if sheet_name in wb:
				ws = wb[sheet_name]
				if table_header is None:
					df = ws
					txtstatus = 'OK!'
				else:
					if set(table_header).issubset(ws.columns):
						df = ws
						txtstatus = 'OK!'
					else:
						result = ProcessError(self.exception_prefix, 'Header tabel tidak sesuai', f'file={file}')
						txtstatus = f'NOK!\r\nHeader tabel tidak sesuai.'
			else:
				for ws_name, sheet in wb.items():
					# Loop through workbook sheets & match header
					if set(table_header).issubset(sheet.columns):
						df = sheet
						txtstatus = 'OK!'
						break

				if df is None:
					result = ProcessError(self.exception_prefix, 'Data tidak ditemukan.', f'file={file}')
					txtstatus = f'NOK!\r\nData tidak ditemukan!'

			if isinstance(df, pd.DataFrame):
				if base_column is not None:
					# Neglect null value on base column
					df = df[df[base_column].notnull()]
				result = df

		except (ValueError, ImportError):
			try:
				# Attempting to open file with xml format
				result = read_xml(file, **kwargs)
			except (ValueError, ImportError):
				result = ProcessError(self.exception_prefix, 'Gagal membuka file.', f'file={file}')
				txtstatus = 'NOK!\r\nGagal membuka file.'
		except FileNotFoundError:
			result = ProcessError(self.exception_prefix, 'File tidak ditemukan.', f'file={file}')
			txtstatus = 'NOK!\r\nFile tidak ditemukan.'

		self.cached_file[file] = df
		print(f'{txt_prefix} {txt_path} {txtstatus}')
		return result

	def post_open_file(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Process right after file opened.
		
		Args:
			df : dataframe
		"""
		return df

	def set_date_range(self, date_start, date_stop) -> None:
		"""Set date range from given parameters.

		Args:
			date_start : oldest date limit
			date_stop : newest date limit
		"""
		dtstart = date_start.to_pydatetime() if isinstance(date_start, pd.Timestamp) else date_start
		dtstop = date_stop.to_pydatetime() if isinstance(date_stop, pd.Timestamp) else date_stop
		self._date_range = (dtstart, dtstop)

	def set_file(self, files: Union[str, FilePaths, FileDict]) -> None:
		self._files = files
		self.initialize()

	@property
	def date_range(self):
		return self._date_range

	@property
	def date_start(self):
		return self.date_range[0]

	@property
	def date_stop(self):
		return self.date_range[1]
	
	@property
	def duration(self) -> float:
		return getattr(self, '_duration', None)

	@property
	def errors(self):
		return self._errors

	@property
	def loaded(self):
		return self._loaded

	@property
	def warnings(self):
		return self._warnings


class SpectrumFileReader(_FileReader):
	"""This class used generally for loading Spectrum's Sequence of Events (SOE) files.

	Args:
		files : path of file(s)
	"""
	column_dtype: DtypeMapping = immutable_dict(SOE_COLUMNS_DTYPE)
	column_list: List[str] = SOE_COLUMNS
	sheet_name: str = 'HIS_MESSAGES'
	time_series_column: str = 'Time stamp'
	cpoint_file: str = 'availability/src/cpoint*.xlsx'
	soe_control_disable: pd.DataFrame
	soe_local_remote: pd.DataFrame
	soe_rtu_updown: pd.DataFrame
	soe_switching: pd.DataFrame
	soe_synchro: pd.DataFrame
	soe_trip: pd.DataFrame
	soe_all: pd.DataFrame

	def __init__(self, files: Union[str, FilePaths, FileDict, None] = None, **kwargs):
		# Need this for cooperative multiple-inheritance
		super().__init__(files, **kwargs)

	def load(self, **kwargs) -> Tuple[pd.DataFrame, List[Any], float]:
		"""Load each file in filepaths into dataframe."""
		# Load point description
		self.point_description = load_cpoint(self.cpoint_file)
		return super().load(**kwargs)

	async def async_load(self, **kwargs) -> Tuple[pd.DataFrame, List[Any], float]:
		"""Load each file in filepaths asynchronously & concurrently into dataframe."""
		# Load point description
		self.point_description = load_cpoint(self.cpoint_file)
		return await super().async_load(**kwargs)

	def post_load(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Executed method right after all files loaded.

		Args:
			df : Dataframe
		"""
		if pd.api.types.is_object_dtype(df['Time stamp']) or pd.api.types.is_object_dtype(df['System time stamp']):
			df['Time stamp'] = df['Time stamp'].map(lambda x: test_datetime_format(x))
			df['System time stamp'] = df['System time stamp'].map(lambda x: test_datetime_format(x))
		# Format B2 as string value
		df['B2'] = df['B2'].map(lambda x: re.sub(r'\.\d+', '', str(x)))
		df_post = super().post_load(df)
		soe_all = self.prepare_data(df_post)
		# self.soe_all = soe_all
		return soe_all

	def prepare_data(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
		"""Filtering, convertion & validation process of dataframe input then split into specific purposes.

		Args:
			df : Dataframe
		"""
		col1 = ['B1', 'B2', 'B3']
		col2 = ['B1 text', 'B2 text', 'B3 text']

		for col in df.columns:
			# Remove unnecessary spaces on begining and or trailing string object
			if pd.api.types.is_object_dtype(df[col]): df[col] = df[col].str.strip()

		new_df = df.copy().fillna('')
		# Filter new DataFrame
		new_df = new_df.loc[(new_df['A']=='') & (new_df[self.time_series_column]>=self.date_range[0]) & (new_df[self.time_series_column]<=self.date_range[1]), self.column_list]
		new_dftype = {key: value for key, value in self.column_dtype.items() if key in new_df.columns}

		new_df = new_df.astype(new_dftype)
		new_df = new_df.sort_values(['System time stamp', 'System milliseconds', 'Time stamp', 'Milliseconds'], ascending=[True, True, True, True]).reset_index(drop=True)
		new_df['Status'] = new_df['Status'].str.title()

		# Change B1, B2, B3 from description style into mnemonic style
		if hasattr(self, 'point_description'):
			df_trans = self.point_description.copy()
			b1_avg_len, b3_avg_len = new_df['B1'].str.len().mean(), new_df['B3'].str.len().mean()
			is_longtext = b1_avg_len>9 or b3_avg_len>9
			if is_longtext:
				# Swap column labels, because exported His. Messages using description text
				# ['B1', 'B2', 'B3', 'B1 text', 'B2 text', 'B3 text'] -> ['B1 text', 'B2 text', 'B3 text', 'B1', 'B2', 'B3']
				df_trans.columns = col2 + col1
				print(f'Debug: b1_average_length={round(b1_avg_len, 1)}, b3_average_length={round(b3_avg_len, 1)}\t>> Swap kolom')
			# Double check duplicated keys
			df_trans.drop_duplicates(subset=col1, keep='first', inplace=True)
			# Merge B1, B2, B3 translation with existing table
			new_df = new_df.merge(df_trans, on=col1, how='left')
			without_description = new_df['B1 text'].isna()

			if new_df[without_description].shape[0]>0:
				# List unknown (nan) Point Description
				no_description = new_df.loc[without_description, col1].drop_duplicates(keep='first').values
				self.warnings.extend([f'{"/".join(map(lambda s: str(s), point))} tidak terdaftar dalam Point Description.' for point in no_description])
				print(f'\n{len(no_description)} poin tidak terdaftar dalam "Point Description".\n{"; ".join([str(x) for i, x in enumerate(no_description) if i<5])}{" ..." if len(no_description)>5 else ""}\nSilahkan update melalui SpectrumOfdbClient atau menambahkan manual pada file cpoint.xlsx!')
				# Fill unknown (nan) Point Description B1, B2, B3 with its own text
				new_df.loc[without_description, col2] = new_df.loc[without_description, col1].values

			if is_longtext:
				# Swap column labels, because exported His. Messages using description text
				new_df[col1 + col2] = new_df[col2 + col1]
			# Rearrange columns
			new_df = new_df[SOE_COLUMNS + col2]

		# Split into DataFrames for each purposes, not reset index
		self.soe_control_disable = new_df[(new_df['Element']=='CD') & (new_df['Status'].isin(['Disable', 'Enable', 'Dist.']))].copy()
		self.soe_local_remote = new_df[(new_df['Element']=='LR') & (new_df['Status'].isin(['Local', 'Remote', 'Dist.']))].copy()
		self.soe_rtu_updown = new_df[(new_df['B1']=='IFS') & (new_df['B2']=='RTU_P1') & (new_df['Status'].isin(['Up', 'Down']))].copy()
		self.soe_switching = new_df[(new_df['Element'].isin(self.switching_element)) & (new_df['Status'].isin(['Open', 'Close', 'Dist.']))].copy()
		self.soe_synchro = new_df[(new_df['Element']=='CSO') & (new_df['Status'].isin(['Off', 'On', 'Dist.']))].copy()
		self.soe_trip = new_df[new_df['Element'].isin(['CBTR', 'MTO'])].copy()
		return new_df


class RCFileReader(_FileReader):
	"""This class used for collecting / combining multiple RCD files.

	Args:
		files : path of file(s)
	"""
	column_list: List[str] = RCD_COLUMNS
	sheet_name: str = 'RC_ONLY'
	time_series_column: str = 'Order Time'
	rcd_all: pd.DataFrame

	def __init__(self, files: Union[str, FilePaths, FileDict, None] = None, **kwargs):
		# Need this for cooperative multiple-inheritance
		super().__init__(files, **kwargs)

	def post_load(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Executed method right after all files loaded.

		Args:
			df : Dataframe
		"""
		# Format B2 as string value
		df['B2'] = df['B2'].map(lambda x: re.sub(r'\.\d+', '', str(x)))
		df_post = super().post_load(df)
		rcd_all = self.prepare_data(df_post)
		# self.rcd_all = rcd_all
		return rcd_all

	def prepare_data(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
		"""Filtering & convertion process of dataframe input.

		Args:
			df : Dataframe
		"""
		# Filter new DataFrame
		new_df = df.loc[(df[self.time_series_column]>=self.date_range[0]) & (df[self.time_series_column]<=self.date_range[1]), self.column_list].copy()
		new_df = new_df.fillna('')\
			.sort_values([self.time_series_column], ascending=[True])\
			.reset_index(drop=True)
		return new_df


class AVRSFileReader(_FileReader):
	"""Class used for collecting / combining multiple AVRS files.

	Args:
		files : path of file(s)
	"""
	column_list: List[str] = RTU_COLUMNS
	column_mark: List[str] = [
		'Marked Maintenance',
		'Marked Link Failure',
		'Marked RTU Failure',
		'Marked Other Failure'
	]
	sheet_name: str = 'DOWNTIME'
	time_series_column: str = 'Down Time'
	rtudown_all: pd.DataFrame

	def __init__(self, files: Union[str, FilePaths, FileDict, None] = None, **kwargs):
		# Need this for cooperative multiple-inheritance
		super().__init__(files, **kwargs)

	def post_load(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Executed method right after all files loaded.

		Args:
			df : Dataframe
		"""
		df_post = super().post_load(df)
		rtudown_all = self.prepare_data(df_post)
		# self.rtudown_all = rtudown_all
		return rtudown_all

	def prepare_data(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
		"""Filtering & convertion process of dataframe input.

		Args:
			df : Dataframe
		"""
		df_columns = df.columns.to_list()
		new_df = df.loc[(df[self.time_series_column]>=self.date_range[0]) & (df[self.time_series_column]<=self.date_range[1]), self.column_list].copy()

		# Define columns for user validation
		new_df['Acknowledged Down Time'] = ['' if adt is pd.NaT else adt for adt in df['Acknowledged Down Time']] if 'Acknowledged Down Time' in df_columns else ''
		new_df['Fix Duration'] = df['Fix Duration'] if 'Fix Duration' in df_columns else new_df['Duration']
		# Define marked columns
		for mcol in self.column_mark:
			# Set value for additional columns, default is empty string ''
			if mcol=='Marked Link Failure':
				if mcol in df_columns:
					new_df[mcol] = df[mcol]
				elif 'Marked Comm. Failure' in df_columns:
					new_df[mcol] = df['Marked Comm. Failure']
				else:
					new_df[mcol] = ''
			else:
				new_df[mcol] = df[mcol] if mcol in df_columns else ''

		new_df = new_df.fillna('').sort_values([self.time_series_column], ascending=[True]).reset_index(drop=True)
		return new_df


class SurvalentFileReader(_FileReader):
	column_dtype: DtypeMapping = immutable_dict(SOE_COLUMNS_DTYPE)
	column_list: List[str] = ['Time', 'Point', 'Message', 'Operator']
	sheet_name: str = 'Report'
	time_series_column: str = 'Time'
	datetime_format: str = '%Y-%m-%d %H:%M:%S.%f'
	elements: List[str] = ['CBTR', 'CD', 'CSO', 'LR', 'MTO']
	statuses: Dict[str, str] = {'opened': 'Open', 'closed': 'Close', 'enabled': 'Enable', 'disabled': 'Disable', 'appear': 'Appeared'}
	b3_dict: Dict[str, str] = dict()
	cmd_order: List[int] = list()
	cmd_neg_feedback: List[int] = list()
	soe_all: pd.DataFrame

	def __init__(self, files: Union[str, FilePaths, FileDict, None] = None, **kwargs):
		super().__init__(files, **kwargs)

	def post_load(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Executed method right after all files loaded.

		Args:
			df : Dataframe
		"""
		df_extracted = self.extractor(df)
		self.soe_all = df_extracted
		# Change time series reference
		self.time_series_column = 'Time stamp'
		df_post = super().post_load(df_extracted)
		return df_post

	def find_cmd_status(self, df: pd.DataFrame, feedback_index: int) -> str:
		"""Find negative feedback "Status" parameter from previous command.

		Args:
			df : dataframe input
			feedback_index : dataframe row index of known feedback
		"""
		cmd_status = ''
		dt0, ms0, b3, elm = df.loc[feedback_index, ['Time stamp', 'Milliseconds', 'B3', 'Element']]
		df_cmd: pd.DataFrame = df[(df['B3']==b3) & (df['Element']==elm) & (df['Operator']!='')]

		if df_cmd[(df_cmd['Tag']=='OR') & (join_datetime(df_cmd['Time stamp'], df_cmd['Milliseconds'])<join_datetime(dt0, ms0))].shape[0]>0:
			# Check previous command order
			cmd_order = df_cmd[(df_cmd['Tag']=='OR') & (join_datetime(df_cmd['Time stamp'], df_cmd['Milliseconds'])<join_datetime(dt0, ms0))].iloc[-1]
			cmd_status = cmd_order['Status']

		return cmd_status

	def extractor(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Parameterize and annotate input dataframe.

		Args:
			df : dataframe input
		"""
		columns = SOE_COLUMNS
		# Copy and reorder Dataframe
		df0 = df.infer_objects().fillna('').sort_values('Time')
		df0[['Point B3', 'Element']] = df0['Point'].str.split(',', expand=True)

		# Filter only required element
		df0 = df0[(df0['Element'].isin(self.elements + self.switching_element)) & ~((df0['Message'].str.contains('Put in scan')) | (df0['Message'].str.contains('ALL ALARMS BLOCKED')) | (df0['Message'].str.contains('Blocked')) | (df0['Message'].str.contains('Unblocked')) | (df0['Message'].str.contains('Manual input')))].reset_index(drop=True)
		df0['A'] = ''
		df0[['Time stamp', 'Milliseconds']] = df0['Time'].str.split('.', expand=True)
		df0[['System time stamp', 'System milliseconds']] = df0[['Time stamp', 'Milliseconds']]
		df0['B1'] = ''
		df0['B2'] = '150'	# assumming all bay is 150kV
		df0['B3'] = ''
		df0['Status'] = ''
		df0['Tag'] = ''
		df0['Comment'] = ''
		df0['User comment'] = ''
		df0['RTU ID'] = ''

		dftype = {key: value for key, value in self.column_dtype.items() if key in df0.columns}
		df0 = df0.astype(dftype)
		# Modify columns for statuses
		for elm1 in self.elements + self.switching_element:
			self.extract_status(df0, elm1)
		# Update b3_dict
		# Used in defining command's B1 column
		b3_unique = df0.loc[df0['B1']!='', ['B1', 'B3']].drop_duplicates(keep='first').values
		for vb1, vb3 in b3_unique:
			if vb3!='': self.b3_dict[vb3] = vb1
		# Modify columns for commands
		for elm2 in self.switching_element:
			self.extract_command(df0, elm2)
		# Modify columns for negative feedback
		for ind in self.cmd_neg_feedback:
			cmd_sts = self.find_cmd_status(df0, ind)
			df0.loc[ind, 'Status'] = cmd_sts

		return df0[columns + ['RTU ID', 'Point', 'Message']]

	def extract_status(self, df: pd.DataFrame, element: str) -> None:
		"""Extract parameters of status events from "Message" column.
		
		Format:
			Normal status : '*<RTU ID> <B1> <B3> <Element> <Status> <Ext. Information>'

		Args:
			df : dataframe input
			element : SCADA point element
		"""
		index = df[(df['Element']==element) & (df['Operator']=='')].index

		for i in index:
			msg, pb3, elm = df.loc[i, ['Message', 'Point B3', 'Element']]
			msg = str(msg).replace(' 20 ', ' ')
			_splitted = msg.split(' '.join((pb3, elm)))

			if len(_splitted)==2:
				_lstr, _rstr = _splitted
				_lsplit = _lstr.strip().split(' ')
				_rsplit = _rstr.strip().split(' ')
				pointid = _lsplit.pop(0).replace('*', '')
				b1 = ' '.join(_lsplit)
				b3 = '' if elm=='CD' else pb3
				sts = self.statuses[_rsplit[0].lower()] if _rsplit[0].lower() in self.statuses else _rsplit[0].title()
			elif len(_splitted)==3:
				_lstr, _rstr = _splitted[0], _splitted[2]
				_rsplit = _rstr.strip().split(' ')
				pointid = _lstr.strip().split(' ')[0].replace('*', '')
				b1 = pb3
				b3 = ''
				sts = self.statuses[_rsplit[0].lower()] if _rsplit[0].lower() in self.statuses else _rsplit[0].title()
			else:
				err = ProcessError('', 'Gagal mengekstrak event status!', f'key="{msg}", string=[{pb3}, {elm}]')
				self._errors.append(err)
				raise err

			df.loc[i, ['B1', 'B3', 'Status', 'RTU ID']] = [b1, b3, sts, pointid]

	def extract_command(self, df: pd.DataFrame, element: str):
		"""Extract parameters of command events from "Message".

		Format:
			Normal command : '*<Status> <B3>,<Element> FROM <UI Dispatcher>::<User>'	
			Negative feedback : '*<B3>,<Element>***<RTU ID> <B1> CONTROL ECHO FAILURE'

		Args:
			df : dataframe input
			element : SCADA point element
		"""
		index = df[(df['Element']==element) & (df['Operator']!='')].index

		for i in index:
			msg, poi, b3, elm = df.loc[i, ['Message', 'Point', 'Point B3', 'Element']]
			msg = str(msg).replace(' 20 ', ' ')
			_splitted = msg.split(poi)

			if len(_splitted)==2:
				_lstr, _rstr = _splitted
				_lsplit = _lstr.strip().split(' ')
				if 'CONTROL ECHO FAILURE' in _rstr:
					tag = 'NE'
					sts = ''
					self.cmd_neg_feedback.append(i)
				else:
					tag = 'OR'
					sts = _lsplit[0].replace('*', '')
					self.cmd_order.append(i)
				b1 = self.b3_dict.get(b3, '')
			else:
				err = ProcessError('', 'Gagal mengekstrak event kontrol!', f'key="{msg}", string=[{b3}, {elm}]')
				self._errors.append(err)
				raise err

			df.loc[i, ['B1', 'B3', 'Status', 'Tag']] = [b1, b3, sts, tag]


def test_random_file(**params):
	print(' TEST OPEN RANDOM FILE '.center(CONSOLE_WIDTH, '#'))
	print('## Dummy load ##')
	f = SpectrumFileReader('sample/sample_rcd_2024_01.xlsx')
	file = input('\r\nMasukkan lokasi file :  ')
	if file:
		f1 = f.open_file(file)
	else:
		f1 = None
	return f1

def test_file_not_exist(**params):
	print(' TEST FILE NOT FOUND '.center(CONSOLE_WIDTH, '#'))
	f = SpectrumFileReader('sample/file_not_existed.xlsx')
	f.load()
	return f

def test_wrong_file(**params):
	print(' TEST WRONG FILE '.center(CONSOLE_WIDTH, '#'))
	f = SpectrumFileReader('sample/wrong_file_1.xlsx')
	f.load()
	return f

def test_file_spectrum(**params):
	print(' TEST FILE SOE SPECTRUM '.center(CONSOLE_WIDTH, '#'))
	f = SpectrumFileReader('sample/sample_rcd*.xlsx, /home/fasop/Documents/HW_SPEC_SCADA.xlsx')
	print('\r\n' + ' TEST FILE SOE SPECTRUM CONCURRENTLY '.center(CONSOLE_WIDTH, '#'))
	f.load()
	return f

def test_file_survalent(**params):
	print(' TEST FILE SOE SURVALENT '.center(CONSOLE_WIDTH, '#'))
	f = SurvalentFileReader('sample/survalent/sample_soe*.XLSX')
	print('\r\n' + ' TEST FILE SOE SURVALENT CONCURRENTLY '.center(CONSOLE_WIDTH, '#'))
	f.load()
	return f

def test_file_rcd_collective(**params):
	print(' TEST FILE RCD COLLECTIVE '.center(CONSOLE_WIDTH, '#'))
	f = RCFileReader('sample/sample_rcd*.xlsx')
	f.load()
	return f

def test_file_rtu_collective(**params):
	print(' TEST FILE RTU COLLECTIVE '.center(CONSOLE_WIDTH, '#'))
	f = AVRSFileReader('sample/sample_rtu*.xlsx')
	f.load()
	return f

if __name__=='__main__':
	test_list = [
		('Test file bebas', test_random_file),
		('Test file tidak ditemukan', test_file_not_exist),
		('Test file salah format', test_wrong_file),
		('Test file SOE Spectrum', test_file_spectrum),
		('Test file SOE Survalent', test_file_survalent),
		('Test file RCD gabungan', test_file_rcd_collective),
		('Test file RTU gabungan', test_file_rtu_collective)
	]
	ans = input('Confirm troubleshooting? [y/n]  ')
	if ans=='y':
		print('\r\n'.join([f'  {no+1}.'.ljust(6) + tst[0] for no, tst in enumerate(test_list)]))
		choice = int(input(f'\r\nPilih modul test [1-{len(test_list)}] :  ')) - 1
		if choice in range(len(test_list)):
			test = test_list[choice][1]()
		else:
			print('Pilihan tidak valid!')