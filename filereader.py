import re
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from glob import glob
from types import MappingProxyType
from typing import Any, Union

import pandas as pd
from global_parameters import RCD_COLUMNS, RTU_COLUMNS, SOE_COLUMNS, SOE_COLUMNS_DTYPE
from lib import CONSOLE_WIDTH, calc_time, immutable_dict, join_datetime, load_cpoint, load_workbook, read_xml, test_datetime_format, truncate


class _FileReader:
	"""Base class for reading files (xls, xlsx, xml) into dataframe.

	Args:
		filepaths : path of file(s)

	Accepted kwargs:
		switching_element : elements to be monitored
		date_start : oldest date limit of data
		date_stop : newest date limit of data
	"""
	__slot__: list[str] = ['switching_element']
	_errors: int = 0
	column_list: list[str] = []
	keep_duplicate: str = 'last'
	sheet_name: str = 'Sheet1'
	time_series_column: str = 'Timestamp'

	def __init__(self, filepaths: Union[str, list], **kwargs):
		self._date_range = None
		self.cached_file = dict()
		self.filepaths = list()

		if isinstance(filepaths, str):
			for f in filepaths.split(','):
				if '*' in f:
					g = glob(f.strip())
					if len(g)>0:
						self.filepaths += g
					else:
						print(f'Warning: File yang menyerupai "{f}" tidak ditemukan.')
				elif f.strip():
					self.filepaths.append(f.strip())
		elif isinstance(filepaths, list):
			self.filepaths = filepaths

		self.switching_element = kwargs['switching_element'] if 'switching_element' in kwargs else ['CB']
		# Set date_range if defined in kwargs
		if 'date_start' in kwargs and 'date_stop' in kwargs:
			self.set_date_range(
				date_start=kwargs['date_start'].replace(hour=0, minute=0, second=0, microsecond=0),
				date_stop=kwargs['date_stop'].replace(hour=23, minute=59, second=59, microsecond=999999)
			)

		if len(self.filepaths)>0:
			self.load()
		# Need this for cooperative multiple-inheritance
		super().__init__(**kwargs)

	@calc_time
	def _load(self, **kwargs):
		errors = 0
		count = len(self.filepaths)

		if count==0:
			raise SyntaxError('Input file belum ditentukan!')
		if count==1:
			df = self.open_file(filepath=self.filepaths[0], sheet_name=self.sheet_name, table_header=self.column_list, base_column=self.time_series_column)
	
			if isinstance(df, pd.DataFrame):
				df = self.post_open_file(df)
				df_result = df.drop_duplicates(keep=self.keep_duplicate)
			else:
				df_result = None
				errors += 1
		else:
			# Optimize file loading with ProcessPoolExecutor
			valid_df = list()
			n = 8
			chunksize = len(self.filepaths)//n + 1

			with ProcessPoolExecutor(n) as ppe:
				futures = list()

				for i in range(0, len(self.filepaths), chunksize):
					fpaths = self.filepaths[i:(i+chunksize)]
					future = ppe.submit(self.open_files_concurrently, fpaths)
					futures.append(future)

				for future in as_completed(futures):
					result_list, fpaths = future.result()

					for result in result_list:
						if isinstance(result, pd.DataFrame):
							df = self.post_open_file(result)
							valid_df.append(df)
						else:
							errors += 1

			if len(valid_df)==0: raise RuntimeError(f'Semua data tidak valid. (Error={errors})')
			# Combine each Dataframe in data list into one and eleminate duplicated rows
			df_result = pd.concat(valid_df).drop_duplicates(keep=self.keep_duplicate)

		self._errors = errors
		self.sources = ',\n'.join(self.filepaths)
		return df_result

	def load(self, **kwargs) -> None:
		"""Load each file in filepaths into dataframe.
		"""
		print(f'\nTotal {len(self.filepaths)} file...')
		df, t = self._load()
		print(f'(durasi={t:.2f}s, error={self.errors})')

		if self.errors:
			choice = input(f'Terdapat {self.errors} error, tetap lanjutkan? [y/n]  ')
			if 'y' in choice:
				# Continue
				pass
			else:
				raise RuntimeError(f'Proses dihentikan oleh user. (Error={self.errors})')

		self.post_load(df)

	def post_load(self, df: pd.DataFrame) -> None:
		"""Executed method right after all files loaded.

		Args:
			df : Dataframe
		"""
		if self._date_range==None: self.set_date_range(date_start=df[self.time_series_column].min(), date_stop=df[self.time_series_column].max())

	def open_files_concurrently(self, filepaths: list):
		"""Open multiple files with Threads.
		
		Args:
			filepaths : list of filepaths
		"""
		with ThreadPoolExecutor(len(filepaths)) as tpe:
			futures = [tpe.submit(self.open_file, filepath, sheet_name=self.sheet_name, table_header=self.column_list, base_column=self.time_series_column) for filepath in filepaths]
			data_list = [future.result() for future in futures]
			return data_list, filepaths

	def open_file(self, filepath: str, sheet_name: str = None, table_header: Union[list, tuple] = None, base_column: str = None, **kwargs):
		"""Open single file into dataframe.

		Args:
			filepath : file source to be opened
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

		if len(txt_prefix)+len(filepath)+4>txt_info_len:
			txt_path = f'"{truncate(text=filepath, max_length=txt_info_len-len(txt_prefix)-4, on="left")}"'
		else:
			txt_path = f'"{filepath}"'.ljust(txt_info_len-len(txt_prefix)-2)

		try:
			wb = load_workbook(filepath)

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
						txtstatus = f'NOK!\r\nHeader tabel tidak sesuai.'
			else:
				for ws_name, sheet in wb.items():
					# Loop through workbook sheets & match header
					if set(table_header).issubset(sheet.columns):
						df = sheet
						txtstatus = 'OK!'
						break

				if df is None: txtstatus = f'NOK!\r\nData tidak ditemukan!'

			if isinstance(df, pd.DataFrame) and base_column is not None:
				# Neglect null value on base column
				df = df[df[base_column].notnull()]

		except (ValueError, ImportError):
			try:
				# Attempting to open file with xml format
				df = read_xml(filepath, **kwargs)
			except (ValueError, ImportError):
				txtstatus = 'NOK!\r\nGagal membuka file.'
		except FileNotFoundError:
			txtstatus = 'NOK!\r\nFile tidak ditemukan.'

		self.cached_file[filepath] = df
		print(f'{txt_prefix} {txt_path} {txtstatus}')
		return df

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

		self._date_start = dtstart
		self._date_stop = dtstop
		self._date_range = (dtstart, dtstop)

	@property
	def date_range(self):
		return self._date_range

	@property
	def date_start(self):
		return self._date_start

	@property
	def date_stop(self):
		return self._date_stop

	@property
	def errors(self):
		return self._errors


class SpectrumFileReader(_FileReader):
	"""This class used generally for loading Spectrum's Sequence of Events (SOE) files.

	Args:
		filepaths : path of file(s)
	"""
	column_dtype: Union[dict, MappingProxyType] = immutable_dict(SOE_COLUMNS_DTYPE)
	column_list: list = SOE_COLUMNS
	sheet_name: str = 'HIS_MESSAGES'
	time_series_column: str = 'Time stamp'
	cpoint_file: str = 'cpoint.xlsx'

	def __init__(self, filepaths: Union[str, list], **kwargs):
		# Load point description
		self.cpoint_description = load_cpoint(self.cpoint_file)
		# Need this for cooperative multiple-inheritance
		super().__init__(filepaths, **kwargs)

	def post_load(self, df: pd.DataFrame) -> None:
		"""Executed method right after all files loaded.

		Args:
			df : Dataframe
		"""
		if pd.api.types.is_object_dtype(df['Time stamp']) or pd.api.types.is_object_dtype(df['System time stamp']):
			df['Time stamp'] = df['Time stamp'].map(lambda x: test_datetime_format(x))
			df['System time stamp'] = df['System time stamp'].map(lambda x: test_datetime_format(x))
		# Format B2 as string value
		df['B2'] = df['B2'].map(lambda x: re.sub(r'\.\d+', '', str(x)))

		super().post_load(df)
		self._soe_all = self.prepare_data(df)

	def prepare_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
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
		if hasattr(self, 'cpoint_description'):
			df_trans = self.cpoint_description.copy()
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
				print(f'{len(no_description)} poin tidak terdaftar dalam "Point Description".\n{"; ".join([str(x) for i, x in enumerate(no_description) if i<5])}{" ..." if len(no_description)>5 else ""}\nSilahkan update melalui SpectrumOfdbClient atau menambahkan manual pada file cpoint.xlsx!')
				# Fill unknown (nan) Point Description B1, B2, B3 with its own text
				new_df.loc[without_description, col2] = new_df.loc[without_description, col1].values

			if is_longtext:
				# Swap column labels, because exported His. Messages using description text
				new_df[col1 + col2] = new_df[col2 + col1]
			# Rearrange columns
			new_df = new_df[SOE_COLUMNS + col2]

		# Split into DataFrames for each purposes, not reset index
		self._soe_control_disable = new_df[(new_df['Element']=='CD') & (new_df['Status'].isin(['Disable', 'Enable', 'Dist.']))].copy()
		self._soe_local_remote = new_df[(new_df['Element']=='LR') & (new_df['Status'].isin(['Local', 'Remote', 'Dist.']))].copy()
		self._soe_rtu_updown = new_df[(new_df['B1']=='IFS') & (new_df['B2']=='RTU_P1') & (new_df['Status'].isin(['Up', 'Down']))].copy()
		self._soe_switching = new_df[(new_df['Element'].isin(self.switching_element)) & (new_df['Status'].isin(['Open', 'Close', 'Dist.']))].copy()
		self._soe_synchro = new_df[(new_df['Element']=='CSO') & (new_df['Status'].isin(['Off', 'On', 'Dist.']))].copy()
		self._soe_trip = new_df[new_df['Element'].isin(['CBTR', 'MTO'])].copy()
		return new_df

	@property
	def soe_all(self):
		return self._soe_all if hasattr(self, '_soe_all') else self.load()

	@property
	def soe_control_disable(self):
		return self._soe_control_disable if hasattr(self, '_soe_control_disable') else self.load()

	@property
	def soe_local_remote(self):
		return self._soe_local_remote if hasattr(self, '_soe_local_remote') else self.load()

	@property
	def soe_rtu_updown(self):
		return self._soe_rtu_updown if hasattr(self, '_soe_rtu_updown') else self.load()

	@property
	def soe_switching(self):
		return self._soe_switching if hasattr(self, '_soe_switching') else self.load()

	@property
	def soe_synchro(self):
		return self._soe_synchro if hasattr(self, '_soe_synchro') else self.load()

	@property
	def soe_trip(self):
		return self._soe_trip if hasattr(self, '_soe_trip') else self.load()


class RCFileReader(_FileReader):
	"""This class used for collecting / combining multiple RCD files.

	Args:
		filepaths : path of file(s)
	"""
	column_list: list = RCD_COLUMNS
	sheet_name: str = 'RC_ONLY'
	time_series_column: str = 'Order Time'

	def __init__(self, filepaths: Union[str, list], **kwargs):
		# Need this for cooperative multiple-inheritance
		super().__init__(filepaths, **kwargs)

	def post_load(self, df:pd.DataFrame) -> None:
		"""Executed method right after all files loaded.

		Args:
			df : Dataframe
		"""
		# Format B2 as string value
		df['B2'] = df['B2'].map(lambda x: re.sub(r'\.\d+', '', str(x)))

		super().post_load(df)
		self._rcd_all = self.prepare_data(df)

	def prepare_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
		"""Filtering & convertion process of dataframe input.

		Args:
			df : Dataframe
		"""
		# Filter new DataFrame
		new_df = df.loc[(df[self.time_series_column]>=self.date_range[0]) & (df[self.time_series_column]<=self.date_range[1]), self.column_list].copy()
		new_df = new_df.fillna('').sort_values([self.time_series_column], ascending=[True]).reset_index(drop=True)
		return new_df

	@property
	def rcd_all(self):
		return self._rcd_all if hasattr(self, '_rcd_all') else self.load()


class AVRSFileReader(_FileReader):
	"""Class used for collecting / combining multiple AVRS files.

	Args:
		filepaths : path of file(s)
	"""
	column_list: list = RTU_COLUMNS
	column_mark: list = [
		'Marked Maintenance',
		'Marked Link Failure',
		'Marked RTU Failure',
		'Marked Other Failure'
	]
	sheet_name: str = 'DOWNTIME'
	time_series_column: str = 'Down Time'

	def __init__(self, filepaths: Union[str, list], **kwargs):
		# Need this for cooperative multiple-inheritance
		super().__init__(filepaths, **kwargs)

	def post_load(self, df: pd.DataFrame) -> None:
		"""Executed method right after all files loaded.

		Args:
			df : Dataframe
		"""
		super().post_load(df)
		self._rtudown_all = self.prepare_data(df)

	def prepare_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
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

	@property
	def rtudown_all(self):
		return self._rtudown_all if hasattr(self, '_rtudown_all') else self.load()


class SurvalentFileReader(_FileReader):
	column_dtype: MappingProxyType[str, dict[str, Any]] = immutable_dict(SOE_COLUMNS_DTYPE)
	column_list: list[str] = ['Time', 'Point', 'Message', 'Operator']
	sheet_name: str = 'Report'
	time_series_column: str = 'Time'
	datetime_format: str = '%Y-%m-%d %H:%M:%S.%f'
	elements: list[str] = ['CBTR', 'CD', 'CSO', 'LR', 'MTO']
	statuses: dict[str, str] = {'opened': 'Open', 'closed': 'Close', 'enabled': 'Enable', 'disabled': 'Disable', 'appear': 'Appeared'}
	b3_dict: dict[str, str] = dict()
	cmd_order: list[int] = list()
	cmd_neg_feedback: list[int] = list()

	def post_load(self, df: pd.DataFrame) -> None:
		"""Executed method right after all files loaded.

		Args:
			df : Dataframe
		"""
		df_extracted = self.extractor(df)
		self._soe_all = df_extracted
		# Change time series reference
		self.time_series_column = 'Time stamp'
		super().post_load(df_extracted)

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
		df0 = df0[(df0['Element'].isin(self.elements + self.switching_element)) & ~((df0['Message'].str.contains('Put in scan')) | (df0['Message'].str.contains('ALL ALARMS BLOCKED')) | (df0['Message'].str.contains('Blocked')) | (df0['Message'].str.contains('Unblocked')))].reset_index(drop=True)
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
				raise IndexError(f'Error extracting value! key="{msg}", string=[{pb3}, {elm}]')

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
				raise IndexError(f'Error extracting value! index={i} key="{msg}", string=[{b3}, {elm}]')

			df.loc[i, ['B1', 'B3', 'Status', 'Tag']] = [b1, b3, sts, tag]


	@property
	def soe_all(self):
		return self._soe_all if hasattr(self, '_soe_all') else self.load()


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
	return f

def test_wrong_file(**params):
	print(' TEST WRONG FILE '.center(CONSOLE_WIDTH, '#'))
	f = SpectrumFileReader('sample/wrong_file_1.xlsx')
	return f

def test_file_spectrum(**params):
	print(' TEST FILE SOE SPECTRUM '.center(CONSOLE_WIDTH, '#'))
	f = SpectrumFileReader('sample/sample_rcd*.xlsx')
	print('\r\n' + ' TEST FILE SOE SPECTRUM CONCURRENTLY '.center(CONSOLE_WIDTH, '#'))
	# f.load_()
	return f

def test_file_survalent(**params):
	print(' TEST FILE SOE SURVALENT '.center(CONSOLE_WIDTH, '#'))
	f = SurvalentFileReader('sample/survalent/sample_soe*.XLSX')
	print('\r\n' + ' TEST FILE SOE SURVALENT CONCURRENTLY '.center(CONSOLE_WIDTH, '#'))
	# f.load_()
	return f

def test_file_rcd_collective(**params):
	print(' TEST FILE RCD COLLECTIVE '.center(CONSOLE_WIDTH, '#'))
	f = RCFileReader('sample/sample_rcd*.xlsx')
	return f

def test_file_rtu_collective(**params):
	print(' TEST FILE RTU COLLECTIVE '.center(CONSOLE_WIDTH, '#'))
	f = AVRSFileReader('sample/sample_rtu*.xlsx')
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
			print()
			test = test_list[choice][1]()
		else:
			print('Pilihan tidak valid!')