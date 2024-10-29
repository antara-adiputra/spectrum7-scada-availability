import asyncio, datetime, getpass, os, socket
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from types import MappingProxyType
from typing import Any, Dict, List, Callable, Literal, Optional, Tuple, TypeAlias, Union

import config, cryptography.fernet, settings
import pandas as pd
import sqlalchemy as sa
from global_parameters import SOE_COLUMNS, SOE_COLUMNS_DTYPE
from lib import ProcessError, calc_time, immutable_dict, load_cpoint, validate_cpoint


DtypeMapping: TypeAlias = MappingProxyType[str, Dict[str, Any]]


class SpectrumOfdbClient:
	"""This class used for collect Sequence of Events (SOE) from Spectrum Offline Database.

	Args:
		date_start : oldest date limit
		date_start : newest date limit
	"""
	__slot__ = ['connection_driver', 'switching_element']
	_errors: List[Any]
	_warnings: List[Any]
	_schemas: str = config.OFDB_SCHEMA
	_tables: MappingProxyType = immutable_dict({
		'point': config.OFDB_TABLE_POINT,
		'analog': config.OFDB_TABLE_ANALOG,
		'digital': config.OFDB_TABLE_DIGITAL,
		'historical': config.OFDB_TABLE_HISTORICAL,
	})
	_conf_path = '.config'
	column_dtype: DtypeMapping = immutable_dict(SOE_COLUMNS_DTYPE)
	column_list: List[str] = SOE_COLUMNS
	cpoint_file: str = 'cpoint.xlsx'
	tzone: datetime.timedelta = datetime.timedelta(hours=8)	# Timezone for Asia/Makassar
	exception_prefix: str = 'LoadFileError'
	keep_duplicate: str = 'last'
	t_timeout: float = config.COMMUNICATION_TIMEOUT

	def __init__(self, date_start: datetime.datetime = None, date_stop: datetime.datetime = None, **kwargs):
		self.available_drivers = config.DB_DRIVERS
		self.sources = f'DRIVER={config.OFDB_DRIVER};SERVER={config.OFDB_HOSTNAME};PORT={config.OFDB_PORT};'
		self.switching_element = kwargs['switching_element'] if 'switching_element' in kwargs else ['CB']
		self.initialize()
		self.reset_date()
		# Set date_range if defined in kwargs
		if isinstance(date_start, datetime.datetime):
			self.set_date_range(date_start, date_stop)
		super().__init__(**kwargs)

	def _run_task(self, **kwargs):
		"""Run multiple task queries."""
		df_list = [
			self.fetch_element(['CB', 'CBTR', 'CD', 'CSO', 'LR', 'MTO']),
			self.fetch_rtu_updown()
		]
		return df_list
	
	def initialize(self) -> None:
		"""Initialize instance."""
		self._errors = list()
		self._warnings = list()

	def check_server(self) -> bool:
		"""Check connection to server."""
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.settimeout(self.t_timeout)
		try:
			sock.connect((config.OFDB_HOSTNAME, int(config.OFDB_PORT)))
		except Exception:
			return False
		else:
			sock.close()
			return True

	def sql_conditional_from_dict(self, filter: Dict[str, Any], operator: str) -> str:
		"""Create SQL conditional query from dict-like filter and "AND/OR" operator.

		Args:
			filter : dict of key & value filters
			operator : logical operator

		Result:
			SQL conditional query string
		"""
		buffer = list()
		oper = f' {operator} '

		for xkey, xval in filter.items():
			if xkey in ['AND', 'OR']:
				# x is logic operand
				if xval: buffer.append(self.sql_conditional_from_dict(xval, xkey))
			elif str(xkey).endswith('time_stamp'):
				time0, time1 = xval[0].strftime('%Y-%m-%d %H:%M:%S'), xval[1].strftime('%Y-%m-%d %H:%M:%S')
				buffer.append(f"({xkey} BETWEEN '{time0}' AND '{time1}')")
			else:
				if isinstance(xval, list):
					multival = [f"RTRIM({xkey}) = '{val}'" for val in xval]
					buffer.append(f"({' OR '.join(multival)})")
				elif xval=='':
					buffer.append(f"({xkey} IS NULL OR RTRIM({xkey}) = '')")
				elif str(xval).endswith('NULL'):
					buffer.append(f"{xkey} {'IS NOT' if str(xval).startswith('-') else 'IS'} NULL")
				else:
					buffer.append(f"RTRIM({xkey}) {'<>' if isinstance(xval, str) and str(xval).startswith('-') else '='} '{xval}'")
		return f"({oper.join(buffer)})"

	def sql_order_from_list(self, columns: List[str]) -> str:
		"""Create SQL order by query from list.

		Args:
			columns : list of table columns

		Result:
			SQL order by query string
		"""
		buffer = list()
		for column in columns:
			how = 'DESC' if column.startswith('-') else 'ASC'
			buffer.append(f"{column} {how}")
		return ", ".join(buffer)
	
	def get_tablename(self, name: str, prefix: bool = True) -> str:
		return f'{self.schemas}.{self.tables[name]}' if prefix else self.tables[name]

	@calc_time
	def fetch(self, querystring: str) -> Tuple[pd.DataFrame, float]:
		"""Create a connection instance and execute sql query.

		Args:
			querystring : SQL querystring

		Result:
			Table data as dataframe
		"""
		def decrypt(token: str):
			key = cryptography.fernet.Fernet(os.getenv('PRIVATE_KEY'))
			try:
				pwd = key.decrypt(token.encode()).decode()
			except cryptography.fernet.InvalidToken:
				pwd = ''
			return pwd

		if self.check_server():
			# Server connection OK
			connection_string = f'DRIVER={config.OFDB_DRIVER};SERVER={config.OFDB_HOSTNAME};PORT={config.OFDB_PORT};DATABASE={config.OFDB_DATABASE};UID={config.OFDB_USER};PWD={decrypt(config.OFDB_TOKEN)};Trusted_Connection=No;'
			connection_url = sa.engine.URL.create('mssql+pyodbc', query={"odbc_connect": connection_string})
			engine = sa.create_engine(connection_url)
			with engine.begin() as conn:
				df = pd.read_sql_query(sa.text(querystring), conn)
				df = df.drop_duplicates(keep=self.keep_duplicate)
		else:
			raise ProcessError(self.exception_prefix, 'Tidak dapat menghubungkan ke server Ofdb.')
		return df

	def setup_digital(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Translate digital table column name."""
		columns_map = {
			'value': 'Status',
			'quality_code': 'Quality',
			'quality_code_scada': 'SCADA value',
			'point_text': 'Path text'
		}
		# Remove first "/" and unnecessary spaces
		point_text = df['point_text'].str.replace(r'^\/|\s{2,}', '', regex=True)
		df[['path1text', 'path2text', 'path3text', 'path4text', 'path5text']] = point_text.str.split(pat='/', expand=True)
		return self.setup_historical(df, **columns_map)

	def setup_historical(self, df: pd.DataFrame, **maps) -> pd.DataFrame:
		"""Translate message table column name and filter specified column only."""
		columns_map = {
			'ack': 'A',
			'time_stamp': 'Time stamp',
			'msec': 'Milliseconds',
			'system_time_stamp': 'System time stamp',
			'system_msec': 'System milliseconds',
			'path1': 'B1',
			'path2': 'B2',
			'path3': 'B3',
			'path4': 'Element',
			'path5': 'Information',
			'msgstatus': 'Status',
			'tag': 'Tag',
			'msgoperator': 'Operator',
			'priority': 'Priority',
			'msgclass': 'Message class',
			'comment_text': 'Comment',
			'path1text': 'B1 text',
			'path2text': 'B2 text',
			'path3text': 'B3 text',
			'path4text': 'Element text',
			'path5text': 'Information Text',
			'value': 'Raw value',
			'elem': 'Element text',
			'console': 'Console',
			'message_text': 'Message'
		}
		columns_map.update(maps)
		col1 = ['B1', 'B2', 'B3']
		col2 = ['B1 text', 'B2 text', 'B3 text']
		# Initialize new dataframe of Historical Messages
		df_his = pd.DataFrame(columns=SOE_COLUMNS)

		for col in df.columns:
			new_col = columns_map[col]

			if new_col in df_his.columns:
				# Remove unnecessary spaces on begining and or trailing string object
				if pd.api.types.is_object_dtype(df[col]):
					df_his[new_col] = df[col].str.strip()
				else:
					df_his[new_col] = df[col]

		df_his['Status'] = df_his['Status'].str.title()

		# Merge B1, B2, B3 translation with existing table
		df_his = df_his.merge(self.cpoint_ifs, on=col1, how='left')
		without_description = df_his['B1 text'].isna()

		if df_his[without_description].shape[0]>0:
			# List unknown (nan) Point Description
			no_description = df_his.loc[without_description, col1].drop_duplicates(keep='first').values
			self.warnings.extend([f'{"/".join(map(lambda s: str(s), point))} tidak terdaftar dalam Point Description.' for point in no_description])
			print(f'\n{len(no_description)} poin tidak terdaftar dalam "Point Description".\n{"; ".join([str(x) for i, x in enumerate(no_description) if i<5])}{" ..." if len(no_description)>5 else ""}\nSilahkan update melalui SpectrumOfdbClient atau menambahkan manual pada file cpoint.xlsx!')
			# Fill unknown (nan) Point Description B1, B2, B3 with its own text
			df_his.loc[without_description, col2] = df_his.loc[without_description, col1].values

		his_dtype = {key: val for key, val in self.column_dtype.items() if key in df_his.columns}
		df_his = df_his.astype(his_dtype)\
			.fillna('')\
			.replace({'None': '', 'nan': ''})\
			.sort_values(['System time stamp', 'System milliseconds', 'Time stamp', 'Milliseconds'], ascending=[True, True, True, True])\
			.reset_index(drop=True)
		return df_his

	def fetch_point(self) -> pd.DataFrame:
		"""Fetch Point Description data from server."""
		table_columns: List[str] = ['point_number', 'path1', 'path2', 'path3', 'path4', 'path5', 'point_name', 'point_text']
		output_columns: List[str] = ['B1', 'B2', 'B3', 'B1 text', 'B2 text', 'B3 text']
		columns_map: Dict[str, str] = {
			'point_number': 'Point number',
			'path1': 'B1',
			'path2': 'B2',
			'path3': 'B3',
			'path4': 'Element',
			'path5': 'Information',
			'path1text': 'B1 text',
			'path2text': 'B2 text',
			'path3text': 'B3 text',
			'path4text': 'Element text',
			'path5text': 'Information Text',
			'point_name': 'Point name',
			'point_text': 'Point text'
		}
		filters: Dict[str, Any] = {'active': 'T'}
		qs_column = ', '.join(table_columns)
		qs_where = self.sql_conditional_from_dict(filters, 'AND')
		qs_order = self.sql_order_from_list(['path1', 'path2', 'path3', 'path4'])
		# Create SQL Query
		querystring = f"SELECT {qs_column} FROM {self.get_tablename('point')} WHERE {qs_where} ORDER BY {qs_order};"
		# Fetch data from server
		raw, t = self.fetch(querystring)
		print(f'Data Point Description durasi {datetime.datetime.fromtimestamp(t).strftime("%H:%M:%S")}')
		# Remove first "/" and unnecessary spaces
		point_name = raw['point_name'].str.replace(r'^\/|\s{2,}', '', regex=True)
		point_text = raw['point_text'].str.replace(r'^\/|\s{2,}', '', regex=True)
		raw['point_name'] = point_name
		raw['point_text'] = point_text
		raw[['path1', 'path2', 'path3', 'path4', 'path5']] = point_name.str.split(pat='/', expand=True)
		raw[['path1text', 'path2text', 'path3text', 'path4text', 'path5text']] = point_text.str.split(pat='/', expand=True)
		# Initialize new dataframe of Point Description
		df_point = pd.DataFrame(columns=[val for val in columns_map.values()])

		for col in raw.columns:
			new_col = columns_map[col]
			if new_col in df_point.columns:
				# Remove unnecessary spaces on begining and or trailing string object
				df_point[new_col] = raw[col].str.strip() if pd.api.types.is_object_dtype(raw[col]) else raw[col]

		point_dtype = {col: 'str' for col in df_point.columns if col!='Point number'}
		df_point = df_point.astype(point_dtype)\
			.fillna('')\
			.replace({'None': '', 'nan': ''})\
			.sort_values(['B1', 'B2', 'B3', 'Element'], ascending=[True, True, True, True])\
			.reset_index(drop=True)
		self.cpoint_ifs = df_point[['B1', 'B2', 'B3', 'B1 text', 'B2 text', 'B3 text']]
		return validate_cpoint(df_point[output_columns])

	def fetch_rtu_updown(self) -> pd.DataFrame:
		"""Fetch IFS Up/Down data from server."""
		if not self.date_isset: raise AttributeError(f'Range waktu belum diset. Jalankan "set_date_range" terlebih dahulu.')

		ltable = self.get_tablename('digital')
		ltbl = 'dgtl'
		rtable = self.get_tablename('point')
		rtbl = 'cpnt'
		filter_ifs = {'path1': 'IFS', 'path2': 'RTU_P1', 'path4': 'IFS-RTU'}
		filters = {f'{ltbl}.system_time_stamp': self.date_range_utc, **{f'{rtbl}.{key}': val for key, val in filter_ifs.items()}}
		qs_where = self.sql_conditional_from_dict(filters, 'AND')
		qs_order = self.sql_order_from_list([f'{ltbl}.system_time_stamp', f'{ltbl}.system_msec'])

		querystring = f"SELECT {ltbl}.time_stamp, {ltbl}.msec, {ltbl}.system_time_stamp, {ltbl}.system_msec, {rtbl}.path1, {rtbl}.path2, {rtbl}.path3, {rtbl}.path4, {rtbl}.path5, {ltbl}.value, {ltbl}.quality_code_scada, {rtbl}.point_text FROM (SELECT DISTINCT * FROM {ltable}) AS {ltbl} " +\
		f"INNER JOIN {rtable} AS {rtbl} ON {ltbl}.point_number={rtbl}.point_number " +\
		f"WHERE {qs_where} ORDER BY {qs_order};"
		df_ifs, t = self.fetch(querystring)
		print(f'Data RTU Up/Down durasi {datetime.datetime.fromtimestamp(t).strftime("%H:%M:%S")}')
		# Modify system_time_stamp from UTC to Local time
		df_ifs['system_time_stamp'] = df_ifs['system_time_stamp'].map(lambda x: x + self.tzone)
		# Translate from value 0-1 to Down-Up
		df_ifs['value'] = df_ifs['value'].map(lambda x: 'Up' if int(float(x))==1 else 'Down')
		# df_updown = self.setup_digital(df_ifs)
		return df_ifs

	def fetch_element(self, element: Union[str, List[str]]) -> pd.DataFrame:
		"""Fetch Historical Messages (SOE) data based on specific element.

		Args:
			element : name of element
		"""
		if not self.date_isset: raise AttributeError(f'Range waktu belum diset. Jalankan "set_date_range" terlebih dahulu.')

		filters = {'system_time_stamp': self.date_range_utc, 'ack': '', 'path4': element, 'path5': 'Status', 'value': '-NULL'}
		qs_column = ', '.join(['ack', 'time_stamp', 'msec', 'system_time_stamp', 'system_msec', 'path1', 'path2', 'path3', 'path4', 'path5', 'value', 'priority', 'tag', 'msgoperator', 'msgclass', 'message_text', 'comment_text', 'path1text', 'path2text', 'path3text', 'elem', 'msgstatus'])
		qs_where = self.sql_conditional_from_dict(filters, 'AND')
		qs_order = self.sql_order_from_list(['system_time_stamp', 'system_msec'])

		querystring = f"SELECT {qs_column} FROM {self.get_tablename('historical')} WHERE {qs_where} ORDER BY {qs_order}"
		df_his, t = self.fetch(querystring)
		print(f'Data {", ".join(element)} durasi {datetime.datetime.fromtimestamp(t).strftime("%H:%M:%S")}')
		# Modify system_time_stamp from UTC to Local time
		df_his['system_time_stamp'] = df_his['system_time_stamp'].map(lambda x: x + self.tzone)
		# df_soe = self.setup_historical(df_his)
		return df_his

	def fetch_all(self, **kwargs) -> pd.DataFrame:
		"""Fetch all data from server."""
		self.initialize()
		self.fetch_point()
		df_list = self._run_task()
		if all([isinstance(x, pd.DataFrame) for x in df_list]):
			df = pd.concat(df_list)\
				.drop_duplicates(keep=self.keep_duplicate)\
				.sort_values(['System time stamp', 'System milliseconds', 'Time stamp', 'Milliseconds'], ascending=[True, True, True, True])\
				.reset_index(drop=True)
		else:
			raise ProcessError(self.exception_prefix, 'Gagal mengambil data dari server.')
		return self.prepare_data(df)

	async def async_fetch_all(self, key: Literal['avrcd', 'avrs'], **kwargs) -> pd.DataFrame:
		"""Fetch all data from server asynchronously."""

		async def run_background(proc: ProcessPoolExecutor, fn: Callable, *fnargs, **fnkwargs):
			loop = asyncio.get_running_loop() or asyncio.get_event_loop()
			return await loop.run_in_executor(proc, partial(fn, *fnargs, **fnkwargs))

		def done_soe(_f: asyncio.Future):
			df = _f.result()
			df_soe = self.setup_historical(df)
			df_list.append(df_soe)

		def done_updown(_f: asyncio.Future):
			df = _f.result()
			df_updown = self.setup_digital(df)
			df_list.append(df_updown)

		self.initialize()
		df_list: List[pd.DataFrame] = list()
		tasks = set()
		n = min(os.cpu_count(), settings.MAX_CPU_USAGE)
		executor = ProcessPoolExecutor(n)

		self.fetch_point()
		task1 = asyncio.create_task(run_background(executor, self.fetch_rtu_updown))
		task1.add_done_callback(done_updown)
		tasks.add(task1)

		if key=='avrcd':
			task2 = asyncio.create_task(run_background(executor, self.fetch_element, ['CB', 'CBTR', 'CD', 'CSO', 'LR', 'MTO']))
			task2.add_done_callback(done_soe)
			tasks.add(task2)

		await asyncio.gather(*tasks)
		executor.shutdown()

		if all([isinstance(x, pd.DataFrame) for x in df_list]):
			df = pd.concat(df_list)\
				.drop_duplicates(keep=self.keep_duplicate)\
				.sort_values(['System time stamp', 'System milliseconds', 'Time stamp', 'Milliseconds'], ascending=[True, True, True, True])\
				.reset_index(drop=True)
		else:
			raise ProcessError(self.exception_prefix, 'Gagal mengambil data dari server.')
		return self.prepare_data(df)

	def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Filtering, convertion & validation process of dataframe input then split into specific purposes.

		Args:
			df : Dataframe
		"""
		new_df = df.copy().fillna('')
		# Split into DataFrames for each purposes, not reset index
		self.soe_control_disable = new_df[(new_df['Element']=='CD') & (new_df['Status'].isin(['Disable', 'Enable', 'Dist.']))].copy()
		self.soe_local_remote = new_df[(new_df['Element']=='LR') & (new_df['Status'].isin(['Local', 'Remote', 'Dist.']))].copy()
		self.soe_rtu_updown = new_df[(new_df['B1']=='IFS') & (new_df['B2']=='RTU_P1') & (new_df['Status'].isin(['Up', 'Down']))].copy()
		self.soe_switching = new_df[(new_df['Element'].isin(self.switching_element)) & (new_df['Status'].isin(['Open', 'Close', 'Dist.']))].copy()
		self.soe_synchro = new_df[(new_df['Element']=='CSO') & (new_df['Status'].isin(['Off', 'On', 'Dist.']))].copy()
		self.soe_trip = new_df[new_df['Element'].isin(['CBTR', 'MTO'])].copy()
		return new_df

	def select_driver(self):
		"""Select installed pyodbc driver (for console user only)."""
		retry = 0
		count = len(self.available_drivers)

		if count>0:
			valid = False
			print('List driver ODBC yang terinstall :')
			for i, drv in enumerate(self.available_drivers):
				print(f'{i+1}. {drv}')

			while not valid:
				try:
					selected = int(input(f'\nPilih driver (1-{count}) : ')) - 1
				except ValueError:
					selected = -1
					retry += 1

				if selected in range(count):
					driver = self.available_drivers[selected]
					valid = True
				else:
					retry += 1

				if retry>3:
					raise ValueError('Program terhenti. Gagal 3 kali percobaan.')
			return driver
		else:
			raise ImportError('Tidak ada driver ODBC yang terinstall!')

	def set_connection(self, **conf):
		"""Attach configuration into instance."""
		allowed = ['hostname', 'port', 'user', 'pswd', 'database', 'driver']

		if conf:
			# Set connection parameter
			for opt, val in conf.items():
				if opt in allowed:
					setattr(self, f'_conn_{opt.lower()}', str(val))
				else:
					raise KeyError(f'Variabel {opt} tidak dikenal!')
		else:
			self._conn_hotname = input('Host\t\t: ')
			self._conn_port = input('Port\t\t: ')
			self._conn_user = input('User\t\t: ')
			self._conn_pswd = getpass.getpass('Password\t: ')
			self._conn_database = input('Database\t: ')
			print('')
			self._conn_driver = self.select_driver()

	def reset_date(self) -> None:
		"""Reset date range."""
		self._date_range = None
		self._date_isset = False

	def set_date_range(self, date_start: datetime.datetime, date_stop: Optional[datetime.datetime] = None):
		"""Set date range from given parameters.

		Args:
			date_start : oldest date limit
			date_stop : newest date limit
		"""
		dtstart = date_start.to_pydatetime() if isinstance(date_start, pd.Timestamp) else date_start
		dtstop = date_stop.to_pydatetime() if isinstance(date_stop, pd.Timestamp) else date_stop

		if date_stop is None:
			# date_stop is not defined
			if date_start<datetime.datetime.now():
				# valid date_start
				if (datetime.datetime.now()-date_start).days>31:
					dtstart = date_start.replace(hour=0, minute=0, second=0, microsecond=0)
					dtstop = date_start.replace(hour=23, minute=59, second=59, microsecond=999999) + datetime.timedelta(days=29)
				else:
					dtstart = date_start.replace(hour=0, minute=0, second=0, microsecond=0)
					dtstop = datetime.datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
			else:
				raise ValueError('"date_start" should not greater than "current_time".')
		else:
			# date_start and date_stop are defined
			if date_start>date_stop:
				dtstop = date_stop.replace(hour=0, minute=0, second=0, microsecond=0)
				dtstart = date_start.replace(hour=23, minute=59, second=59, microsecond=999999)
			else:
				dtstart = date_start.replace(hour=0, minute=0, second=0, microsecond=0)
				dtstop = date_stop.replace(hour=23, minute=59, second=59, microsecond=999999)

		self._date_range = (dtstart, dtstop)
		self._date_isset = True

	def dump_point(self, data: Optional[pd.DataFrame] = None) -> None:
		"""Store point name description.
		
		Args:
			data : point description dataframe
		"""
		print('\nMengupdate "Point Name Description"...')
		if data is None:
			df = self.fetch_point()
		else:
			df = data
		# Write to file
		df.to_excel(self.cpoint_file, index=False)
		print(f'\rData berhasil disimpan kedalam file {self.cpoint_file}.')


	@property
	def date_isset(self):
		return self._date_isset

	@property
	def date_range(self):
		return self._date_range

	@property
	def date_range_utc(self):
		return (self.date_start-self.tzone, self.date_stop-self.tzone) if self.date_range else None

	@property
	def date_start(self):
		return self.date_range[0] if self.date_range else None

	@property
	def date_stop(self):
		return self.date_range[1] if self.date_range else None

	@property
	def schemas(self):
		return self._schemas

	@property
	def tables(self):
		return self._tables

	@property
	def errors(self):
		return self._errors

	@property
	def warnings(self):
		return self._warnings


if __name__=='__main__':
	c = SpectrumOfdbClient()
	c.set_date_range(datetime.datetime(2024,7,1), datetime.datetime(2024,7,31))