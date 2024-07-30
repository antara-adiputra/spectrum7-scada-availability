import socket, datetime
from configparser import ConfigParser
from types import MappingProxyType
from typing import Any, Dict, List, Callable, Literal, Optional, Tuple, TypeAlias, Union

import config
import pandas as pd
import sqlalchemy as sa
from global_parameters import SOE_COLUMNS, SOE_COLUMNS_DTYPE
from lib import calc_time, immutable_dict, load_cpoint, validate_cpoint


DtypeMapping: TypeAlias = MappingProxyType[str, Dict[str, Any]]


class SpectrumOfdbClient:
	__slot__ = ['connection_driver', 'switching_element']
	_errors: List[Any]
	_warnings: List[Any]
	_dbo_point: str = config.OFDB_TABLE_POINT
	_dbo_digital: str = config.OFDB_TABLE_DIGITAL
	_dbo_historical: str = config.OFDB_TABLE_HISTORICAL
	_schemas: str = config.OFDB_SCHEMA
	_tables: MappingProxyType = immutable_dict({
		'point': config.OFDB_TABLE_POINT,
		'digital': config.OFDB_TABLE_DIGITAL,
		'historical': config.OFDB_TABLE_HISTORICAL,
	})
	_conf_path = '.config'
	column_dtype: DtypeMapping = immutable_dict(SOE_COLUMNS_DTYPE)
	column_list: List[str] = SOE_COLUMNS
	point_desc_file: str = 'cpoint.xlsx'
	tzone: datetime.timedelta = datetime.timedelta(hours=8)	# Timezone for Asia/Makassar
	keep_duplicate: str = 'last'
	t_timeout: float = config.COMMUNICATION_TIMEOUT
	filter_his = {'ack': '', 'path4': 'CB', 'path5': 'Status', 'value': '-NULL'}
	filter_ifs = {'path1': 'IFS', 'path2': 'RTU_P1', 'path4': 'IFS-RTU'}

	def __init__(self, date_start: datetime.datetime = None, date_stop: datetime.datetime = None, **kwargs):
		self._date_range = None
		self.available_drivers = config.DB_DRIVERS

		self._init_connection()
		self.sources = f'DRIVER={config.OFDB_DRIVER};SERVER={config.OFDB_HOSTNAME};PORT={config.OFDB_PORT};'
		self.switching_element = kwargs['switching_element'] if 'switching_element' in kwargs else ['CB']
		# Set date_range if defined in kwargs
		if isinstance(date_start, datetime.datetime):
			self.set_date_range(date_start, date_stop)
		# # Automatically update Point Description
		# if self.check_server():
		# 	self.dump_point_description()
		# else:
		# 	# Load point description
		# 	self._cpoint_description = load_cpoint(self.point_desc_file)
		super().__init__(**kwargs)

	def _init_connection(self):
		"""Initialize server connection."""
		# Load configuration
		self.setting = ConfigParser(default_section='GENERAL')
		self.setting.read(self._conf_path)

		if self.setting.has_section('CONNECTION'):
			c = self.setting['CONNECTION']
			self.set_connection(host=config.OFDB_HOSTNAME, port=config.OFDB_PORT, user=c.get('USER'), pswd=c.get('PSWD'), database=config.OFDB_DATABASE, driver=config.OFDB_DRIVER)
		else:
			# .config file can be not exist or no connection section
			self.setting.add_section('CONNECTION')
			self.set_connection()

	def _run_task(self, **kwargs):
		"""Run multiple task queries."""
		df_list = [
			self.read_query_control_disable(**kwargs),
			self.read_query_local_remote(**kwargs),
			self.read_query_rtu_updown(**kwargs),
			self.read_query_switching(**kwargs),
			self.read_query_synchro(**kwargs),
			self.read_query_trip(**kwargs)
		]
		return df_list

	def all(self, force: bool = False, **kwargs):
		"""Concatenate all soe data into single DataFrame.

		Args:
			force : 
		"""
		if force or not hasattr(self, 'soe_all'):
			df_list = self._run_task(force=True)

			if all([type(x)==pd.DataFrame for x in df_list]):
				df = pd.concat(df_list).drop_duplicates(keep=self.keep_duplicate).sort_values(['System time stamp', 'System milliseconds', 'Time stamp', 'Milliseconds'], ascending=[True, True, True, True]).reset_index(drop=True)

				df_list = self._run_task(query=df, reset_index=True)
			else:
				df = None
				print('Gagal memuat data dari server.')

			self.soe_all = df
		
		return self.soe_all

	def check_server(self):
		"""Check connection to server."""
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.settimeout(self.t_timeout)
		try:
			sock.connect((self._conn_host, int(self._conn_port)))
		except Exception:
			return False
		else:
			sock.close()
			return True

	def dump_point_description(self, type:str='excel', force:bool=False):
		"""
		Store point name description.
		"""

		print('\nMengupdate "Point Name Description"...')
		if force:
			df = self.read_query_cpoint(force=True)
		else:
			df = self.cpoint_description

		self._cpoint_description = validate_cpoint(df)

		print('Menyimpan kedalam file...', end='', flush=True)
		self.cpoint_description.to_excel(self.point_desc_file, index=False)
		print(f'\rData berhasil disimpan kedalam file {self.point_desc_file}.')

	def sql_conditional_from_dict(self, filter: Dict[str, Any], operator: str):
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

	def sql_order_from_list(self, columns: List[str], operator: str):
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

	def get_cpoint_querystring(self, sort: Literal['ASC', 'DESC'] = 'ASC', filters: dict = {}, count: Optional[int] = None):
		"""Create SQL querystring to read Point table.

		Args:
			sort : how to sort the query
			filters : sql conditional rules
			count : count of the rows

		Result:
			SQL querystring text
		"""
		new_filters = filters.copy()
		qs_column = ', '.join(['point_number', 'path1', 'path2', 'path3', 'path4', 'path5', 'point_name', 'point_text'])
		qs_where = self.sql_conditional_from_dict(new_filters, 'AND')
		qs_order = self.sql_order_from_list(['path1', 'path2', 'path3', 'path4'])
		return f"SELECT {qs_column} FROM {self._dbo_point} WHERE {qs_where} ORDER BY {qs_order};"

	def get_digital_cpoint_querystring(self, sort:str='ASC', count=None):
		"""
		Create SQL querystring to specifically read between CPoint and HisMessage table.
		"""

		if not self.date_isset: raise AttributeError(f'"date_range" is not defined. Run set_date_range first.')
		tbl1 = self._dbo_digital
		tbl1_alias = 'dgtl'
		tbl2 = self._dbo_point
		tbl2_alias = 'cpnt'

		new_filters = {f'{tbl1_alias}.system_time_stamp': self.date_range_utc, **{f'{tbl2_alias}.{key}': val for key, val in self.filter_ifs.items()}}
		qs_count = f'TOP {str(count)}' if type(count)==int else ''
		qs_where = self.sql_conditional_from_dict(new_filters, 'AND')

		querystring = f"""SELECT {qs_count}
		{tbl1_alias}.time_stamp, {tbl1_alias}.msec, {tbl1_alias}.system_time_stamp, {tbl1_alias}.system_msec, {tbl2_alias}.path1, {tbl2_alias}.path2, {tbl2_alias}.path3, {tbl2_alias}.path4, {tbl2_alias}.path5, {tbl1_alias}.value, {tbl1_alias}.quality_code_scada, {tbl2_alias}.point_text
		FROM (SELECT DISTINCT * FROM {tbl1}) AS {tbl1_alias}
		INNER JOIN {tbl2} AS {tbl2_alias} ON {tbl1_alias}.point_number={tbl2_alias}.point_number
		WHERE {qs_where}
		ORDER BY {tbl1_alias}.system_time_stamp {sort}, {tbl1_alias}.system_msec {sort};
		"""

		return querystring

	def get_message_querystring(self, sort:str='ASC', filters:dict={}, count=None):
		"""
		Create SQL querystring to read table scd_his_message.
		"""

		if not self.date_isset: raise AttributeError(f'"date_range" is not defined. Run set_date_range first.')
		new_filters = {'system_time_stamp': self.date_range_utc, **filters}
		qs_count = f'TOP {str(count)}' if type(count)==int else ''
		qs_where = self.sql_conditional_from_dict(new_filters, 'AND')

		querystring = f"""SELECT {qs_count}
		ack, time_stamp, msec, system_time_stamp, system_msec, path1, path2, path3, path4, path5, value, priority, tag, msgoperator, msgclass, message_text, comment_text, path1text, path2text, path3text, elem, msgstatus
		FROM {self._dbo_historical}
		WHERE {qs_where}
		ORDER BY system_time_stamp {sort}, system_msec {sort};
		"""

		return querystring
	
	def prepare_cpoint(self, df:pd.DataFrame, **kwargs):
		"""
		Translate cpoint table column name and filter specified column only
		"""

		columns_name = {
			'point_number': 'Point number', 'path1': 'B1', 'path2': 'B2', 'path3': 'B3', 'path4': 'Element', 'path5': 'Information',
			'path1text': 'B1 text', 'path2text': 'B2 text', 'path3text': 'B3 text', 'path4text': 'Element text', 'path5text': 'Information Text',
			'point_name': 'Point name', 'point_text': 'Point text'
		}

		# Remove first "/" and unnecessary spaces
		point_name = df['point_name'].str.replace('^\/|\s{2,}', '', regex=True)
		point_text = df['point_text'].str.replace('^\/|\s{2,}', '', regex=True)
		df['point_name'] = point_name
		df['point_text'] = point_text
		df[['path1', 'path2', 'path3', 'path4', 'path5']] = point_name.str.split(pat='/', expand=True)
		df[['path1text', 'path2text', 'path3text', 'path4text', 'path5text']] = point_text.str.split(pat='/', expand=True)

		new_df = pd.DataFrame(columns=[val for key, val in columns_name.items()])

		for col in df.columns:
			new_col = columns_name[col]

			if new_col in new_df.columns:
				# Remove unnecessary spaces on begining and or trailing string object
				if pd.api.types.is_object_dtype(df[col]):
					new_df[new_col] = df[col].str.strip()
				else:
					new_df[new_col] = df[col]

		new_dtype = {col: 'str' for col in new_df.columns if col!='Point number'}

		new_df = new_df.astype(new_dtype).fillna('').replace({'None': '', 'nan': ''})
		new_df = new_df.sort_values(['B1', 'B2', 'B3', 'Element'], ascending=[True, True, True, True]).reset_index(drop=True)

		return new_df
	
	def prepare_his_digital(self, df:pd.DataFrame):
		"""
		Translate his_digital table column name.
		"""

		columns_name = {'value': 'Status', 'quality_code': 'Quality', 'quality_code_scada': 'SCADA value', 'point_text': 'Path text'}

		# Remove first "/" and unnecessary spaces
		point_text = df['point_text'].str.replace('^\/|\s{2,}', '', regex=True)
		df[['path1text', 'path2text', 'path3text', 'path4text', 'path5text']] = point_text.str.split(pat='/', expand=True)

		return self.prepare_his_message(df, **columns_name)

	def prepare_his_message(self, df:pd.DataFrame, **kwargs):
		"""
		Translate his_message table column name and filter specified column only
		"""

		columns_name = {
			'ack': 'A', 'time_stamp': 'Time stamp', 'msec': 'Milliseconds', 'system_time_stamp': 'System time stamp', 'system_msec': 'System milliseconds',
			'path1': 'B1', 'path2': 'B2', 'path3': 'B3', 'path4': 'Element', 'path5': 'Information',
			'msgstatus': 'Status', 'tag': 'Tag', 'msgoperator': 'Operator', 'priority': 'Priority', 'msgclass': 'Message class', 'comment_text': 'Comment',
			'path1text': 'B1 text', 'path2text': 'B2 text', 'path3text': 'B3 text', 'path4text': 'Element text', 'path5text': 'Information Text', 'value': 'Raw value', 'elem': 'Element text',
			'console': 'Console', 'message_text': 'Message'
		}
		columns_name.update(kwargs)
		col1 = ['B1', 'B2', 'B3']
		col2 = ['B1 text', 'B2 text', 'B3 text']

		new_df = pd.DataFrame(columns=SOE_COLUMNS)

		for col in df.columns:
			new_col = columns_name[col]

			if new_col in new_df.columns:
				# Remove unnecessary spaces on begining and or trailing string object
				if pd.api.types.is_object_dtype(df[col]):
					new_df[new_col] = df[col].str.strip()
				else:
					new_df[new_col] = df[col]

		new_df['Status'] = new_df['Status'].str.title()

		# Merge B1, B2, B3 translation with existing table
		new_df = new_df.merge(self.cpoint_description, on=col1, how='left')
		without_description = new_df['B1 text'].isna()

		if new_df[without_description].shape[0]>0:
			# List unknown (nan) Point Description
			no_description = new_df.loc[without_description, col1].drop_duplicates(keep='first').values
			print(f'{len(no_description)} poin tidak terdaftar dalam "Point Description".\n{"; ".join([str(x) for i, x in enumerate(no_description) if i<5])}{" ..." if len(no_description)>5 else ""}\nSilahkan update melalui SpectrumOfdbClient atau menambahkan manual pada file cpoint.xlsx!')
			# Fill unknown (nan) Point Description B1, B2, B3 with its own text
			new_df.loc[without_description, col2] = new_df.loc[without_description, col1].values

		new_dtype = {key: val for key, val in self.column_dtype.items() if key in new_df.columns}

		new_df = new_df.astype(new_dtype).fillna('').replace({'None': '', 'nan': ''})
		new_df = new_df.sort_values(['System time stamp', 'System milliseconds', 'Time stamp', 'Milliseconds'], ascending=[True, True, True, True]).reset_index(drop=True)

		return new_df

	def query_cpoint(self, **kwargs):
		"""
		"""

		querystring = self.get_cpoint_querystring(filters={**kwargs})
		df = self.read_query(querystring)

		return df

	def query_element(self, element: Union[str, list]):
		"""
		"""

		filters = self.filter_his.copy()
		filters['path4'] = element
		
		querystring = self.get_message_querystring(filters=filters)
		df = self.read_query(querystring)
		# Modify system_time_stamp from UTC to Local time
		if type(df)==pd.DataFrame: df['system_time_stamp'] = df['system_time_stamp'].map(lambda x: x + self.tzone)

		return df

	@calc_time
	def read_query(self, querystring:str):
		"""
		Create instance of database connection and execute query
		"""

		if self.check_server():
			# Server connection OK
			connection_string = f'DRIVER={self._conn_driver};SERVER={self._conn_host};PORT={self._conn_port};DATABASE={self._conn_database};UID={self._conn_user};PWD={self._conn_pswd};Trusted_Connection=No;'
			connection_url = sa.engine.URL.create('mssql+pyodbc', query={"odbc_connect": connection_string})
			engine = sa.create_engine(connection_url)

			with engine.begin() as conn:
				df = pd.read_sql_query(sa.text(querystring), conn)
				df = df.drop_duplicates(keep=self.keep_duplicate)
			status = 'OK'
		else:
			# Server connection NOK
			df = None
			status = 'NOK'

		return df

	def read_query_control_disable(self, force:bool=False, query:pd.DataFrame=None, **kwargs):
		"""
		"""

		if force or not hasattr(self, '_soe_control_disable'):
			print('Memuat data "Control Disable" dari server...', end=' ', flush=True)
			
			raw = self.query_element('CD')
			df = self.prepare_his_message(raw) if type(raw)==pd.DataFrame else None

			# Store raw SQL value and cleaned value
			self.raw_control_disable = raw
			self._soe_control_disable = df

		# Reset index
		if type(query)==pd.DataFrame:
			self._soe_control_disable = query[(query['Element']=='CD') & (query['Status'].isin(['Disable', 'Enable', 'Dist.']))].copy()

		return self._soe_control_disable

	def read_query_cpoint(self, force:bool=False, **kwargs):
		"""
		"""

		df = None
		filters = {'active': 'T', **kwargs}

		if force or not hasattr(self, '_cpoint_description'):
			if not kwargs.get('silent'): print('Memuat data "Point Name Description" dari server...', end=' ', flush=True)

			raw = self.query_cpoint(**filters)
			if type(raw)==pd.DataFrame:
				df = self.prepare_cpoint(raw)

			# Store raw SQL value and cleaned value
			# self.raw_cpoint_ifs = raw
			self._cpoint_description = df[['B1', 'B2', 'B3', 'B1 text', 'B2 text', 'B3 text']]

		return df

	def read_query_local_remote(self, force:bool=False, query:pd.DataFrame=None, **kwargs):
		"""
		"""

		if force or not hasattr(self, '_soe_local_remote'):
			print('Memuat data "Local/Remote" dari server...', end=' ', flush=True)
			
			raw = self.query_element('LR')
			df = self.prepare_his_message(raw) if type(raw)==pd.DataFrame else None

			# Store raw SQL value and cleaned value
			self.raw_local_remote = raw
			self._soe_local_remote = df

		# Reset index
		if type(query)==pd.DataFrame:
			self._soe_local_remote = query[(query['Element']=='LR') & (query['Status'].isin(['Local', 'Remote', 'Dist.']))].copy()

		return self._soe_local_remote

	def read_query_rtu_updown(self, force:bool=False, query:pd.DataFrame=None, **kwargs):
		"""
		"""

		if force or not hasattr(self, '_soe_rtu_updown'):
			print('Memuat data "RTU Up/Down" dari server...', end=' ', flush=True)

			querystring = self.get_digital_cpoint_querystring()
			raw = self.read_query(querystring)
			if type(raw)==pd.DataFrame:
				# Modify system_time_stamp from UTC to Local time
				raw['system_time_stamp'] = raw['system_time_stamp'].map(lambda x: x + self.tzone)
				# Translate from value 0-1 to Down-Up
				raw['value'] = raw['value'].map(lambda x: 'Up' if int(float(x))==1 else 'Down')

				df = self.prepare_his_digital(raw)
			else:
				df = None

			# Store raw SQL value and cleaned value
			self.raw_rtu_updown = raw
			self._soe_rtu_updown = df

		# Reset index
		if type(query)==pd.DataFrame:
			self._soe_rtu_updown = query[(query['B1']=='IFS') & (query['B2']=='RTU_P1') & (query['Status'].isin(['Up', 'Down']))].copy()

		return self._soe_rtu_updown

	def read_query_switching(self, force:bool=False, query:pd.DataFrame=None, **kwargs):
		"""
		"""

		if force or not hasattr(self, '_soe_switching'):
			print('Memuat data "Switching" dari server...', end=' ', flush=True)
			
			raw = self.query_element(self.switching_element)
			df = self.prepare_his_message(raw) if type(raw)==pd.DataFrame else None

			# Store raw SQL value and cleaned value
			self.raw_switching = raw
			self._soe_switching = df

		# Reset index
		if type(query)==pd.DataFrame:
			self._soe_switching = query[(query['Element'].isin(self.switching_element)) & (query['Status'].isin(['Open', 'Close', 'Dist.']))].copy()

		return self._soe_switching

	def read_query_synchro(self, force:bool=False, query:pd.DataFrame=None, **kwargs):
		"""
		"""

		if force or not hasattr(self, '_soe_synchro'):
			print('Memuat data "Synchro. Switch" dari server...', end=' ', flush=True)
			
			raw = self.query_element('CSO')
			df = self.prepare_his_message(raw) if type(raw)==pd.DataFrame else None

			# Store raw SQL value and cleaned value
			self.raw_synchro = raw
			self._soe_synchro = df

		# Reset index
		if type(query)==pd.DataFrame:
			self._soe_synchro = query[(query['Element']=='CSO') & (query['Status'].isin(['Off', 'On', 'Dist.']))].copy()

		return self._soe_synchro

	def read_query_trip(self, force:bool=False, query:pd.DataFrame=None, **kwargs):
		"""
		"""

		if force or not hasattr(self, '_soe_trip'):
			print('Memuat data "Protection Trip" dari server...', end=' ', flush=True)
			
			raw = self.query_element(['CBTR', 'MTO'])
			df = self.prepare_his_message(raw) if type(raw)==pd.DataFrame else None

			# Store raw SQL value and cleaned value
			self.raw_trip = raw
			self._soe_trip = df

		# Reset index
		if type(query)==pd.DataFrame:
			self._soe_trip = query[query['Element'].isin(['CBTR', 'MTO'])].copy()

		return self._soe_trip

	def save_config(self, prompt: bool = True):
		"""Save database configuration.

		Args:
			prompt : prompt before save
		"""
		allowed = ['host', 'port', 'user', 'pswd', 'database', 'driver']
		commit = False

		if prompt:
			if input('Simpan konfigurasi? [yes/no] ').lower() in ['yes', 'y']:
				commit = True
		else:
			commit = True

		if commit:
			for opt in allowed:
				self.setting.set('CONNECTION', opt.upper(), getattr(self, f'_conn_{opt}', ''))
			# Save config file
			with open(self._conf_path, 'w') as conf:
				self.setting.write(conf)

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
		allowed = ['host', 'port', 'user', 'pswd', 'database', 'driver']

		if conf:
			# Set connection parameter
			for opt, val in conf.items():
				if opt in allowed:
					setattr(self, f'_conn_{opt.lower()}', str(val))
				else:
					raise KeyError(f'Variabel {opt} tidak dikenal!')
		else:
			self._conn_host = input('Host\t\t: ')
			self._conn_port = input('Port\t\t: ')
			self._conn_user = input('User\t\t: ')
			self._conn_pswd = input('Password\t: ')
			self._conn_database = input('Database\t: ')
			print('')
			self._conn_driver = self.select_driver()
			self.save_config(prompt=False)

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
		self.date_isset = True


	@property
	def cpoint_description(self):
		return self._cpoint_description if hasattr(self, '_cpoint_description') else self.read_query_cpoint()

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
	def soe_control_disable(self):
		return self._soe_control_disable if hasattr(self, '_soe_control_disable') else self.read_query_control_disable()

	@property
	def soe_local_remote(self):
		return self._soe_local_remote if hasattr(self, '_soe_local_remote') else self.read_query_local_remote()

	@property
	def soe_rtu_updown(self):
		return self._soe_rtu_updown if hasattr(self, '_soe_rtu_updown') else self.read_query_rtu_updown()

	@property
	def soe_switching(self):
		return self._soe_switching if hasattr(self, '_soe_switching') else self.read_query_switching()

	@property
	def soe_synchro(self):
		return self._soe_synchro if hasattr(self, '_soe_synchro') else self.read_query_synchro()

	@property
	def soe_trip(self):
		return self._soe_trip if hasattr(self, '_soe_trip') else self.read_query_trip()
		

def main():
	pass

if __name__=='__main__':
	main()