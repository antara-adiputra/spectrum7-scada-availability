import datetime, os, re
from dataclasses import dataclass, field, replace
from functools import cached_property

import pandas as pd

from .base import Config, DataModel, DataTable, FieldMetadata, frozen_dataclass_set, model_dtypes, model_mappings, repr_dataclass
from .excel import *
from .filereader import FileReader
from . import params
from ..lib import get_datetime, join_datetime, logprint
from ..types import *
from .. import config


XLS_FORMAT_DATETIME_S = XlsxFormat(num_format='dd-mm-yyyy hh:mm:ss', align='center', text_wrap=True)

@dataclass
class SOEModel(DataModel):
	"""Basic standardized SOE model."""
	ack: str = field(
		default=None,
		metadata=FieldMetadata(
			header='A',
			required=True,
			column_format=XLS_FORMAT_TEXT_CENTER,
			width=3,
		)
	)
	timestamp: datetime.datetime = field(
		default=None,
		metadata=FieldMetadata(
			header='Time stamp',
			dtype='datetime64[s]',
			required=True,
			column_format=XLS_FORMAT_DATETIME_S,
			width=18,
		)
	)
	ms: int = field(
		default=None,
		metadata=FieldMetadata(
			header='Milliseconds',
			dtype='uint32',
			required=True,
			column_format=XLS_FORMAT_INTEGER,
			width=6,
		)
	)
	system_timestamp: datetime.datetime = field(
		default=None,
		metadata=FieldMetadata(
			header='System time stamp',
			dtype='datetime64[s]',
			column_format=XLS_FORMAT_DATETIME_S,
			width=18,
		)
	)
	system_ms: int = field(
		default=None,
		metadata=FieldMetadata(
			header='System milliseconds',
			dtype='uint32',
			column_format=XLS_FORMAT_INTEGER,
			width=6,
		)
	)
	b1: str = field(
		default=None,
		metadata=FieldMetadata(
			header='B1',
			required=True,
			column_format=XLS_FORMAT_TEXT,
		)
	)
	b2: str = field(
		default=None,
		metadata=FieldMetadata(
			header='B2',
			required=True,
			column_format=XLS_FORMAT_TEXT,
		)
	)
	b3: str = field(
		default=None,
		metadata=FieldMetadata(
			header='B3',
			required=True,
			column_format=XLS_FORMAT_TEXT,
		)
	)
	element: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Element',
			required=True,
			column_format=XLS_FORMAT_TEXT,
		)
	)
	status: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Status',
			required=True,
			column_format=XLS_FORMAT_TEXT,
		)
	)
	tag: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Tag',
			required=True,
			column_format=XLS_FORMAT_TEXT,
		)
	)
	operator: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Operator',
			required=True,
			column_format=XLS_FORMAT_TEXT,
		)
	)
	comment: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Comment',
			column_format=XLS_FORMAT_TEXT_WRAP,
			width=22,
		)
	)
	user_comment: str = field(
		default=None,
		metadata=FieldMetadata(
			header='User comment',
			column_format=XLS_FORMAT_TEXT_WRAP,
			width=12,
		)
	)

	@staticmethod
	def _normalize_status(status: str) -> str:
		if isinstance(status, str):
			return params.SOE_STATUS_DICT.get(status.lower(), status.title())
		else:
			return status

	@classmethod
	def validate_dataframe(cls, df: pd.DataFrame, debug: bool = False, **kwargs) -> pd.DataFrame:
		# if not cls.validate_schema(df):
		# 	raise ValueError(f'Dataframe with columns {tuple(df.columns)} does not match with {cls.__name__} model')

		column_short = ['b1', 'b2', 'b3']
		column_long = ['b1_text', 'b2_text', 'b3_text']
		# model_mapping = model_mappings(cls, swap=True, as_dict=True)
		# df.columns = [model_mapping.get(col, col) for col in df.columns]

		df0 = super().validate_dataframe(df, debug=debug, **kwargs)
		# Create new copy
		new_df = df0.infer_objects()\
			.fillna('')\
			.reset_index(drop=True)
		# It's IMPORTANT to fillna("") to prevent errors on the next operation
		df = df.fillna('')
		for col in df.columns:
			if pd.api.types.is_object_dtype(df[col]):
				# Remove unnecessary spaces on begining and or trailing string object
				df[col] = df[col].str.strip()

		# Filter new DataFrame & redefine column type
		new_df = df[df['ack']==''].astype(model_dtypes(cls))
		new_df = new_df.sort_values(['system_timestamp', 'system_ms', 'timestamp', 'ms'], ascending=[True, True, True, True])\
			.reset_index(drop=True)
		# Capitalize data on column Status
		new_df['status'] = new_df['status'].map(cls._normalize_status)
		# Rename column b1, b2, b3 into b1_text, b2_text, b3_text
		new_df.columns = [column_long[column_short.index(col)] if col in column_short else col for col in new_df.columns]
		if isinstance(config.POINT_LIBRARY, pd.DataFrame):
			# Change B1, B2, B3 from description style into mnemonic style
			df_point = config.POINT_LIBRARY.copy()
			# Double check duplicated keys
			df_point.drop_duplicates(subset=column_long, keep='first', inplace=True)
			# Merge B1, B2, B3 translation with existing table
			new_df = new_df.merge(df_point, on=column_long, how='left')
			without_description = new_df[(new_df['b1'].isna()) & (new_df['b2'].isna()) & (new_df['b3'].isna())]
			if without_description.shape[0]>0:
				# List unknown (nan) Point Description
				no_description = without_description[column_long].drop_duplicates(keep='first').values
				logprint(f'{len(no_description)} point(s) is not in point library.\n{"; ".join([str(x) for i, x in enumerate(no_description) if i<5])}{" ..." if len(no_description)>5 else ""}\nPlease kindly update on file cpoint_*.xlsx')
				# Fill unknown (nan) Point Description B1, B2, B3 with its own text
				new_df.loc[without_description.index, column_short] = new_df.loc[without_description.index, column_long].values

		if debug:
			return new_df
		else:
			to_model_columns = model_mappings(SOEModel, as_dict=True)
			return new_df[to_model_columns.keys()]


@dataclass
class SurvalentSOEModel(DataModel):
	"""Survalent alarm message model.

	#### Data column: ['Time', 'Point', 'Message', 'Operator']
	"""
	time: datetime.datetime = field(
		default=None,
		metadata=FieldMetadata(
			header='Time',
			required=True,
		)
	)
	point: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Point',
			required=True,
		)
	)
	message: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Message',
			required=True,
		)
	)
	operator: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Operator',
			required=True,
		)
	)

	def __post_init__(self):
		if isinstance(self.time, str):
			self.time = datetime.datetime.strptime(self.time, '%Y-%m-%d %H:%M:%S.%f')

	@classmethod
	def from_dataframe(cls, df: pd.DataFrame, convert_model: bool = True) -> DataTable[Union['SurvalentSOEModel', SOEModel]]:
		if convert_model:
			model_list = list(map(lambda ix: cls.from_series(df.loc[ix]).convert_model(), df.index))
			return cls._tag_possible_rc_feedback(model_list)
		else:
			return DataTable(list(map(lambda ix: cls.from_series(df.loc[ix]), df.index)))

	@classmethod
	def _tag_possible_rc_feedback(cls, values: List[SOEModel]) -> DataTable[SOEModel]:
		fback_tag = 'RC'
		fback_comment = 'Kemungkinan feedback RC'

		logprint(f'Scanning possible RC feedback...', level='info')
		df0 = DataTable(values).to_dataframe()
		order_indexes = df0[df0['tag'].isin(params.ORDER_TAG)].index
		for ix in order_indexes:
			t_order, _ = get_datetime(df0.loc[ix])
			b1, b2, b3, elm, sta = df0.loc[ix, ['b1', 'b2', 'b3', 'element', 'status']]
			df_after_10s = df0[
				(df0['b1']==b1) & (df0['b2']==b2) & (df0['b3']==b3) & (df0['element']==elm) & (df0['status']==sta) &
				(join_datetime(df0['system_timestamp'], df0['system_ms'])>t_order) &
				(join_datetime(df0['system_timestamp'], df0['system_ms'])<=join_datetime(t_order, 10000)) &
				(df0['tag']=='')
			]
			if df_after_10s.shape[0]>0:
				# Feedback is after 
				ifirst = df_after_10s.index[0]
				df0.loc[ifirst, ['tag', 'user_comment']] = [fback_tag, fback_comment]
			else:
				df_before_10s = df0[
					(df0['b1']==b1) & (df0['b2']==b2) & (df0['b3']==b3) & (df0['element']==elm) & (df0['status']==sta) &
					(join_datetime(df0['system_timestamp'], df0['system_ms'])<t_order) &
					(join_datetime(df0['system_timestamp'], df0['system_ms'])>join_datetime(t_order, -10000)) &
					(df0['tag']=='')
				]
				if df_before_10s.shape[0]>0:
					ilast = df_before_10s.index[-1]
					df0.loc[ilast, ['tag', 'user_comment']] = [fback_tag, fback_comment]

		return SOEModel.from_dataframe(df0)

	def _extract_time(self, data: Dict[str, Any]) -> Dict[str, Any]:
		if isinstance(self.time, datetime.datetime):
			ms = self.time.microsecond//1000
			time_stamp = self.time.replace(microsecond=0)
		else:
			ms = None
			time_stamp = None

		data.update({'timestamp': time_stamp, 'ms': ms, 'system_timestamp': time_stamp, 'system_ms': ms})
		return data

	def _extract_point(self, data: Dict[str, Any]) -> Dict[str, Any]:
		if isinstance(self.point, str):
			station_name, element = str(self.point).replace('SAS_', '').split(',')
		else:
			station_name = None
			element = None
		
		data.update({'station_name': station_name, 'element': element})
		return data

	def _extract_message(self, data: Dict[str, Any]) -> Dict[str, Any]:
		if self.message is None:
			return data

		if not ('station_name' in data and 'element' in data):
			data = self._extract_point(data)

		if isinstance(self.operator, str) and len(self.operator)>0:
			# Command event
			data = self._extract_message_control(data)
		else:
			# Status event
			data = self._extract_message_status(data)

		return data

	def _extract_message_control(self, data: Dict[str, Any]) -> Dict[str, Any]:
		message = self.message.replace(' 20 ', ' ').replace('*', '')
		station: str = data['station_name']
		element: str = data['element']
		user_comment = ''
		# Get start & end of event in remained text, then get prefix and suffix
		match_text = re.search(f'{station}.{element}', message)
		if match_text is None:
			# Skip
			return
		
		prefix = message[:match_text.start()].strip()
		suffix = message[match_text.end():].strip()
		b123_format: bool = False

		if len(re.findall('_', station))==2 and not station.startswith('BAY_'):
			# New alarm structure, <b1>_<b2>_<b3>
			b1, b2, b3 = station.split('_')
			b123_format = True
		else:
			# Arbitrary format
			b1 = ''
			b2 = '150'
			b3 = station
		
		from_text = re.search('from', suffix, re.IGNORECASE)
		if from_text:
			# Command format '*<Status> <Station>,<Element> FROM <UI Dispatcher>::<User>'
			# Command format '??? <Operator Action>? From <UI Dispatcher>::<User>'
			if from_text.start()==0:
				tag = 'OR'
			else:
				tag = ''
				user_comment = suffix[:from_text.start()]
		elif 'CONTROL ECHO FAILURE' in suffix:
			# Failed command format '*<Station>,<Element>***<Station ID> <B1> CONTROL ECHO FAILURE'
			tag = 'NE'
		else:
			tag = ''

		sts_open = re.search('open', message, re.IGNORECASE)
		sts_close = re.search('close', message, re.IGNORECASE)
		if sts_close is None:
			if sts_open is None:
				# Unknown status
				status = ''
			else:
				status = sts_open[0].title()
		else:
			status = sts_close[0].title()

		data.update({'b1': b1, 'b2': b2, 'b3': b3, 'tag': tag, 'status': status, 'user_comment': user_comment})
		return data

	def _extract_message_status(self, data: Dict[str, Any]) -> Dict[str, Any]:
		message = self.message.replace(' 20 ', ' ').replace('*', '')
		station: str = data['station_name']
		element: str = data['element']
		tag = ''
		# Get start & end of event in remained text, then get prefix and suffix
		match_text = re.search(f'{station}.{element}', message)
		if match_text is None:
			# Skip
			return

		prefix = message[:match_text.start()].strip()
		suffix = message[match_text.end():].strip()
		b123_format: bool = False

		if len(re.findall('_', station))==2 and not station.startswith('BAY_'):
			# New alarm structure, <b1>_<b2>_<b3>
			b1, b2, b3 = station.split('_')
			b123_format = True
		else:
			# Arbitrary format
			b1 = station.replace('SAS_', '')
			if element=='CD':
				b2 = ''
				b3 = ''
			else:
				b2 = '150'
				b3 = station

		splitted = suffix.split(' ')
		if splitted[0] and splitted[0].lower() in params.SOE_STATUS_DICT:
			status = params.SOE_STATUS_DICT[splitted[0].lower()]
		else:
			status = splitted[0].title()

		data.update({'b1': b1, 'b2': b2, 'b3': b3, 'tag': tag, 'status': status})
		return data

	def convert_model(self) -> SOEModel:
		model = SOEModel
		extracted_data = dict(
			ack='',
			# timestamp,
			# ms,
			# system_timestamp,
			# system_ms,
			# b1,
			# b2,
			# b3,
			# element,
			# status,
			# tag,
			operator=self.operator or '',
			comment='',
			user_comment='',
			original_message='/'.join([str(getattr(self, f)) for f in self.__dataclass_fields__.keys()])
		)
		extracted_data = self._extract_time(extracted_data)
		extracted_data = self._extract_point(extracted_data)
		extracted_data = self._extract_message(extracted_data)
		model_fields, _ = list(zip(*model_mappings(model)))
		return model(**{key: value for key, value in extracted_data.items() if key in model_fields})

	@staticmethod
	def _split_message(msg: str) -> Dict[str, str]:
		message_format = {
			'normal_status': {
				'pattern': re.compile(r'^\*?(\d{4,}) ([\w\-]+).(\w+)\s*([\w\(\)]*)'),
				'fields': ['station_id', 'msg_station', 'msg_element', 'status']
			},
			'control_order': {
				'pattern': re.compile(r'^\*?([\w\(\)]+)\s+([\w\-]+),(\w+) FROM ([\w\-]+)::\w+'),
				'fields': ['status', 'msg_station', 'msg_element', 'ws']
			},
			'control_negative_feedback': {
				'pattern': re.compile(r'^\*?([\w\-]+),(\w+)\*+(\d{4,})\s?([\w\(\)]+)?\s?(CONTROL ECHO FAILURE)'),
				'fields': ['msg_station', 'msg_element', 'station_id', 'status', 'note']
			},
			'user_activation': {
				'pattern': re.compile(r'^\*?"([\w\-]+),(\w+)"\s?(\d{4,})\s?([\w\W]+\??) from ([\w\-]+)::\w+ ([\w\W]+)'),
				'fields': ['msg_station', 'msg_element', 'station_id', 'action', 'ws', 'note']
			}
		}
		splitted = dict(status='', tag='')
		for name, fmt in message_format.items():
			pattern: re.Pattern = fmt['pattern']
			result = pattern.findall(msg)
			if result:
				splitted: Dict[str, str] = dict(zip(fmt['fields'], result[0]))
				# Assign extra information
				if name=='control_order':
					splitted['tag'] = 'OR'
				elif name=='control_negative_feedback':
					splitted['tag'] = 'NE'

				status = splitted.get('status')
				if status:
					splitted['status'] = params.SOE_STATUS_DICT.get(status.lower(), status.title())

				break

		return splitted

	@staticmethod
	def _scan_control_feedback(df: pd.DataFrame) -> pd.DataFrame:
		cmd_status = ''
		df_failed = df[df['tag']=='NE']
		timeseries = join_datetime(df['timestamp'], df['ms'])

		logprint(f'Scanning status of failed RC...', level='info')
		for ix in df_failed.index:
			s = df.loc[ix]
			t_fback = join_datetime(s['timestamp'], s['ms'])
			b1, b2, b3, elm, opr = df.loc[ix, ['b1', 'b2', 'b3', 'element', 'operator']]
			df_ord = df[
				(timeseries>=join_datetime(t_fback, -15000)) &
				(timeseries<t_fback) &
				(df['b1']==b1) &
				(df['b2']==b2) &
				(df['b3']==b3) &
				(df['element']==elm) &
				(df['tag']=='OR') &
				(df['operator']==opr)
			]
			if df_ord.shape[0]>0:
				# Get the last order
				cmd_status = df_ord.sort_values(['timestamp', 'ms']).iloc[-1]['status']

			df.loc[ix, 'status'] = cmd_status

		fback_tag = 'RC'
		fback_comment = 'Kemungkinan feedback RC'
		df_order = df[df['tag'].isin(params.ORDER_TAG)]

		logprint(f'Scanning possible RC feedback...', level='info')
		for ix in df_order.index:
			t_order, _ = get_datetime(df.loc[ix])
			b1, b2, b3, elm, sta = df.loc[ix, ['b1', 'b2', 'b3', 'element', 'status']]
			df_after_20s = df[
				(timeseries>t_order) &
				(timeseries<=join_datetime(t_order, 20000)) &
				(df['b1']==b1) &
				(df['b2']==b2) &
				(df['b3']==b3) &
				(df['element']==elm) &
				(df['status']==sta) &
				(df['tag'].isna() | (df['tag']==''))
			]
			if df_after_20s.shape[0]>0:
				# Feedback is after 
				ifirst = df_after_20s.sort_values(['timestamp', 'ms']).index[0]
				df.loc[ifirst, ['tag', 'user_comment']] = [fback_tag, fback_comment]
			else:
				df_before_20s = df[
					(timeseries<t_order) &
					(timeseries>=join_datetime(t_order, -20000)) &
					(df['b1']==b1) &
					(df['b2']==b2) &
					(df['b3']==b3) &
					(df['element']==elm) &
					(df['status']==sta) &
					(df['tag'].isna() | (df['tag']==''))
				]
				if df_before_20s.shape[0]>0:
					ilast = df_before_20s.sort_values(['timestamp', 'ms']).index[-1]
					df.loc[ilast, ['tag', 'user_comment']] = [fback_tag, fback_comment]

		return df

	@staticmethod
	def _join_action_note(df: pd.DataFrame) -> pd.DataFrame:
		def str_cat(s0: pd.Series, *s: pd.Series) -> pd.Series:
			list_s = list(s)
			try:
				s1 = list_s.pop(0)
				r0 = s0.str.cat(s1, sep='\n', na_rep='')
				res = str_cat(r0, *list_s) if list_s else r0
			except IndexError:
				res = s0
			finally:
				return res.str.strip()

		df['comment'] = str_cat(df['comment'], df['action'], df['note'])
		return df

	@classmethod
	def validate_dataframe(cls, df: pd.DataFrame, debug: bool = False, **kwargs) -> pd.DataFrame:
		"""
		Format:
			Normal status 		: '*<Station ID> <B1> <B3> <Element> <Status> <Ext. Information>'
			Normal command		: '*<Status> <B3>,<Element> FROM <UI Dispatcher>::<User>'	
			Negative feedback	: '*<B3>,<Element>***<Station ID> <B1> CONTROL ECHO FAILURE'
		"""
		# if not cls.validate_schema(df):
		# 	raise ValueError(f'Dataframe with columns {tuple(df.columns)} does not match with {cls.__name__} model')

		# model_mapping = model_mappings(cls, swap=True, as_dict=True)
		# df.columns = [model_mapping.get(col, col) for col in df.columns]

		df0 = super().validate_dataframe(df, debug=debug, **kwargs)
		# Create new copy
		new_df = df0.infer_objects()\
			.fillna('')\
			.reset_index(drop=True)

		# Extract columns
		s_time = new_df['time'].str.split('.', n=1, expand=True)		# [/timestamp, /ms]
		s_point = new_df['point'].str.rsplit(',', n=1, expand=True)		# [/station, /element]
		s_operator = new_df['operator'].str.strip()		# <operator>

		new_df[['timestamp', 'ms']] = s_time
		new_df[['system_timestamp', 'system_ms']] = s_time

		new_df['station'] = s_point[0].str.removeprefix('SAS_')
		new_df['element'] = s_point[1]

		split_station = new_df['station'].str.rsplit('_', n=2, expand=True).fillna('')
		if len(split_station.columns)==3:
			new_df[['b1', 'b2', 'b3']] = split_station
		else:
			new_df['b1'] = ''
			new_df['b2'] = ''
			new_df['b3'] = split_station

		# new_df['status'] = ''
		new_df['ack'] = ''
		# new_df['tag'] = ''
		new_df['operator'] = s_operator
		new_df['comment'] = ''
		new_df['user_comment'] = ''

		# Extract message
		df_split = new_df['message']\
			.map(cls._split_message)\
			.apply(pd.Series)

		to_model_dtypes = model_dtypes(SOEModel)
		# Concat new columns
		df_result = pd.concat([new_df, df_split], axis=1).astype(to_model_dtypes)
		df_result = cls._scan_control_feedback(df_result)
		df_result = cls._join_action_note(df_result)
		if debug:
			return df_result
		else:
			to_model_columns = model_mappings(SOEModel, as_dict=True)
			return df_result[to_model_columns.keys()]


@dataclass
class SurvalentSPModel(DataModel):
	"""Survalent status point model.

	#### Data columns: ['Time', 'Type', 'User Type', 'Point', 'RTU', 'Value', 'Source']
	"""
	time: datetime.datetime = field(
		default=None,
		metadata=FieldMetadata(
			header='Time',
			required=True,
		)
	)
	type: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Type',
			required=True,
		)
	)
	user_type: str = field(
		default=None,
		metadata=FieldMetadata(
			header='User Type',
		)
	)
	point: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Point',
			required=True,
		)
	)
	rtu: str = field(
		default=None,
		metadata=FieldMetadata(
			header='RTU',
			required=True,
		)
	)
	value: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Value',
			required=True,
		)
	)
	source: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Source',
		)
	)

	def __post_init__(self):
		if isinstance(self.time, str):
			self.time = datetime.datetime.strptime(self.time, '%Y-%m-%d %H:%M:%S.%f')

	@classmethod
	def from_dataframe(cls, df: pd.DataFrame, convert_model: bool = True) -> DataTable:
		if convert_model:
			return DataTable(list(map(lambda ix: cls.from_series(df.loc[ix]).convert_model(), df.index)))
		else:
			return DataTable(list(map(lambda ix: cls.from_series(df.loc[ix]), df.index)))

	def _extract_time(self, data: Dict[str, Any]) -> Dict[str, Any]:
		if isinstance(self.time, datetime.datetime):
			ms = self.time.microsecond//1000
			time_stamp = self.time.replace(microsecond=0)
		else:
			ms = None
			time_stamp = None

		data.update({'timestamp': time_stamp, 'ms': ms, 'system_timestamp': time_stamp, 'system_ms': ms})
		return data

	def _extract_source(self, data: Dict[str, Any]) -> Dict[str, Any]:
		protocols = ['IEC104', 'IEC101', 'DNP3']
		data['protocol'] = None
		data['station_id'] = None
		if self.source is None:
			# Skip
			pass
		else:
			try:
				for proto in protocols:
					if proto in self.source:
						data['protocol'] = proto
						data['station_id'] = self.source.replace(proto, '')
						break
			except TypeError:
				pass

		return data

	def _extract_point(self, data: Dict[str, Any]) -> Dict[str, Any]:
		if isinstance(self.point, str):
			station_name, element = str(self.point).replace('SAS_', '').split(',')
		else:
			station_name = None
			element = None
		
		data.update({'station_name': station_name, 'element': element})
		return data

	def _extract_point_station(self, data: Dict[str, Any]) -> Dict[str, Any]:
		if not ('station_name' in data and 'element' in data):
			data = self._extract_point(data)

		station: str = data['station_name']
		element: str = data['element']
		if station is None:
			return data

		if element=='RTU-STAT':
			data['b1'] = 'IFS'
			data['b2'] = 'COMMLINE'
			data['b3'] = station
		else:
			station_split = station.split('_', 2)
			if len(station_split)<3:
				# Force to create list of 3 items
				station_split += ['']*(3-len(station_split))

			data['b1'] = station_split[0]
			data['b2'] = station_split[1]
			data['b3'] = station_split[2]

		return data

	def _extract_value(self, data: Dict[str, Any]) -> Dict[str, Any]:
		# Sometimes status values followed with integer state value, ex: Up(1), Down(0), Alarm(1)
		if isinstance(self.value, str):
			data['status'] = re.sub(r'\s?\(\d+\)', '', self.value.title())
		else:
			data['status'] = None

		return data

	def convert_model(self) -> SOEModel:
		model = SOEModel
		extracted_data = dict(
			ack='',
			# timestamp,
			# ms,
			# system_timestamp,
			# system_ms,
			# b1,
			# b2,
			# b3,
			# element,
			# status,
			tag='',
			operator='',
			comment='',
			user_comment='',
			original_message='/'.join([str(getattr(self, f)) for f in self.__dataclass_fields__.keys()])
		)
		extracted_data = self._extract_time(extracted_data)
		extracted_data = self._extract_source(extracted_data)
		extracted_data = self._extract_point(extracted_data)
		extracted_data = self._extract_point_station(extracted_data)
		extracted_data = self._extract_value(extracted_data)
		model_fields, _ = list(zip(*model_mappings(model)))
		return model(**{key: value for key, value in extracted_data.items() if key in model_fields})

	@classmethod
	def validate_dataframe(cls, df: pd.DataFrame, debug: bool = False, **kwargs) -> pd.DataFrame:
		# if not cls.validate_schema(df):
		# 	raise ValueError(f'Dataframe with columns {tuple(df.columns)} does not match with {cls.__name__} model')

		# model_mapping = model_mappings(cls, swap=True, as_dict=True)
		# df.columns = [model_mapping.get(col, col) for col in df.columns]

		df0 = super().validate_dataframe(df, debug=debug, **kwargs)
		# Create new copy
		new_df = df0.infer_objects()\
			.fillna('')\
			.reset_index(drop=True)

		# Extract columns
		s_time = new_df['time'].str.split('.', n=1, expand=True)		# [/timestamp, /ms]
		s_point = new_df['point'].str.rsplit(',', n=1, expand=True)		# [/station, /element]
		s_value = new_df['value'].str.replace(r'\s*\(\d+\)$', '', regex=True)		# <status(\d)> ==> <status>
		s_source = new_df['source'].str.split(r'(IEC101|IEC104|DNP3)(\d+)', expand=True)		# [_, /protocol, /station_id, _]

		new_df[['timestamp', 'ms']] = s_time
		new_df[['system_timestamp', 'system_ms']] = s_time
		new_df[['protocol', 'station_id']] = s_source[[1, 2]]

		new_df['station'] = s_point[0].str.removeprefix('SAS_')
		new_df['element'] = s_point[1]

		split_station = new_df['station'].str.rsplit('_', n=2, expand=True).fillna('')
		if len(split_station.columns)==3:
			new_df[['b1', 'b2', 'b3']] = split_station
		else:
			new_df['b1'] = ''
			new_df['b2'] = ''
			new_df['b3'] = split_station[split_station.columns[0]]

		new_df['status'] = s_value.str.title()
		new_df['ack'] = ''
		new_df['tag'] = ''
		new_df['operator'] = ''
		new_df['comment'] = ''
		new_df['user_comment'] = ''

		# Complete assignment for RTU-STAT
		rtu_statuses = ('RTU-STAT',)
		df_rtu = new_df[new_df['element'].isin(rtu_statuses)]
		if df_rtu.shape[0]>0:
			indexes = df_rtu.index
			new_df.loc[indexes, 'b1'] = 'IFS'
			new_df.loc[indexes, 'b2'] = 'COMMLINE'

		to_model_dtypes = model_dtypes(SOEModel)
		new_df = new_df.astype(to_model_dtypes)
		if debug:
			return new_df
		else:
			to_model_columns = model_mappings(SOEModel, as_dict=True)
			return new_df[to_model_columns.keys()]


@dataclass(frozen=True)
class SOEData:
	"""Imutable classified/grouped SOE dataclass."""
	data: Optional[pd.DataFrame] = None
	# RC lookup element
	rc_element: Optional[Tuple[str]] = field(default=None)
	# Source of data
	sources: str = field(default=None)
	# All non-acknowledgement historical data
	his: pd.DataFrame = field(default=None)
	# Control Disable only
	CD: pd.DataFrame = field(default=None)
	# Synchrocheck only
	CSO: pd.DataFrame = field(default=None)
	# Switching element only
	CSW: pd.DataFrame = field(default=None)
	# IFS Up/Down only
	RTU: pd.DataFrame = field(default=None)
	# Local/Remote only
	LR: pd.DataFrame = field(default=None)
	# Protection (CBTR, MTO) only
	PROT: pd.DataFrame = field(default=None)

	@cached_property
	def date_max(self) -> datetime.datetime:
		tsmax = self.his['timestamp'].max()
		return tsmax.to_pydatetime() if isinstance(tsmax, pd.Timestamp) else tsmax
	
	@cached_property
	def date_min(self) -> datetime.datetime:
		tsmin = self.his['timestamp'].min()
		return tsmin.to_pydatetime() if isinstance(tsmin, pd.Timestamp) else tsmin

	def __repr__(self):
		return repr_dataclass(self)

	def __post_init__(self):
		if self.data is None: return

		df = self.data[self.data['ack']==''].copy()
		df = df.sort_values(
			['system_timestamp', 'system_ms', 'timestamp', 'ms'],
			ascending=[True, True, True, True]
		).reset_index(drop=True)
		# Filter His. Messages only if related to RC Event's B1, B2 and B3
		# df_bay = self._filter_for_rc(df)
		df_bay = df
		# Because of frozen dataclass, set attr via object
		frozen_dataclass_set(
			self,
			his=df,
			CSW=self._get_switching_data(df_bay),
			CD=self._get_control_disable_data(df_bay),
			CSO=self._get_synchro_switch_data(df_bay),
			LR=self._get_local_remote_data(df_bay),
			PROT=self._get_protection_data(df_bay),
			RTU=self._get_rtu_updown_data(df),
		)

	def _filter_for_rc(self, df: pd.DataFrame):
		# Filter His. Messages with order tag only
		orders = df[
			(df['element'].isin(self.rc_element)) &
			(df['tag'].isin(params.ORDER_TAG))
		]
		b1x = orders['b1'].unique()
		b2x = orders['b2'].unique()
		b3x = orders['b3'].unique()
		# Filter His. Messages only if related to RC Event's B1, B2 and B3
		df_bay = df[
			(df['b1'].isin(b1x)) &
			(df['b2'].isin(b2x)) &
			(df['b3'].isin(b3x))
		]
		return df_bay

	def _get_switching_data(self, df: pd.DataFrame) -> pd.DataFrame:
		if isinstance(self.rc_element, (tuple, list, set)):
			element = self.rc_element
		else:
			element = ('CB', 'BI1', 'BI2', 'LI', 'ES')

		soe_csw = df[
			(df['element'].isin(element)) &
			(df['status'].isin(params.CSW_STATUS))
		].copy()
		# Reset comment column and search for order tag for RC element
		soe_csw['comment'] = ''
		return soe_csw

	def _get_control_disable_data(self, df: pd.DataFrame) -> pd.DataFrame:
		return df[
			(df['element']=='CD') &
			(df['status'].isin(params.CD_STATUS))
		].copy()

	def _get_synchro_switch_data(self, df: pd.DataFrame) -> pd.DataFrame:
		return df[
			(df['element']=='CSO') &
			(df['status'].isin(params.CSO_STATUS))
		].copy()

	def _get_local_remote_data(self, df: pd.DataFrame) -> pd.DataFrame:
		return df[
			(df['element']=='LR') &
			(df['status'].isin(params.LR_STATUS))
		].copy()

	def _get_protection_data(self, df: pd.DataFrame) -> pd.DataFrame:
		protections = ['CBTR', 'MTO']
		return df[
			(df['element'].isin(protections)) &
			(df['status'].isin(params.ALARM_STATUS))
		].copy()

	def _get_rtu_updown_data(self, df: pd.DataFrame, rtus: Iterable[str] = list()) -> pd.DataFrame:
		# Filter IFS messages only if related to RC Event
		# NOTE: From 04-08-2025 we no longer filter IFS based on RC bays (b1x), to provide more flexibility for RS availability calculation
		RTU_DOWN_ELEMENT = (
			'Operational State',
			'RTU-STAT',
		)
		soe_rtu = df[
			(df['b1']=='IFS') &
			(df['element'].isin(RTU_DOWN_ELEMENT)) &
			(df['status'].isin(params.RTU_STATUS)) &
			(df['tag']=='')
		].copy()
		if len(rtus)>0:
			soe_rtu = soe_rtu[soe_rtu['b3'].isin(rtus)]

		return soe_rtu

	def compress(self, inplace: bool = False) -> Optional[Self]:
		new_data = self._filter_for_rc(self.his)
		uniqueb1 = new_data['b1'].unique()
		if inplace:
			frozen_dataclass_set(
				self,
				his=new_data,
				CSW=self._get_switching_data(new_data),
				CD=self._get_control_disable_data(new_data),
				CSO=self._get_synchro_switch_data(new_data),
				LR=self._get_local_remote_data(new_data),
				PROT=self._get_protection_data(new_data),
				RTU=self._get_rtu_updown_data(self.his, rtus=uniqueb1),
			)
		else:
			new_obj = SOEData(data=new_data, rc_element=self.rc_element)
			frozen_dataclass_set(new_obj, RTU=self._get_rtu_updown_data(self.his, rtus=uniqueb1))
			return new_obj

	def get_cleaned_his(self) -> pd.DataFrame:
		"""Get data collection from filtered CD, CSO, switching elements, IFS, and protection data."""
		return pd.concat(
			[df.dropna(axis=1, how='all') for df in (self.CD, self.CSO, self.CSW, self.RTU, self.LR, self.PROT)],
			copy=False
		).drop_duplicates(keep='first')\
		.sort_values(['timestamp', 'ms'])

	def _copydf(self, value: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
		if isinstance(value, pd.DataFrame):
			return value.copy(deep=True)
		else:
			return None

	def copy(self) -> Self:
		return replace(
			self,
			data=self._copydf(self.data),
			rc_element=self.rc_element,
			sources=self.sources,
			his=self._copydf(self.his),
			CD=self._copydf(self.CD),
			CSO=self._copydf(self.CSO),
			CSW=self._copydf(self.CSW),
			RTU=self._copydf(self.RTU),
			LR=self._copydf(self.LR),
			PROT=self._copydf(self.PROT),
		)


class SOE:
	"""Read SOE file from multiple master (Spectrum, Survalent)."""

	def __init__(self, config: Optional[Config] = None):
		self.config = config

	def read_file(self, files: FileInput, sheet: Optional[str] = None, **kwargs):
		"""Read SOE files. If config defined, can automatically choose filereader model handler.
		Default is Spectrum SOE model.
		"""
		if self.config is None or self.config.master=='spectrum':
			reader = FileReader(SOEModel, files=files, **kwargs)
			soe = reader.load(sheet_name=sheet, **kwargs)
			return self.post_read(soe, sources=reader.sources)
		else:
			return self.read_survalent(files=files, sheet=sheet)

	async def async_read_file(self, files: FileInput, sheet: Optional[str] = None, **kwargs):
		"""Read SOE files asynchronously. If config defined, can automatically choose filereader model handler.
		Default is Spectrum SOE model.
		"""
		if self.config is None or self.config.master=='spectrum':
			reader = FileReader(SOEModel, files=files, **kwargs)
			soe = await reader.async_load(sheet_name=sheet, **kwargs)
			return self.post_read(soe, sources=reader.sources)
		else:
			return await self.async_read_survalent(files=files, sheet=sheet)

	def post_read(self, df: pd.DataFrame, sources: str) -> SOEData:
		"""Classify/group SOE data."""
		return SOEData(data=df, sources=sources) if self.config is None else SOEData(data=df, rc_element=getattr(self.config, 'elements', None), sources=sources)

	def read_survalent(self, files: FileInput, sheet: Optional[str] = None, **kwargs):
		"""Read Survalent SOE / Status point files."""
		reader = FileReader(SurvalentSOEModel, SurvalentSPModel, files=files, **kwargs)
		soe = reader.load(sheet_name=sheet, **kwargs)
		return self.post_read(soe, sources=reader.sources)

	async def async_read_survalent(self, files: FileInput, sheet: Optional[str] = None, **kwargs):
		"""Asynchronously read Survalent SOE / Status point files."""
		reader = FileReader(SurvalentSOEModel, SurvalentSPModel, files=files, **kwargs)
		soe = await reader.async_load(sheet_name=sheet, **kwargs)
		return self.post_read(soe)

