import datetime, os, re, time
from dataclasses import dataclass, field
from queue import Queue

import pandas as pd

from .base import Base, Config, DataModel, FieldMetadata, frozen_dataclass_set, model_fieldnames, repr_dataclass
from .excel import *
from .filewriter import SheetInfo, SheetSOE, SheetWrapper, xl_hyperlink_to_range
from .main import Availability, AvailabilityCore, AvailabilityData, AvailabilityResult, QueueMessage
from .soe import SOEData
from ..lib import join_datetime, logprint, toggle_attr, try_remove
from ..test import *
from ..types import *
from .. import config, settings


xlf = XlsxFormula()


@dataclass
class RTUDownTime(Base):
	"""RTU up/down event dataclass representation of row data on excel."""
	down_time: datetime.datetime = field(
		default=None,
		metadata=FieldMetadata(
			header='Down Time',
			dtype='datetime64[ns]',
			required=True,
			column_format=XLS_FORMAT_DATETIME,
			width=23,
		)
	)
	up_time: datetime.datetime = field(
		default=None,
		metadata=FieldMetadata(
			header='Up Time',
			dtype='datetime64[ns]',
			required=True,
			column_format=XLS_FORMAT_DATETIME,
			width=23,
		)
	)
	rtu: str = field(
		default=None,
		metadata=FieldMetadata(
			header='RTU',
			required=True,
			column_format=XLS_FORMAT_TEXT,
		)
	)
	long_name: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Long Name',
			column_format=XLS_FORMAT_TEXT,
			freeze=True,
		)
	)
	duration: datetime.timedelta = field(
		default=None,
		metadata=FieldMetadata(
			header='Duration',
			dtype='timedelta64[ns]',
			required=True,
			column_format=XLS_FORMAT_TIMEDELTA,
			width=13,
		)
	)
	annotations: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Annotations',
			column_format=XLS_FORMAT_TEXT_WRAP,
			width=28,
		)
	)


@dataclass
class MarkableDownTime(RTUDownTime):
	"""Row data structure used in RTU Up/Down event table, include formatting & sizing in Excel."""
	ack_downtime: datetime.datetime = field(
		default=None,
		metadata=FieldMetadata(
			header='Acknowledged Down Time',
			dtype='datetime64[ns]',
			column_format=XLS_FORMAT_DATETIME,
			width=23,
		)
	)
	fix_duration: datetime.timedelta = field(
		default_factory=datetime.timedelta,
		metadata=FieldMetadata(
			header='Fix Duration',
			dtype='timedelta64[ns]',
			column_format=XLS_FORMAT_TIMEDELTA,
			width=13,
		)
	)
	marked_maintenance: bool = field(
		default=False,
		metadata=FieldMetadata(
			header='Marked Maintenance',
			required=True,
			column_format=XLS_FORMAT_TEXT_CENTER,
			width=13,
		)
	)
	marked_link_failure: bool = field(
		default=False,
		metadata=FieldMetadata(
			header='Marked Link Failure',
			required=True,
			column_format=XLS_FORMAT_TEXT_CENTER,
			width=13,
		)
	)
	marked_rtu_failure: bool = field(
		default=False,
		metadata=FieldMetadata(
			header='Marked RTU Failure',
			required=True,
			column_format=XLS_FORMAT_TEXT_CENTER,
			width=13,
		)
	)
	marked_other_failure: bool = field(
		default=False,
		metadata=FieldMetadata(
			header='Marked Other Failure',
			required=True,
			column_format=XLS_FORMAT_TEXT_CENTER,
			width=13,
		)
	)
	navigation: Tuple[int, int] = field(
		default_factory=tuple,
		metadata=FieldMetadata(
			header='Navigation',
			column_format=XlsxFormat(bold=True, font_color='blue', align='center', valign='vcenter', border=1, bg_color='#dcdcdc'),
			width=11,
		)
	)

	def __post_init__(self):
		for f in ('marked_maintenance', 'marked_link_failure', 'marked_rtu_failure', 'marked_other_failure'):
			if isinstance(getattr(self, f), bool):
				pass
			else:
				if getattr(self, f)=='*':
					setattr(self, f, True)
				else:
					setattr(self, f, False)


@dataclass
class RTUAvailability(Base):
	"""Row data structure used in RTU Availability table, include formatting & sizing in Excel."""
	rtu: str = field(
		default=None,
		metadata=FieldMetadata(
			header='RTU',
			column_format=XLS_FORMAT_TEXT,
		)
	)
	long_name: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Long Name',
			column_format=XLS_FORMAT_TEXT,
			freeze=True,
		)
	)
	downtime_occurences: int = field(
		default=None,
		metadata=FieldMetadata(
			header='Downtime Occurences',
			column_format=XLS_FORMAT_INTEGER,
			width=14,
		)
	)
	total_downtime: datetime.timedelta = field(
		default=None,
		metadata=FieldMetadata(
			header='Total Downtime',
			column_format=XLS_FORMAT_TIMEDELTA,
			width=12,
		)
	)
	average_downtime: datetime.timedelta = field(
		default=None,
		metadata=FieldMetadata(
			header='Average Downtime',
			column_format=XLS_FORMAT_TIMEDELTA,
			width=12,
		)
	)
	longest_downtime: datetime.timedelta = field(
		default=None,
		metadata=FieldMetadata(
			header='Longest Downtime',
			column_format=XLS_FORMAT_TIMEDELTA,
			width=12,
		)
	)
	longest_downtime_date: datetime.datetime = field(
		default=None,
		metadata=FieldMetadata(
			header='Longest Downtime Date',
			column_format=XLS_FORMAT_DATETIME,
			width=23,
		)
	)
	time_range: datetime.timedelta = field(
		default=None,
		metadata=FieldMetadata(
			header='Time Range',
			column_format=XLS_FORMAT_TIMEDELTA,
			width=12,
		)
	)
	uptime: datetime.timedelta = field(
		default=None,
		metadata=FieldMetadata(
			header='Uptime',
			column_format=XLS_FORMAT_TIMEDELTA,
			width=12,
		)
	)
	rtu_downtime: datetime.timedelta = field(
		default=None,
		metadata=FieldMetadata(
			header='RTU Downtime',
			column_format=XLS_FORMAT_TIMEDELTA,
			width=12,
		)
	)
	link_downtime: datetime.timedelta = field(
		default=None,
		metadata=FieldMetadata(
			header='Link Downtime',
			column_format=XLS_FORMAT_TIMEDELTA,
			width=12,
		)
	)
	other_downtime: datetime.timedelta = field(
		default=None,
		metadata=FieldMetadata(
			header='Other Downtime',
			column_format=XLS_FORMAT_TIMEDELTA,
			width=12,
		)
	)
	unclassified_downtime: datetime.timedelta = field(
		default=None,
		metadata=FieldMetadata(
			header='Unclassified Downtime',
			column_format=XLS_FORMAT_TIMEDELTA,
			width=13,
		)
	)
	quality: int = field(
		default=None,
		metadata=FieldMetadata(
			header='Quality',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	rtu_availability: float = field(
		default=None,
		metadata=FieldMetadata(
			header='RTU Availability',
			column_format=XLS_FORMAT_PERCENTAGE,
			width=13,
		)
	)
	link_availability: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Link Availability',
			column_format=XLS_FORMAT_PERCENTAGE,
			width=13,
		)
	)
	overall: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Overall',
			column_format=XLS_FORMAT_PERCENTAGE,
			width=13,
		)
	)


RTUDownTimeModel = type('RTUDownTimeModel', (MarkableDownTime, DataModel), {})
RTUAvailabilityModel = type('RTUAvailabilityModel', (RTUAvailability, DataModel), {})


@dataclass
class _DowntimeCategory:
	name: str
	threshold: datetime.timedelta


class DowntimeRules:
	"""Downtime rules list.
	
	Args:
		value : list of rules
	"""

	def __init__(self, value: Optional[List[Union[_DowntimeCategory, Tuple, List]]] = None):
		if value is None:
			value = config.DOWNTIME_RULES
			logprint('Using default downtime rules.', level='info')

		if all(map(lambda r: isinstance(r, _DowntimeCategory), value)):
			self._value = value
		elif all(map(lambda r: isinstance(r, (tuple, list)), value)):
			self._value = list(map(lambda cat: _DowntimeCategory(name=cat[0], threshold=datetime.timedelta(hours=cat[1])), value))
		else:
			self._value = list()

		self.sort(desc=True)

	def sort(self, desc: bool = False):
		self._value = sorted(self._value, key=lambda rule: rule.threshold, reverse=desc)

	def categorize(self, value: Union[datetime.timedelta, int, float]) -> Optional[_DowntimeCategory]:
		"""Get up/down category from rules."""
		result = None
		if isinstance(value, datetime.timedelta):
			val = value
		elif isinstance(value, (int, float)):
			val = datetime.timedelta(hours=value)
		else:
			return result

		for rule in self._value:
			if val>=rule.threshold:
				result = rule
				break

		return result


def default_downtime_rules():
	return DowntimeRules(config.DOWNTIME_RULES)


@dataclass
class RTUConfig(Config):
	"""RTU availability calculation configuration.

	Args:
		rules : categorization rules based on downtime duration
		rtu_names : 
		known_rtus_only : whether only list known RTUs included in Availability
		maintenance_mark : string used to mark event as maintenance
		link_failure_mark : string used to mark event as link failure
		rtu_failure_mark : string used to mark event as RTU failure
		other_failure_mark : string used to mark event as other failure
	"""
	rules: DowntimeRules = field(default_factory=default_downtime_rules)
	rtu_file_name: str = ''
	# Whether only list known RTUs included in Availability
	known_rtus_only: bool = False
	maintenance_mark: str = '**maintenance**'
	link_failure_mark: str = '**link**'
	rtu_failure_mark: str = '**rtu**'
	other_failure_mark: str = '**other**'

	@property
	def rtu_names(self) -> Dict[str, str]:
		rtus = config.RTU_NAMES_CONFIG.get(self.rtu_file_name, dict())
		if not rtus and self.known_rtus_only:
			self.known_rtus_only = False

		return rtus


@dataclass(frozen=True)
class RTUDownData(AvailabilityData):
	"""Imutable classified/grouped RTU dataclass.

	Args:
		all : SOE event data
		config : calculation configuration
		start_date : declared start date
		end_date : declared end date

	Attributes:
		rtu_names : RTU name table
		valid : all valid data that used for statistics calculation, parameterized in configuration
		availability : availability data summary of each RTU
	"""
	config: RTUConfig = field(kw_only=True)
	valid: pd.DataFrame = field(init=False, default=None)
	availability: pd.DataFrame = field(init=False, default=None)

	def __repr__(self):
		return repr_dataclass(self)

	def __post_init__(self):
		super().__post_init__()
		if not isinstance(self.all, pd.DataFrame):
			logprint(f'Invalid data type of {type(self.all)}', level='error')
			return

		df = self.all.copy()\
			.sort_values(['down_time'], ascending=[True])\
			.reset_index(drop=True)
		# Set default ack_downtime value into its down time
		unack_downtime = df['ack_downtime'].isna()
		df.loc[unack_downtime, 'ack_downtime'] = df.loc[unack_downtime, 'down_time'].values
		df_valid = df.copy()
		availability = self.group(df_valid, columns=['rtu'])	# 03-09-2025, dataframe merge & groupby now only indexed from RTU column
		frozen_dataclass_set(
			self,
			all=df,
			valid=df_valid,
			availability=availability,
		)

	def group(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
		"""Base function to get aggregation values based on defined "columns".

		Args:
			df : data input
			columns : list of columns as reference

		Result:
			Grouped data
		"""
		groupby_columns = columns + ['duration']
		# Columns order
		output_columns = [
			'rtu',
			'long_name',
			'downtime_occurences',
			'total_downtime',
			'average_downtime',
			'longest_downtime',
			'longest_downtime_date',
			'time_range',
			'uptime',
			'rtu_downtime',
			'link_downtime',
			'other_downtime',
			'unclassified_downtime',
			'quality',
			'rtu_availability',
			'link_availability',
			'overall'
		]

		merge_how = 'left' if self.config.known_rtus_only else 'outer'
		df_rtu = pd.DataFrame(data=self.config.rtu_names.items(), columns=['rtu', 'long_name'])
		df_pre = df.copy()

		# Declared filters
		filter_max_downtime = df_pre.groupby(columns, as_index=False)['duration']\
			.transform('max')==df_pre['duration']
		filter_rtu_down = (df['marked_rtu_failure']=='*')
		filter_link_down = (df['marked_link_failure']=='*')
		filter_other_down = (df['marked_other_failure']=='*')
		filter_unc_down =  (df['marked_maintenance']=='*') & (df['marked_rtu_failure']=='*') & (df['marked_link_failure']=='*') & (df['marked_other_failure']=='*')

		# Groupby
		down_count = df_pre[groupby_columns]\
			.groupby(columns, as_index=False).\
			count().\
			rename(columns={'duration': 'downtime_occurences'})
		down_agg = df_pre[groupby_columns]\
			.groupby(columns)\
			.agg(['sum', 'mean', 'max'])\
			.reset_index()
		down_agg.columns = ['rtu', 'total_downtime', 'average_downtime', 'longest_downtime']
		down_max_t = df_pre.loc[filter_max_downtime, columns + ['down_time']].rename(columns={'down_time': 'longest_downtime_date'})
		down_rtu = df.loc[filter_rtu_down, groupby_columns]\
			.groupby(columns, as_index=False)\
			.sum()\
			.rename(columns={'duration': 'rtu_downtime'})
		down_link = df.loc[filter_link_down, groupby_columns]\
			.groupby(columns, as_index=False)\
			.sum()\
			.rename(columns={'duration': 'link_downtime'})
		down_other = df.loc[filter_other_down, groupby_columns]\
			.groupby(columns, as_index=False)\
			.sum()\
			.rename(columns={'duration': 'other_downtime'})
		down_uncls = df.loc[filter_unc_down, groupby_columns]\
			.groupby(columns, as_index=False)\
			.sum()\
			.rename(columns={'duration': 'unclassified_downtime'})

		# Merge with RTU names list
		df_groupby = df_rtu.merge(right=down_count, how=merge_how, on=columns)
		df_groupby['long_name'] = df_groupby['long_name'].fillna('')
		df_groupby['downtime_occurences'] = df_groupby['downtime_occurences'].fillna(0)
		# Merge existing table with aggregated table and fill NaT with timedelta(0 second)
		for dfgroup in [down_agg, down_uncls, down_rtu, down_link, down_other]:
			df_groupby = df_groupby.merge(right=dfgroup, how='left', on=columns).fillna(datetime.timedelta(seconds=0))

		df_groupby = df_groupby.merge(right=down_max_t, how='left', on=columns).fillna(self.end_date)
		df_groupby['time_range'] = self.end_date - self.start_date + datetime.timedelta(milliseconds=1)
		df_groupby['uptime'] = df_groupby['time_range'] - df_groupby['total_downtime']
		df_groupby['quality'] = 1
		df_groupby['rtu_availability'] = round(1 - (df_groupby['rtu_downtime'] / df_groupby['time_range']), 4)
		df_groupby['link_availability'] = round(1 - (df_groupby['link_downtime'] / df_groupby['time_range']), 4)
		df_groupby['overall'] = round(df_groupby['uptime'] / df_groupby['time_range'], 4)

		return df_groupby[output_columns]


@dataclass(frozen=True, kw_only=True)
class DowntimeStatistics:
	""""""
	data: pd.DataFrame
	total_count: int = field(init=False, default=None)
	total_downtime: datetime.timedelta = field(init=False, default_factory=datetime.timedelta)
	downtime_min: datetime.timedelta = field(init=False, default_factory=datetime.timedelta)
	downtime_max: datetime.timedelta = field(init=False, default_factory=datetime.timedelta)
	downtime_avg: datetime.timedelta = field(init=False, default_factory=datetime.timedelta)
	longest_downtime: RTUDownTimeModel = field(init=False, default=None)

	def __repr__(self):
		return repr_dataclass(self)

	def __post_init__(self):
		self.calculate_downtime(self.data)

	def _get_longest_downtime(self, df: pd.DataFrame) -> Optional[RTUDownTimeModel]:
		if df.shape[0]:
			return RTUDownTimeModel.from_series(df.loc[df['duration'].idxmax()])
		else:
			return None

	def calculate_downtime(self, df: pd.DataFrame):
		total_count = df.shape[0]
		attrs = dict(total_count=total_count)
		if total_count:
			attrs.update(
				total_downtime=df['duration'].sum().to_pytimedelta(),
				downtime_min=df['duration'].min().to_pytimedelta(),
				downtime_max=df['duration'].max().to_pytimedelta(),
				downtime_avg=df['duration'].mean().to_pytimedelta(),
				longest_downtime=self._get_longest_downtime(df),
			)

		frozen_dataclass_set(self, **attrs)


@dataclass(frozen=True, kw_only=True)
class AvRTUResult(AvailabilityResult, DowntimeStatistics):
	"""
	"""
	data: RTUDownData
	rtu_count: int = field(init=False, default=0)
	maintenance: DowntimeStatistics = field(init=False, default=None)
	link: DowntimeStatistics = field(init=False, default=None)
	rtu: DowntimeStatistics = field(init=False, default=None)
	other: DowntimeStatistics = field(init=False, default=None)
	uncategorized: DowntimeStatistics = field(init=False, default=None)
	max_occurences: RTUAvailabilityModel = field(init=False, default=None)
	max_total_dt: RTUAvailabilityModel = field(init=False, default=None)

	def __post_init__(self):
		super().__post_init__()
		attrs = dict()
		data = self.data.all
		self.calculate_downtime(data)
		if self.total_count:
			filter_maintenance = data['marked_maintenance']=='*'
			filter_link = data['marked_link_failure']=='*'
			filter_rtu = data['marked_rtu_failure']=='*'
			filter_other = data['marked_other_failure']=='*'
			attrs.update(
				rtu_count=self.data.availability.shape[0],
				maintenance=DowntimeStatistics(data=data[filter_maintenance]),
				link=DowntimeStatistics(data=data[filter_link]),
				rtu=DowntimeStatistics(data=data[filter_rtu]),
				other=DowntimeStatistics(data=data[filter_other]),
				uncategorized=DowntimeStatistics(data=data[~((filter_maintenance) | (filter_link) | (filter_rtu) | (filter_other))]),
				max_occurences=self._rtu_with_most_occurences(),
				max_total_dt=self._rtu_with_longest_downtime(),
			)

		frozen_dataclass_set(self, **attrs)

	def _rtu_with_most_occurences(self) -> Optional[RTUAvailabilityModel]:
		if self.data.availability.shape[0]:
			return RTUAvailabilityModel.from_series(self.data.availability.loc[self.data.availability['downtime_occurences'].idxmax()])
		else:
			return

	def _rtu_with_longest_downtime(self) -> Optional[RTUAvailabilityModel]:
		if self.data.availability.shape[0]:
			return RTUAvailabilityModel.from_series(self.data.availability.loc[self.data.availability['total_downtime'].idxmax()])
		else:
			return


class RTUCore(AvailabilityCore):
	topic = 'event downtime'
	subject = 'availability RTU'
	model_class = RTUDownTimeModel
	config: RTUConfig

	def __init__(self, config: Optional[RTUConfig] = None, data: Optional[SOEData] = None, **kwargs):
		cfg = config if isinstance(config, RTUConfig) else RTUConfig()
		super().__init__(config=cfg, data=data, **kwargs)

	def _get_category_note(self, downtime: Union[datetime.timedelta, int, float]) -> Optional[str]:
		"""Annotate downtime duration category from defined rules.

		Args:
			downtime : downtime duration

		Result:
			Annotation text
		"""
		category = self.config.rules.categorize(downtime)
		if category is None:
			return category
		else:
			return f'Downtime > {category.threshold.total_seconds()//3600} jam ({category.name})'

	def select_data(self) -> pd.DataFrame:
		return self.data.RTU

	def get_key_items(self, df: pd.DataFrame) -> List[str]:
		rtu_names = set(self.config.rtu_names.keys())
		rtu_in_soe = df['b3'].unique()
		if self.config.known_rtus_only:
			return list(rtu_names.intersection(set(rtu_in_soe)))
		else:
			return list(rtu_in_soe)

	def get_data_count(self, df: pd.DataFrame) -> int:
		status_down = ('Down',)
		event_down = df[df['status'].isin(status_down)]
		return super().get_data_count(event_down)

	def analyze_for_key(self, df: pd.DataFrame, key: str, **kwargs) -> List[Dict[str, Any]]:
		df_rtu = df[df['b3']==key]
		# Get index of columns
		i_sys_tstamp = df.columns.get_loc('system_timestamp')
		i_sys_msec = df.columns.get_loc('system_ms')
		i_status = df.columns.get_loc('status')
		i_user_comment = df.columns.get_loc('user_comment')
		# i_b3_text = df.columns.get_loc('B3 text')

		index0 = df.index.min()
		t0 = join_datetime(*self.data.his.iloc[0, [i_sys_tstamp, i_sys_msec]])
		rtu_datas: List[Dict[str, Any]] = list()
		notes: List[str] = list()

		# Look for queue for inter-process communication
		queue: Queue = kwargs.get('queue')
		if queue is not None:
			n = self.get_data_count(df_rtu)
			qmsg = QueueMessage(
				message=f'Menganalisa {n} kali downtime RTU {key}.',
				count=n
			)
			queue.put(qmsg)

		for x in range(df_rtu.shape[0]):
			t1 = join_datetime(*df_rtu.iloc[x, [i_sys_tstamp, i_sys_msec]])
			# status, comment, description = df_rtu.iloc[x, [i_status, i_user_comment, i_b3_text]]
			status, comment = df_rtu.iloc[x, [i_status, i_user_comment]]
			description = ''
			long_name = self.config.rtu_names.get(key) or description
			data = RTUDownTimeModel(
				rtu=key,
				long_name=long_name,
				navigation=(0, 0)
			)

			# Copy User Comment if any
			if comment:
				if self.config.maintenance_mark in comment:
					data.marked_maintenance = True
					notes.append('User menandai downtime akibat pemeliharaan**')
				elif self.config.link_failure_mark in comment:
					data.marked_link_failure = True
					notes.append('User menandai downtime akibat gangguan telekomunikasi**')
				elif self.config.rtu_failure_mark in comment:
					data.marked_rtu_failure = True
					notes.append('User menandai downtime akibat gangguan RTU**')
				elif self.config.other_failure_mark in comment:
					data.marked_other_failure =True
					notes.append('User menandai downtime akibat gangguan lainnya**')
				else:
					# Eleminate unnecessary character
					txt = re.sub(r'^\W*|\s*$', '', comment)
					notes += txt.split('\n')

			if status=='Up':
				t_up = t1
				nav1 = df_rtu.iloc[x].name
				if x==0:
					# This is first event of RTU
					# Calculate downtime duration in second and append to analyzed_rows
					t_down = t0
					nav0 = index0
				else:
					# At least 1 event before current
					# Make sure if previous event is status of down, or will marked as invalid
					if df_rtu.iloc[x-1, i_status]=='Down':
						# VALID
						t_down = join_datetime(*df_rtu.iloc[x-1, [i_sys_tstamp, i_sys_msec]])
						nav0 = df_rtu.iloc[x-1].name
					else:
						# INVALID
						t_down = t1
						nav0 = nav1
						notes.append('Event status down tidak ditemukan.')
			elif status=='Down':
				t_down = t1
				nav0 = df_rtu.iloc[x].name
				if x==df_rtu.shape[0]-1:
					# RTU down until max time range
					t_up = join_datetime(*self.data.his.iloc[self.data.his.shape[0]-1, [i_sys_tstamp, i_sys_msec]])
					nav1 = df.index.max()
				else:
					# At least 1 event after current
					# Make sure if next event is status of up, or will marked as invalid
					if df_rtu.iloc[x+1, i_status]=='Up':
						# VALID
						continue
					else:
						# INVALID
						t_up = t1
						nav1 = nav0
						notes.append('Event status up tidak ditemukan.')
			else:
				# Unknown / unexpected status
				continue
					
			downtime = t_up - t_down
			category = self._get_category_note(downtime)
			if category: notes.append(category)

			data.down_time = t_down
			data.up_time = t_up
			data.duration = downtime
			data.ack_downtime = t_down	# Default to system down time
			data.fix_duration = downtime
			data.annotations = '\n'.join(notes)
			data.navigation = (nav0, nav1)

			rtu_datas.append(data.dump())
			# Reset anno
			notes.clear()

		return rtu_datas

	def post_analyze(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
		df_post = df.sort_values(['down_time', 'up_time'], ascending=[True, True]).reset_index(drop=True)
		return super().post_analyze(df_post, **kwargs)


class SheetDowntime(SheetWrapper, model_class=RTUDownTimeModel):

	def generate_formula(self, sref: Optional[SheetSOE] = None, **kwargs):
		self.header_format.text_wrap = True

		# Real excel row start with 1 + 1 row header
		range_ = range(self.row_offset + 1, self.data_rows + self.row_offset + 1)
		create_hyperlink = bool('navigation' in self.data.columns and sref is not None)

		cell_ack_downtime = self.xlcell_var('ack_downtime', abs_col=True)
		formula_expr = {
			'duration': f'={self.xlcell_var("up_time", abs_col=True)}-{self.xlcell_var("down_time", abs_col=True)}',
			'fix_duration': xlf.if_(
				xlf.isnumber(cell_ack_downtime),
				self.xlcell_var('up_time', abs_col=True) + '-' + cell_ack_downtime,
				self.xlcell_var('duration', abs_col=True),
				eq=True
			)
		}

		formula = dict()
		for column, expr in formula_expr.items():
			formula[column] = list(map(lambda r: expr.format(row=r), range_))

		if create_hyperlink:
			formula['navigation'] = list(map(
				lambda point: xl_hyperlink_to_range(sref, point[0], point[1], text='CARI >>'), self.data['navigation'].tolist()
			))

		self.formula = pd.DataFrame(data=formula)


class SheetAvailability(SheetWrapper, model_class=RTUAvailabilityModel):

	def generate_formula(self, sref: Optional[SheetDowntime] = None, **kwargs):
		self.header_format.text_wrap = True

		# Real excel row start with 1 + 1 row header
		range_ = range(self.row_offset + 1, self.data_rows + self.row_offset + 1)

		range_fix_duration = sref.get_xlcolumn_range('fix_duration', isexternal=True)
		rule_rtu = xlf.range_criteria(sref.get_xlcolumn_range('rtu', isexternal=True), self.xlcell_var('rtu'))
		rule_dtlink = xlf.range_criteria(sref.get_xlcolumn_range('marked_link_failure', isexternal=True), '"*"')
		rule_dtrtu = xlf.range_criteria(sref.get_xlcolumn_range('marked_rtu_failure', isexternal=True), '"*"')
		rule_dtother = xlf.range_criteria(sref.get_xlcolumn_range('marked_other_failure', isexternal=True), '"*"')
		rule_dtuncertain = [
			xlf.range_criteria(sref.get_xlcolumn_range('marked_maintenance', isexternal=True), '""'),
			xlf.range_criteria(sref.get_xlcolumn_range('marked_link_failure', isexternal=True), '""'),
			xlf.range_criteria(sref.get_xlcolumn_range('marked_rtu_failure', isexternal=True), '""'),
			xlf.range_criteria(sref.get_xlcolumn_range('marked_other_failure', isexternal=True), '""'),
		]

		cell_time_range = self.xlcell_var('time_range', abs_col=True)
		cell_quality = self.xlcell_var('quality', abs_col=True)

		formula_expr = {
			'downtime_occurences': xlf.countifs(rule_rtu, eq=True),
			'total_downtime': xlf.sumifs(range_fix_duration, rule_rtu, eq=True),
			'average_downtime': xlf.averageifs(range_fix_duration, rule_rtu, default='0', eq=True),
			'rtu_downtime': xlf.sumifs(range_fix_duration, rule_rtu, rule_dtrtu, eq=True),
			'link_downtime': xlf.sumifs(range_fix_duration, rule_rtu, rule_dtlink, eq=True),
			'other_downtime': xlf.sumifs(range_fix_duration, rule_rtu, rule_dtother, eq=True),
			'unclassified_downtime': xlf.sumifs(range_fix_duration, rule_rtu, *rule_dtuncertain, eq=True),
			'uptime': f'={cell_time_range}-{self.xlcell_var("total_downtime", abs_col=True)}',
			'rtu_availability': xlf.div(
				f'({cell_time_range}-{self.xlcell_var("rtu_downtime", abs_col=True)})*{cell_quality}',
				cell_time_range,
				eq=True
			),
			'link_availability': xlf.div(
				f'({cell_time_range}-{self.xlcell_var("link_downtime", abs_col=True)})*{cell_quality}',
				cell_time_range,
				eq=True
			),
			'overall': xlf.div(
				f'({cell_time_range}-{self.xlcell_var("unclassified_downtime", abs_col=True)})*{cell_quality}',
				cell_time_range,
				eq=True
			),
		}

		formula = dict()
		for column, expr in formula_expr.items():
			formula[column] = list(map(lambda r: expr.format(row=r), range_))

		self.formula = pd.DataFrame(data=formula)

	def get_footer_data(self, **kwargs) -> pd.DataFrame:
		row = dict()
		row['downtime_occurences'] = xlf.sum(self.xlcolumn_range['downtime_occurences'], eq=True)
		row['quality'] = xlf.sum(self.xlcolumn_range['quality'], eq=True)
		for column in ['rtu_availability', 'link_availability', 'overall']:
			row[column] = xlf.div(
				xlf.sum(self.xlcolumn_range[column]),
				f'${self.xlcolumn["quality"]}${self.data_rows+2}',
				eq=True
			)

		return pd.DataFrame(data=[row], columns=model_fieldnames(self.model_class)).fillna('')


class RTU(Availability):
	config: RTUConfig
	result: AvRTUResult

	def init_core(self) -> RTUCore:
		return RTUCore(self.config)

	def get_result(self, df: pd.DataFrame, start_date: Optional[DateSpec] = None, end_date: Optional[DateSpec] = None) -> AvRTUResult:
		if isinstance(start_date, datetime.datetime) and isinstance(end_date, datetime.datetime):
			pass
		elif isinstance(start_date, datetime.date) and isinstance(end_date, datetime.date):
			start_date = datetime.datetime.combine(start_date, datetime.time())
			end_date = datetime.datetime.combine(end_date, datetime.time(23, 59, 59, 999999))
		else:
			columns = ['down_time', 'up_time']
			# Get date min and max from dataframe 
			start_date = df[columns].min(axis=1).min(axis=0).to_pydatetime()
			end_date = df[columns].max(axis=1).max(axis=0).to_pydatetime()

		df_filter = df[(df['down_time']>=start_date) & (df['up_time']<=end_date)]
		rtu_data = RTUDownData(df_filter, start_date=start_date, end_date=end_date, config=self.config)
		return AvRTUResult(data=rtu_data)

	def post_read_file(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
		columns = df.columns.tolist()
		# Remove navigation column, not used in cummulative loads
		try_remove(columns, 'navigation')

		# Exclude footer data
		df_rtu = df[~(df['down_time'].isna()) & ~(df['up_time'].isna())]
		df_rtu = df_rtu[columns]\
			.fillna('')\
			.reset_index(drop=True)
		self.data = df_rtu
		return super().post_read_file(df, **kwargs)

	def get_xlsheet(self, **infokwargs) -> Dict[str, SheetWrapper]:
		sheets: List[SheetWrapper] = list()
		sheet1 = None
		filename = infokwargs.get('filename', '<undefined>')

		# Add sheet HIS_MESSAGES if any
		if isinstance(self.core.data, SOEData):
			sheet1 = SheetSOE('HIS_MESSAGES', data=self.core.data.his, master=self.config.master)
			sheets.append(sheet1)

		sheet2 = SheetDowntime('DOWNTIME', data=self.result.data.all, master=self.config.master)
		sheet3 = SheetAvailability('AVAILABILITY', data=self.result.data.availability, master=self.config.master)
		sheet4 = SheetInfo(
			'Info',
			info_data=[
				SheetInfo.sub_title('SUMMARY'),
				('RTU Availability', f'=ROUND({sheet3.xlcell(sheet3.data_rows+1, "rtu_availability", isexternal=True)}*100,2)&"%"'),
				('Link Availability', f'=ROUND({sheet3.xlcell(sheet3.data_rows+1, "link_availability", isexternal=True)}*100,2)&"%"'),
				('Overall', f'=ROUND({sheet3.xlcell(sheet3.data_rows+1, "overall", isexternal=True)}*100,2)&"%"'),
			],
			kwargs={
				'source': self.sources or '<undefined>',
				'output': f'{filename}.xlsx',
				'date_range': (self.result.date_min, self.result.date_max),
				'execution_time': f'{self.duration:.3f}',
				'node': settings.PY_NODE,
				'processed_date': datetime.datetime.now(),
				'user': 'fasop',
			}
		)

		# Generate sheet formula
		sheet2.generate_formula(sheet1)
		sheet3.generate_formula(sheet2)
		sheets.extend([sheet2, sheet3, sheet4])
		return {sheet.sheet_name: sheet for sheet in sheets}

	def get_properties(self) -> Dict[str, str]:
		return super().get_properties(title='Availability Remote Station & Link')


# def av_analyze_file(**params):
# 	handler = AVRSFromFile
# 	filepaths = 'sample/sample_rtu*.xlsx'
# 	title = 'RTU'
# 	return test_analyze(handler, title=title, filepaths=filepaths)

# def av_collective(**params):
# 	handler = AVRSCollective
# 	filepaths = 'sample/sample_rtu*.xlsx'
# 	title = 'RTU'
# 	return test_collective(handler, title=title, filepaths=filepaths)


# if __name__=='__main__':
# 	test_list = [
# 		('Test analisa file SOE Spectrum', av_analyze_file),
# 		('Test menggabungkan file', av_collective)
# 	]
# 	ans = input('Confirm troubleshooting? [y/n]  ')
# 	if ans=='y':
# 		print('\r\n'.join([f'  {no+1}.'.ljust(6) + tst[0] for no, tst in enumerate(test_list)]))
# 		choice = int(input(f'\r\nPilih modul test [1-{len(test_list)}] :  ')) - 1
# 		if choice in range(len(test_list)):
# 			print()
# 			test = test_list[choice][1]()
# 		else:
# 			print('Pilihan tidak valid!')