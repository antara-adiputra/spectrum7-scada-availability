import datetime, re, time
from dataclasses import asdict, dataclass, field
from queue import Queue

import numpy as np
import pandas as pd

from .base import Base, Config, DataModel, FieldMetadata, frozen_dataclass_set, model_fieldnames, model_mappings, repr_dataclass
from .excel import *
from .filewriter import SheetInfo, SheetSOE, SheetWrapper, xl_col_to_name, xl_hyperlink_to_range
from .main import Availability, AvailabilityCore, AvailabilityData, AvailabilityResult, QueueMessage
from .soe import SOEData
from . import params
from ..lib import get_datetime, get_execution_duration, get_termination_duration, get_rtu_timestamp, get_system_timestamp, join_datetime, logprint, toggle_attr, try_remove
from ..types import *
from .. import settings


Cell: TypeAlias = Tuple[int, str]
KeyPair: TypeAlias = Tuple[str, str]

xlf = XlsxFormula()


@dataclass
class MarkableRCDEvent(Base):
	"""Row data structure used in RCD event table, include formatting & sizing in Excel."""
	order_time: datetime.datetime = field(
		default=None,
		metadata=FieldMetadata(
			header='Order Time',
			required=True,
			column_format=XLS_FORMAT_DATETIME,
			width=25,
		)
	)
	feedback_time: datetime.datetime = field(
		default=None,
		metadata=FieldMetadata(
			header='Feedback Time',
			required=True,
			column_format=XLS_FORMAT_DATETIME,
			width=25,
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
			freeze=True,
		)
	)
	tag: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Tag',
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
	pre_result: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Pre Result',
			required=True,
			column_format=XLS_FORMAT_TEXT_CENTER,
		)
	)
	execution: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Execution (s)',
			column_format=XLS_FORMAT_DECIMAL3,
			width=12,
		)
	)
	termination: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Termination (s)',
			column_format=XLS_FORMAT_DECIMAL3,
			width=12,
		)
	)
	txrx: float = field(
		default=None,
		metadata=FieldMetadata(
			header='TxRx (s)',
			column_format=XLS_FORMAT_DECIMAL3,
			width=9,
		)
	)
	rep_flag: bool = field(
		default=False,
		metadata=FieldMetadata(
			header='Rep. Flag',
			required=True,
			column_format=XLS_FORMAT_TEXT_CENTER,
			width=7,
		)
	)
	marked_unused: bool = field(
		default=False,
		metadata=FieldMetadata(
			header='Marked Unused',
			required=True,
			column_format=XLS_FORMAT_TEXT_CENTER,
			width=12,
		)
	)
	marked_success: bool = field(
		default=False,
		metadata=FieldMetadata(
			header='Marked Success',
			required=True,
			column_format=XLS_FORMAT_TEXT_CENTER,
			width=12,
		)
	)
	marked_failed: bool = field(
		default=False,
		metadata=FieldMetadata(
			header='Marked Failed',
			required=True,
			column_format=XLS_FORMAT_TEXT_CENTER,
			width=12,
		)
	)
	final_result: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Final Result',
			required=True,
			column_format=XLS_FORMAT_TEXT_CENTER,
		)
	)
	annotations: List[str] = field(
		default_factory=list,
		metadata=FieldMetadata(
			header='Annotations',
			column_format=XLS_FORMAT_TEXT_WRAP,
			width=28,
		)
	)
	navigation: Tuple[int, int] = field(
		default_factory=tuple,
		metadata=FieldMetadata(
			header='Navigation',
			column_format=XlsxFormat(bold=True, font_color='blue', align='center', valign='vcenter', border=1, bg_color='#dcdcdc'),
			width=12,
		)
	)
	order_iloc: int = field(
		default=None,
		metadata=FieldMetadata(
			header='Order Row (Helper)',
			column_format=XLS_FORMAT_INTEGER,
			width=12,
		)
	)
	feedback_iloc: int = field(
		default=None,
		metadata=FieldMetadata(
			header='Feedback Row (Helper)',
			column_format=XLS_FORMAT_INTEGER,
			width=12,
		)
	)


@dataclass
class RCDGroupB1(Base):
	"""Row data structure used in RCD for B1 table, include formatting & sizing in Excel."""
	b1: str = field(
		default=None,
		metadata=FieldMetadata(
			header='B1',
			column_format=XLS_FORMAT_TEXT,
			freeze=True,
		)
	)
	occurences: int = field(
		default=None,
		metadata=FieldMetadata(
			header='RC Occurences',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	success: int = field(
		default=None,
		metadata=FieldMetadata(
			header='RC Success',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	failed: int = field(
		default=None,
		metadata=FieldMetadata(
			header='RC Failed',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	success_rate: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Success Rate',
			column_format=XLS_FORMAT_PERCENTAGE,
		)
	)
	execution_avg: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Execution Avg.',
			column_format=XLS_FORMAT_DECIMAL3,
		)
	)
	termination_avg: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Termination Avg.',
			column_format=XLS_FORMAT_DECIMAL3,
		)
	)
	txrx_avg: float = field(
		default=None,
		metadata=FieldMetadata(
			header='TxRx Avg.',
			column_format=XLS_FORMAT_DECIMAL3,
		)
	)


@dataclass
class RCDGroupB3(Base):
	"""Row data structure used in RCD for B1, B2, B3 table, include formatting & sizing in Excel."""
	b1: str = field(
		default=None,
		metadata=FieldMetadata(
			header='B1',
			column_format=XLS_FORMAT_TEXT,
		)
	)
	b2: str = field(
		default=None,
		metadata=FieldMetadata(
			header='B2',
			column_format=XLS_FORMAT_TEXT,
		)
	)
	b3: str = field(
		default=None,
		metadata=FieldMetadata(
			header='B3',
			column_format=XLS_FORMAT_TEXT,
			freeze=True,
		)
	)
	occurences: int = field(
		default=None,
		metadata=FieldMetadata(
			header='RC Occurences',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	success: int = field(
		default=None,
		metadata=FieldMetadata(
			header='RC Success',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	failed: int = field(
		default=None,
		metadata=FieldMetadata(
			header='RC Failed',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	success_rate: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Success Rate',
			column_format=XLS_FORMAT_PERCENTAGE,
		)
	)
	open_success: int = field(
		default=None,
		metadata=FieldMetadata(
			header='Open Success',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	open_failed: int = field(
		default=None,
		metadata=FieldMetadata(
			header='Open Failed',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	close_success: int = field(
		default=None,
		metadata=FieldMetadata(
			header='Close Success',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	close_failed: int = field(
		default=None,
		metadata=FieldMetadata(
			header='Close Failed',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	contribution: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Contribution',
			column_format=XLS_FORMAT_PERCENTAGE,
			width=13
		)
	)
	reduction: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Reduction',
			column_format=XLS_FORMAT_PERCENTAGE,
		)
	)
	tagging: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Tagging',
			column_format=XLS_FORMAT_TEXT_CENTER,
		)
	)


@dataclass
class RCDGroupOperator(Base):
	"""Row data structure used in RCD for Operator table, include formatting & sizing in Excel."""
	operator: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Operator',
			column_format=XLS_FORMAT_TEXT,
			freeze=True,
		)
	)
	occurences: int = field(
		default=None,
		metadata=FieldMetadata(
			header='RC Occurences',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	success: int = field(
		default=None,
		metadata=FieldMetadata(
			header='RC Success',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	failed: int = field(
		default=None,
		metadata=FieldMetadata(
			header='RC Failed',
			column_format=XLS_FORMAT_INTEGER,
		)
	)
	success_rate: float = field(
		default=None,
		metadata=FieldMetadata(
			header='Success Rate',
			column_format=XLS_FORMAT_PERCENTAGE,
		)
	)


@dataclass
class RCEventModel(MarkableRCDEvent, DataModel):

	def __post_init__(self):
		self.rep_flag = bool(self.rep_flag=='*')
		self.marked_unused = bool(self.marked_unused=='*')
		self.marked_success = bool(self.marked_success=='*')
		self.marked_failed = bool(self.marked_failed=='*')
		if isinstance(self.annotations, str):
			self.annotations = self.annotations.split('\n')

	def dump(self, as_title: bool = False, **kwargs) -> Dict[str, Any]:
		dumped = super().dump(as_title, **kwargs)
		frep_flag = 'rep_flag'
		fmark_unused = 'marked_unused'
		fmark_success = 'marked_success'
		fmark_failed = 'marked_failed'
		fannotations = 'annotations'
		if as_title:
			mappings = model_mappings(self, as_dict=True)
			frep_flag = mappings[frep_flag]
			fmark_unused = mappings[fmark_unused]
			fmark_success = mappings[fmark_success]
			fmark_failed = mappings[fmark_failed]
			fannotations = mappings[fannotations]

		dumped[frep_flag] = '*' if self.rep_flag else ''
		dumped[fmark_unused] = '*' if self.marked_unused else ''
		dumped[fmark_success] = '*' if self.marked_success else ''
		dumped[fmark_failed] = '*' if self.marked_failed else ''
		dumped[fannotations] = '\n'.join(self.annotations)
		return dumped


RCB1Model = type('RCB1Model', (RCDGroupB1, DataModel), {})
RCB3Model = type('RCB3Model', (RCDGroupB3, DataModel), {})
RCOperatorModel = type('RCOperatorModel', (RCDGroupOperator, DataModel), {})


@dataclass
class RCDConfig(Config):
	"""RCD calculation configuration dataclass.
	
	Args:
		include_bi : wether includes BI in calculation
		include_repetition : if True, calculate all RC events without eleminate repetition. Default False
		reduction_ratio_threshold : threshold value used in tagging recommendation
		t_search : duration in seconds that used in sapmling SOE events
	"""
	# Whether include Bus Isolator in calculation
	include_bi: bool = False
	# Whether include repetitive RC or not
	include_repetition: bool = False
	# Used in tagging recomendation
	reduction_ratio_threshold: int = 1
	# Duration in seconds that used in sampling SOE events
	t_search: int = 30	# 3*60*60
	success_mark: str = '**success**'
	failed_mark: str = '**failed**'
	unused_mark: str = '**unused**'

	@property
	def elements(self) -> Tuple[str]:
		elements = ['CB']
		if self.include_bi:
			elements += ['BI1', 'BI2']
		return tuple(elements)


@dataclass
class TSDQuality:
	"""Store scanned status quality to faster the analyzation process."""
	CD: Dict[NameOrTuple, str] = field(default_factory=dict)
	CSO: Dict[NameOrTuple, str] = field(default_factory=dict)
	LR: Dict[NameOrTuple, str] = field(default_factory=dict)


@dataclass(frozen=True)
class RCDData(AvailabilityData):
	"""Imutable classified/grouped RCD dataclass.

	Attributes:
		all : RC events data
		valid : all valid data that used for statistics calculation, parameterized in configuration
		station : grouped RC based on station (B1)
		bay : grouped RC based on bay (B1, B2, B3)
		operator : grouped RC based on RC executor (dispatcher)
		marked : user-marked RC data
	"""
	config: RCDConfig = field(kw_only=True)
	valid: pd.DataFrame = field(init=False, default=None)
	station: pd.DataFrame = field(init=False, default=None)
	bay: pd.DataFrame = field(init=False, default=None)
	operator: pd.DataFrame = field(init=False, default=None)
	marked: pd.DataFrame = field(init=False, default=None)

	def __repr__(self):
		return repr_dataclass(self)

	def __post_init__(self):
		super().__post_init__()
		if not isinstance(self.all, pd.DataFrame):
			logprint(f'Invalid data type of {type(self.all)}', level='error')
			return

		df = self.all.copy()\
			.sort_values(['order_time'], ascending=[True])\
			.reset_index(drop=True)
		# Apply filtering for calculations to eliminate junk data
		df_valid = df[df['marked_unused']=='']
		# Filter only rows without repetition-marked
		if not self.config.include_repetition:
			df_valid = df_valid[df_valid['rep_flag']=='']

		station = self.group_station(df_valid)
		bay = self.group_bay(df_valid)
		operator = self.group(df_valid, ['operator'])
		# Marked RC stats
		marked = df[
			(df['marked_failed']=='*') |
			(df['marked_success']=='*') |
			(df['marked_unused']=='*')
		]
		frozen_dataclass_set(
			self,
			all=df,
			valid=df_valid,
			station=station,
			bay=bay,
			operator=operator,
			marked=marked,
		)

	def group(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
		"""Base function to get aggregation values based on defined "columns".

		Args:
			df : data input
			columns : list of columns as reference

		Result:
			Grouped data
		"""
		groupby_columns = columns + ['final_result']
		rc_count = df[groupby_columns]\
			.groupby(columns, as_index=False)\
			.count()\
			.rename(columns={'final_result': 'occurences'})
		rc_success = df.loc[(df['final_result']=='SUCCESS'), groupby_columns]\
			.groupby(columns, as_index=False)\
			.count()\
			.rename(columns={'final_result': 'success'})
		rc_failed = df.loc[(df['final_result']=='FAILED'), groupby_columns]\
			.groupby(columns, as_index=False)\
			.count()\
			.rename(columns={'final_result': 'failed'})
		df_groupby = rc_count.merge(right=rc_success, how='left', on=columns)\
			.merge(right=rc_failed, how='left', on=columns)\
			.fillna(0)
		df_groupby['success_rate'] = np.round(df_groupby['success']/df_groupby['occurences'], 4)
		return df_groupby

	def group_station(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Get aggregation values based on columns Station (B1).

		Args:
			df : data input

		Result:
			Grouped data
		"""
		columns = ['b1']
		groupby_columns = columns + ['execution', 'termination', 'txrx']
		df_groupby = self.group(df, columns)
		df_tmp = df.loc[df['final_result']=='SUCCESS', groupby_columns]\
			.groupby(columns, as_index=False)\
			.mean()\
			.round(3)\
			.rename(
				columns={
					'execution': 'execution_avg',
					'termination': 'termination_avg',
					'txrx': 'txrx_avg'
				}
			)
		df_groupby = df_groupby.merge(right=df_tmp, how='left', on=columns)\
			.fillna(0)
		return df_groupby

	def group_bay(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Get aggregation values based on Bay columns reference (B1, B2, B3).

		Args:
			df : data input

		Result:
			Grouped data
		"""
		columns = ['b1', 'b2', 'b3']
		groupby_columns = columns + ['final_result']
		df_groupby = None
		# Assign column 'open_success', 'open_failed', 'close_success', 'close_failed'
		for status in ['open', 'close']:
			for result in ['success', 'failed']:
				df_tmp = df.loc[(df['final_result']==result.upper()) & (df['status']==status.title()), groupby_columns]\
					.groupby(columns, as_index=False)\
					.count()\
					.rename(columns={'final_result': f'{status}_{result}'})
				df_groupby = df_groupby.merge(right=df_tmp, how='outer', on=columns) if isinstance(df_groupby, pd.DataFrame) else df_tmp

		df_groupby = self.group(df, columns)\
			.merge(right=df_groupby, how='left', on=columns)\
			.fillna(0)
		total_rc = df_groupby['occurences'].sum()
		df_groupby['contribution'] = df_groupby['occurences'].map(lambda x: x/total_rc)
		df_groupby['reduction'] = df_groupby['failed'].map(lambda y: y/total_rc)
		df_groupby['tagging'] = ''
		return df_groupby


@dataclass(frozen=True, kw_only=True)
class AvRCDResult(AvailabilityResult):
	"""Imutable RC calculation statistics.

	Args:
		data : RC dataclass or dataframe

	Result:
		- Success ratio Close, Open, and overall
		- Total repetition events
		- Comparison between Close & Open operation
	"""
	data: RCDData
	total_count: int = field(init=False, default=None)
	total_reps: int = field(init=False, default=None)
	total_marked: int = field(init=False, default=None)
	total_marked_failed: int = field(init=False, default=None)
	total_marked_success: int = field(init=False, default=None)
	total_marked_unused: int = field(init=False, default=None)
	total_valid: int = field(init=False, default=None)
	total_valid_close: int = field(init=False, default=None)
	total_valid_open: int = field(init=False, default=None)
	total_failed: int = field(init=False, default=None)
	total_failed_close: int = field(init=False, default=None)
	total_failed_open: int = field(init=False, default=None)
	total_success: int = field(init=False, default=None)
	total_success_close: int = field(init=False, default=None)
	total_success_open: int = field(init=False, default=None)
	success_ratio: float = field(init=False, default=None)
	success_close_ratio: float = field(init=False, default=None)
	success_open_ratio: float = field(init=False, default=None)

	def __repr__(self):
		return repr_dataclass(self)

	def __post_init__(self):
		super().__post_init__()
		data = self.data.all
		valid = self.data.valid
		bay = self.data.bay
		total_count = data.shape[0]
		# Repetition stats
		total_reps = data[data['rep_flag']=='*'].shape[0]
		total_marked_failed = data[data['marked_failed']=='*'].shape[0]
		total_marked_success = data[data['marked_success']=='*'].shape[0]
		total_marked_unused = data[data['marked_unused']=='*'].shape[0]
		total_marked = total_marked_failed + total_marked_success + total_marked_unused
		# Valid RC stats
		total_valid_close = valid[valid['status']=='Close'].shape[0]
		total_valid_open = valid[valid['status']=='Open'].shape[0]
		total_valid = valid.shape[0]
		total_failed = bay['failed'].sum()
		total_failed_close = bay['close_failed'].sum()
		total_failed_open = bay['open_failed'].sum()
		total_success = bay['success'].sum()
		total_success_close = bay['close_success'].sum()
		total_success_open = bay['open_success'].sum()
		# Overall RC stats
		success_ratio = round(self.div(total_success, total_valid), 4)
		success_close_ratio = round(self.div(total_success_close, total_valid_close), 4)
		success_open_ratio = round(self.div(total_success_open, total_valid_open), 4)

		frozen_dataclass_set(
			self,
			total_count=total_count,
			total_reps=total_reps,
			total_marked_failed=total_marked_failed,
			total_marked_success=total_marked_success,
			total_marked_unused=total_marked_unused,
			total_marked=total_marked,
			total_valid_close=total_valid_close,
			total_valid_open=total_valid_open,
			total_valid=total_valid,
			total_failed=total_failed,
			total_failed_close=total_failed_close,
			total_failed_open=total_failed_open,
			total_success=total_success,
			total_success_close=total_success_close,
			total_success_open=total_success_open,
			success_ratio=success_ratio,
			success_close_ratio=success_close_ratio,
			success_open_ratio=success_open_ratio,
		)

	@staticmethod
	def div(x, y, default: int = 0):
		"""Formula : <x>/<y>, if error return <default> value."""
		try:
			if y==0:
				z = default
			else:
				z = x / y
		except Exception:
			z = default
		return z


class RCDCore(AvailabilityCore):
	topic = 'event RCD'
	subject = 'keberhasilan RCD'
	model_class = RCEventModel
	config: RCDConfig

	def __init__(self, config: Optional[RCDConfig] = None, data: Optional[SOEData] = None, **kwargs):
		cfg = config if isinstance(config, RCDConfig) else RCDConfig()
		super().__init__(config=cfg, data=data, **kwargs)
		self.sts_quality = TSDQuality()

	def _get_failed_annotation(self, row_order: pd.Series, row_status: pd.Series):
		"""Find status from SOE data of specific bay.

		Args:
			row_order : RC order event row
			row_status : event row of status changes to check

		Result:
			Extra information about the condition when failed RC occured
		"""
		t_delta = get_execution_duration(row_order, row_status)
		elm = row_order['element']
		sts0 = row_order['status']
		sts1 = row_status['status']
		lr_st, lr_q = self._check_remote_status(row_status)
		cd_st, cd_q = self._check_enable_status(row_status)
		# Additional status monitored (CD, LR, etc.)
		extra_info = list()
		annotation = ''

		if cd_q=='good':
			extra_info.append(f'CD={cd_st}')

		if sts0=='Close':
			# Check synchro mode and protection interlock on RC Close
			cso_st, cso_q = self._check_synchro_interlock(row_status)
			prt = self._check_protection_interlock(row_status)
			# Add CSO status if CSO quality is Good
			if cso_q=='good':
				extra_info.append(f'CSO={cso_st}')
			# Add protection status if any
			if prt:
				extra_info.append(f'{prt}=Appeared')

		if lr_st=='Remote' and t_delta<=params.MONITORING_TIME[elm]:
			# Could be Success RC
			if sts0==sts1:
				annotation = f'Potensi RC sukses ({t_delta}s)'
			elif sts1=='Dist.':
				annotation = f'Potensi RC sukses tapi status {sts0} anomali ({t_delta}s)'
			else:
				annotation = f'RC {sts0} tapi status balikan {sts1}. Perlu ditelusuri!'
		else:
			# Valid status or anomaly
			if sts0==sts1:
				annotation = 'Eksekusi lokal GI'
			elif sts1=='Dist.':
				annotation = f'Eksekusi lokal GI tapi status {sts0} anomali'

		if extra_info:
			annotation += f' ({", ".join(extra_info)})'

		return annotation

	def _check_enable_status(self, data: pd.Series) -> Tuple[str, str]:
		"""Check CD (Control Disable) status on an event.

		Args:
			data : RC event row

		Result:
			CD status and CD quality
		"""
		# Initialize
		status = 'Enable'
		b1 = data['b1']
		t_ref = get_rtu_timestamp(data)
		df = self.data.CD
		df_cd: pd.DataFrame = df[df['b1']==b1]

		# Check CD quality in cached memory
		if b1 in self.sts_quality.CD:
			quality = self.sts_quality.CD[b1]
		else:
			# Do CD value check
			cd_values = df_cd['status'].values
			if 'Enable' in cd_values and 'Disable' in cd_values:
				# CD quality good (Enable and Disable exists)
				quality = 'good'
			elif ('Enable' in cd_values or 'Disable' in cd_values) and 'Dist.' in cd_values:
				# CD quality bad (Dist. or one of Enable or Disable only)
				quality = 'bad'
			else:
				# CD quality unknown, no status changes occured
				quality = 'uncertain'

			# Cache quality
			self.sts_quality.CD[b1] = quality

		if quality in ['good', 'bad']:
			# If quality good, filter only valid status
			if quality=='good':
				df_cd = df_cd[df_cd['status'].isin(['Enable', 'Disable'])]

			if df_cd[join_datetime(df_cd['system_timestamp'], df_cd['system_ms'])<t_ref].empty:
				# CD status changes occured after
				cd_first_change = df_cd[join_datetime(df_cd['system_timestamp'], df_cd['system_ms'])>=t_ref].iloc[0]
				if cd_first_change['status']=='Enable':
					status = 'Disable'
				else:
					status = 'Enable'
			else:
				# CD status changes occured before 
				cd_last_change = df_cd[join_datetime(df_cd['system_timestamp'], df_cd['system_ms'])<t_ref].iloc[-1]
				if cd_last_change['status']=='Enable':
					status = 'Enable'
				else:
					status = 'Disable'

		return status, quality

	def _check_rtu_status(self, data: pd.Series) -> Tuple[str, str]:
		"""Check RTU status on an event.

		Args:
			data : RC event row

		Result:
			IFS status and IFS name
		"""
		# Initialize
		status = 'Up'
		t_hyst = 2*60
		df = self.data.RTU
		# Change here if using rtu_name_matching
		rtu_name = data['b1']
		t_ref = get_rtu_timestamp(data)

		if rtu_name:
			df_rtu: pd.DataFrame = df[self.data.RTU['b3']==rtu_name]
			if not df_rtu.empty:
				if df_rtu[join_datetime(df_rtu['system_timestamp'], df_rtu['system_ms'])<t_ref].empty:
					# IFS status changes occured after
					rtu_first_change = df_rtu[join_datetime(df_rtu['system_timestamp'], df_rtu['system_ms'])>=t_ref].iloc[0]
					t_delta = round((get_system_timestamp(rtu_first_change) - t_ref).total_seconds(), 1)
					if abs(t_delta)<t_hyst:
						# Transition phase to Up or Down
						if rtu_first_change['status']=='Down':
							status = f'transisi menuju Down ({t_delta}s)'
						else:
							status = f'transisi menuju Up ({t_delta}s)'
					else:
						if rtu_first_change['status']=='Down':
							status = 'Up'
						else:
							status = 'Down'
				else:
					# IFS status changes occured before
					rtu_last_change = df_rtu[join_datetime(df_rtu['system_timestamp'], df_rtu['system_ms'])<t_ref].iloc[-1]
					if rtu_last_change['status']=='Down':
						status = 'Down'
					else:
						status = 'Up'

		return status, rtu_name

	def _check_protection_interlock(self, data: pd.Series) -> str:
		"""Check protection signal on an event.

		Args:
			data : RC event row

		Result:
			Active protection signal
		"""
		# Initialize
		active_prot = ''
		index = -1
		b1 = data['b1']
		b2 = data['b2']
		b3 = data['b3']
		t_ref = get_rtu_timestamp(data)
		df = self.data.PROT
		df_prt: pd.DataFrame = df[
			(df['b1']==b1) &
			(df['b2']==b2) &
			(df['b3']==b3)
		]

		if not df_prt[join_datetime(df_prt['system_timestamp'], df_prt['system_ms'])<t_ref].empty:
			# Latched protection Appeared before
			prot_last_appear: pd.Series = df_prt[join_datetime(df_prt['system_timestamp'], df_prt['system_ms'])<t_ref].iloc[-1]
			if prot_last_appear['status']=='Appeared':
				active_prot = prot_last_appear['element']
				index = prot_last_appear.name

		return active_prot

	def _check_remote_status(self, data: pd.Series) -> Tuple[str, str]:
		"""Check LR (Local/Remote) status on an event.

		Args:
			data : RC event row

		Result:
			LR status and LR quality
		"""
		# Initialize
		status = 'Remote'
		b1 = data['b1']
		b2 = data['b2']
		b3 = data['b3']
		t_ref = get_rtu_timestamp(data)
		df = self.data.LR
		df_lr: pd.DataFrame = df[
			(df['b1']==b1) &
			(df['b2']==b2) &
			(df['b3']==b3)
		]

		# Check LR quality in cached memory
		if (b1, b2, b3) in self.sts_quality.LR:
			quality = self.sts_quality.LR[(b1, b2, b3)]
		else:
			# Do LR value check
			lr_values = df_lr['status'].values
			if 'Remote' in lr_values and 'Local' in lr_values:
				# LR quality good (Remote and Local exists)
				quality = 'good'
			elif ('Remote' in lr_values or 'Local' in lr_values) and 'Dist.' in lr_values:
				# LR quality bad (Dist. or one of Remote or Local only)
				quality = 'bad'
			else:
				# LR quality unknown, no status changes occured
				quality = 'uncertain'

			# Cache quality
			self.sts_quality.LR[(b1, b2, b3)] = quality

		if quality in ['good', 'bad']:
			# If quality good, filter only valid status
			if quality=='good':
				df_lr = df_lr[df_lr['status'].isin(('Remote', 'Local'))]

			if df_lr[join_datetime(df_lr['system_timestamp'], df_lr['system_ms'])<t_ref].empty:
				# LR status changes occured after
				lr_first_change = df_lr[join_datetime(df_lr['system_timestamp'], df_lr['system_ms'])>=t_ref].iloc[0]
				if lr_first_change['status']=='Remote':
					status = 'Local'
				else:
					status = 'Remote'
			else:
				# LR status changes occured before
				lr_last_change = df_lr[join_datetime(df_lr['system_timestamp'], df_lr['system_ms'])<t_ref].iloc[-1]
				if lr_last_change['status']=='Remote':
					status = 'Remote'
				else:
					status = 'Local'

		return status, quality

	def _check_synchro_interlock(self, data: pd.Series) -> Tuple[str, str]:
		"""Check CSO (Check Synchro Override) status on an event.

		Args:
			data : RC event row

		Result:
			CSO status and CSO quality
		"""
		# Initialize
		status = 'Off'
		b1 = data['b1']
		b2 = data['b2']
		b3 = data['b3']
		t_ref = get_rtu_timestamp(data)
		df = self.data.CSO
		df_cso: pd.DataFrame = df[
			(df['b1']==b1) &
			(df['b2']==b2) &
			(df['b3']==b3)
		]

		# Check CSO quality in cached memory
		if (b1, b2, b3) in self.sts_quality.CSO:
			quality = self.sts_quality.CSO[(b1, b2, b3)]
		else:
			# Do CSO value check
			cso_values = df_cso['status'].values
			if 'On' in cso_values and 'Off' in cso_values:
				# CSO quality good (On and Off exists)
				quality = 'good'
			elif ('On' in cso_values or 'Off' in cso_values) and 'Dist.' in cso_values:
				# CSO quality bad (Dist. or One of On or Off Only)
				quality = 'bad'
			else:
				# CSO quality unknown, no status changes occured
				quality = 'uncertain'

			# Save in buffer
			self.sts_quality.CSO[(b1, b2, b3)] = quality

		if quality in ['good', 'bad']:
			# If quality good, filter only valid status
			if quality=='good':
				df_cso = df_cso[df_cso['status'].isin(('On', 'Off'))]

			if df_cso[join_datetime(df_cso['system_timestamp'], df_cso['system_ms'])<t_ref].empty:
				# CSO status changes occured after
				cso_first_change = df_cso[join_datetime(df_cso['system_timestamp'], df_cso['system_ms'])>=t_ref].iloc[0]
				if cso_first_change['status']=='On':
					status = 'Off'
				else:
					status = 'On'
			else:
				# CSO status changes occured before
				cso_last_change = df_cso[join_datetime(df_cso['system_timestamp'], df_cso['system_ms'])<t_ref].iloc[-1]
				if cso_last_change['status']=='On':
					status = 'On'
				else:
					status = 'Off'

		return status, quality

	def select_data(self) -> pd.DataFrame:
		return self.data.CSW

	def get_key_items(self, df: pd.DataFrame) -> List[Tuple[str, str, str]]:
		columns = ['b1', 'b2', 'b3']
		orders = df.loc[df['tag'].isin(params.ORDER_TAG)]
		unique_bays = orders[columns].drop_duplicates(subset=columns, keep='first')
		return list(unique_bays.values)

	def get_data_count(self, df: pd.DataFrame) -> int:
		orders = df.loc[df['tag'].isin(params.ORDER_TAG)]
		return super().get_data_count(orders)

	def analyze_for_key(self, df: pd.DataFrame, key: Tuple[str, str, str], **kwargs) -> List[Dict[str, Any]]:
		b1, b2, b3 = key
		df_bay = df[
			(df['b1']==b1) &
			(df['b2']==b2) &
			(df['b3']==b3)
		]
		order_indexes = df_bay.loc[df_bay['tag'].isin(params.ORDER_TAG)].index.values
		his_update: Dict[Cell, List[str]] = dict()
		sts_quality = TSDQuality()
		rcd_datas: List[RCEventModel] = list()
		# Store RC repetition
		buffer1: Dict[KeyPair, List[int]] = dict()
		buffer2: Dict[KeyPair, List[int]] = dict()
		queue: Queue = kwargs.get('queue')

		# No RCD for this bay, skip check
		if len(order_indexes)==0:
			return rcd_datas

		date_origin = df_bay.loc[order_indexes[0], 'timestamp']

		def lappend(list_: List, value: Any):
			"""Lookup before append to list."""
			if value not in list_:
				list_.append(value)

		def set_mark_repetition(elm_status: Tuple[str, str]):
			for m in range(len(buffer1[elm_status])):
				if m==len(buffer1[elm_status])-1:
					# End of repetition
					comment_text = f'Percobaan RC ke-{m+1} (terakhir)'
				else:
					comment_text = f'Percobaan RC ke-{m+1}'
					# Set repetition mark
					rcd_datas[buffer2[elm_status][m]].rep_flag = '*'

				rcd_datas[buffer2[elm_status][m]].annotations.insert(0, comment_text)

				if (buffer1[elm_status][m], 'comment') in his_update:
					his_update[(buffer1[elm_status][m], 'comment')].append(comment_text)
				else:
					his_update[(buffer1[elm_status][m], 'comment')] = [comment_text]

		for x, idx in enumerate(order_indexes):
			idx_order = idx
			row_order: pd.Series = df_bay.loc[idx]
			t_order, t_tx = get_datetime(row_order)
			t_fback = t_tx
			elm, sts, tag, opr = df_bay.loc[idx, ['element', 'status', 'tag', 'operator']]
			ix_result = idx
			note_cd_anomaly = 'Status CD anomali'
			note_lr_anomaly = 'Status LR anomali'
			note_cso_anomaly = 'Status CSO anomali'
			note_time_anomaly = 'Anomali timestamp RTU'
			soe_comment = str(row_order['comment']).split('\n')
			data = RCEventModel(
				order_time=t_order,
				feedback_time=t_fback,
				b1=b1,
				b2=b2,
				b3=b3,
				element=elm,
				status=sts,
				tag=tag,
				operator=opr,
				execution=0,
				termination=0,
				txrx=0,
				navigation=(idx_order, ix_result)
			)

			if queue is not None:
				qmsg = QueueMessage(message=f'Memeriksa RC {data.status} {data.element} bay {data.b1}/{data.b2}/{data.b3} pada {data.order_time}')
				queue.put(qmsg)

			# Check IFS before RC
			pre_rc_rtu_st, pre_rc_rtu_name = self._check_rtu_status(row_order)
			if pre_rc_rtu_st=='Down':
				# RC ordered at OutPoll condition, annotate event
				pre_rc_rtu_note = f'RC dalam kondisi IFS "{pre_rc_rtu_name}" Down'
				data.annotations.append(pre_rc_rtu_note)
				soe_comment.append(pre_rc_rtu_note)
			else:
				# RC (assumed) ordered at InPoll condition, skip
				pass

			# Check LR before RC
			pre_rc_lr_st, pre_rc_lr_q = self._check_remote_status(row_order)
			if pre_rc_lr_q=='good' and pre_rc_lr_st=='Local':
				# RC ordered at Local position, annotate event
				pre_rc_lr_note = 'Status LR Local'
				data.annotations.append(pre_rc_lr_note)
				lappend(soe_comment, pre_rc_lr_note)
			elif pre_rc_lr_q=='bad':
				# LR quality is bad, annotate event
				data.annotations.append(note_lr_anomaly)
				lappend(soe_comment, note_lr_anomaly)
			else:
				# LR status uncertain, skip
				pass

			# Check CD before RC
			pre_rc_cd_st, pre_rc_cd_q = self._check_enable_status(row_order)
			if pre_rc_cd_q=='good' and pre_rc_cd_st=='Disable':
				# RC ordered at Disabled CD status, annotate event
				pre_rc_cd_note = 'Status CD Disable'
				data.annotations.append(pre_rc_cd_note)
				lappend(soe_comment, pre_rc_cd_note)
			elif pre_rc_cd_q=='bad':
				# CD quality is bad, annotate event
				data.annotations.append(note_cd_anomaly)
				lappend(soe_comment, note_cd_anomaly)
			else:
				# CD status uncertain, skip
				pass

			# Check CSO and protection on RC Close only
			active_prot = ''
			if sts=='Close':
				# Check CSO before RC
				pre_rc_cso_st, pre_rc_cso_q = self._check_synchro_interlock(row_order)
				if pre_rc_cso_q=='good':
					# Annotate CSO status
					txt_rc_at_cso = f'Status CSO {pre_rc_cso_st}'
					data.annotations.append(txt_rc_at_cso)
					lappend(soe_comment, txt_rc_at_cso)
				elif pre_rc_cso_q=='bad':
					# CSO quality is bad, annotate event
					data.annotations.append(note_cso_anomaly)
					lappend(soe_comment, note_cso_anomaly)
				else:
					# CSO status uncertain, skip
					pass

				# Check active protection before RC
				active_prot = self._check_protection_interlock(row_order)
				if active_prot:
					# Any protection active when RC ordered
					pre_rc_prt = f'Proteksi {active_prot} sedang aktif'
					data.annotations.append(pre_rc_prt)
					lappend(soe_comment, pre_rc_prt)

			# Sampling dataframe for condition :
			# 	>= current order time
			# 	<= current order time + t_search time
			# 	same element
			# 	is valid status
			df_range = df_bay[
				(join_datetime(df_bay['system_timestamp'], df_bay['system_ms'])>=t_order) &
				(join_datetime(df_bay['system_timestamp'], df_bay['system_ms'])<=join_datetime(t_order, self.config.t_search*1000)) &
				(df_bay['element']==elm) &
				(df_bay['status'].isin(params.CSW_STATUS))
			]
			df_result = df_range[
				(df_range['status']==sts) &
				(df_range['tag'].isin(params.FEEDBACK_TAG))
			]
			if df_result.shape[0]>0:
				# Continue check feedback
				row_result = df_result.iloc[0]
			else:
				# Check potential feedback with leading timestamp
				# Likely happened in anomaly timesync
				df_range_before = df_bay[
					(join_datetime(df_bay['system_timestamp'], df_bay['system_ms'])<t_order) &
					(join_datetime(df_bay['system_timestamp'], df_bay['system_ms'])>=join_datetime(t_order, -self.config.t_search*1000)) &
					(df_bay['element']==elm) &
					(df_bay['status'].isin(params.CSW_STATUS))
				]
				df_result = df_range_before[
					(df_range_before['status']==sts) &
					(df_range_before['tag'].isin(params.FEEDBACK_TAG))
				]
				if df_result.shape[0]>0:
					# df_range = df_range_before
					# Get latest feedback
					row_result = df_result.iloc[-1]
				else:
					# df_range = None
					row_result = None

			if isinstance(row_result, pd.Series):
				# RC feedback found
				ix_result = row_result.name
				if 'R' in row_result['tag']:
					rc_result = 'SUCCESS'
				else:
					rc_result = 'FAILED'

				t_fback, t_rx = get_datetime(row_result)
				t_exec = get_execution_duration(row_order, row_result)
				t_term = get_termination_duration(row_order, row_result)
				t_txrx = t_term - t_exec
				last_index = ix_result
				data.feedback_time = t_fback
				data.execution = t_exec
				data.termination = t_term
				data.txrx = t_txrx
				data.navigation = (idx_order, ix_result)

				# Check if feedback time leading order time
				if t_exec<0 or t_txrx<0:
					data.annotations.append(note_time_anomaly)
					soe_result_comment = str(row_result['comment']).split('\n')
					lappend(soe_result_comment, note_time_anomaly)

					if (ix_result, 'comment') in his_update:
						his_update[(ix_result, 'comment')].append(soe_result_comment)
					else:
						his_update[(ix_result, 'comment')] = [soe_result_comment]
			else:
				# Cut operation if no feedback found
				# Return order index with status UNCERTAIN
				rc_result = 'UNCERTAIN'
				last_index = idx_order

			# Operations if RC is failed
			if rc_result=='FAILED':
				# Check IFS after RC
				if pre_rc_rtu_st=='Up':
					post_rc_rtu_st, post_rc_rtu_name = self._check_rtu_status(row_result)
					if post_rc_rtu_st=='Down':
						# IFS down some time after RC ordered, annotate event
						txt_rtu_after_rc = f'IFS "{post_rc_rtu_name}" Down sesaat setelah RC'
						data.annotations.append(txt_rtu_after_rc)
						soe_comment.append(txt_rtu_after_rc)

				# Check events within time range RC order until next t_search duration, only Tag [OR, O*, RC, R*, ""] would pass
				df_post_rc = df_range[
					(df_range.index>row_order.name) &
					(df_range['tag'].isin(list(params.ORDER_TAG) + ['RC', 'R*', '']))
				]

				if df_post_rc.empty:
					# No event occurs after RC order for current bay, skip
					pass
				else:
					if df_post_rc[df_post_rc['tag'].isin(params.ORDER_TAG)].empty:
						# No other RC order occured towards, continue
						pass
					else:
						# At least one RC order occured, narrow the limit range
						next_order = df_post_rc[df_post_rc['tag'].isin(params.ORDER_TAG)].iloc[0]
						df_post_rc = df_post_rc[df_post_rc.index<next_order.name]

					if df_post_rc.empty:
						# No status change occured
						pass
					else:
						if df_post_rc[df_post_rc['status'].isin(['Close', 'Open'])].empty:
							# Anomaly status occured
							first_change = df_post_rc.iloc[0]
						else:
							# Normal status Open / Close occured
							first_change = df_post_rc[df_post_rc['status'].isin(['Close', 'Open'])].iloc[0]

						ix_first_change = first_change.name
						his_comment = str(self.data.CSW.loc[ix_first_change, 'comment']).split('\n')
						note = self._get_failed_annotation(row_order, first_change)

						if ix_first_change>ix_result:
							# Change last index for navigation
							last_index = ix_first_change

						if note:
							data.annotations.append(note)
							lappend(his_comment, note)

						if (ix_first_change, 'comment') in his_update:
							his_update[(ix_first_change, 'comment')].append(note)
						else:
							his_update[(ix_first_change, 'comment')] = [note]
			else:
				# No bother check if status already SUCCESS or UNCERTAIN
				pass

			# Copy User Comment if any
			user_comment = df_range.loc[df_range.index<=last_index, 'user_comment'].to_list()
			for cmt in user_comment:
				if cmt and '**' not in cmt:
					# Eleminate unnecessary character
					txt = re.sub(r'^\W*|\s*$', '', cmt)
					data.annotations.append(txt)

			final_result = rc_result
			# Check wether RC event marked by user
			if self.config.unused_mark in df_range['user_comment'].to_list():
				data.annotations.append('User menandai RC dianulir**')
				data.marked_unused = '*'
			elif self.config.success_mark in df_range['user_comment'].to_list():
				final_result = 'SUCCESS'
				data.annotations.append('User menandai RC sukses**')
				data.marked_success = '*'
			elif self.config.failed_mark in df_range['user_comment'].to_list():
				final_result = 'FAILED'
				data.annotations.append('User menandai RC gagal**')
				data.marked_failed = '*'

			his_update[(idx_order, 'comment')] = soe_comment
			# his_update[(ix_result, 'RC Feedback')] = rc_result
			data.pre_result = rc_result
			data.final_result = final_result

			# Append data into list
			rcd_datas.append(data)

			# Repetition check starts here
			if self.config.include_repetition:
				# No need to set repetition marks
				pass
			else:
				# Check repetition
				elm_status = (elm, sts)
				if date_origin.year==t_tx.year and \
					date_origin.month==t_tx.month and \
					date_origin.day==t_tx.day:
					# If in the same day and not last iteration
					if elm_status in buffer1:
						# Element & status already in buffer, append buffer
						buffer1[elm_status] += [idx]
						buffer2[elm_status] += [x]

						if final_result=='SUCCESS':
							# Comment to mark as last RC repetition
							set_mark_repetition(elm_status)
							del buffer1[elm_status]
							del buffer2[elm_status]

					else:
						if final_result in ['FAILED', 'UNCERTAIN']:
							buffer1[elm_status] = [idx]
							buffer2[elm_status] = [x]
				else:
					for buf1key, buf1val in buffer1.items():
						if len(buf1val)>1:
							# Comment to mark as multiple RC event in 1 day
							set_mark_repetition(buf1key)

					# If dates are different, set date_origin
					date_origin = t_tx
					# Reset buffer
					buffer1.clear()
					buffer2.clear()
					# Insert into new buffer
					if final_result in ['FAILED', 'UNCERTAIN']:
						buffer1[elm_status] = [idx]
						buffer2[elm_status] = [x]

				if x==len(order_indexes)-1:
					# This is last RC event
					for buf1key, buf1val in buffer1.items():
						if len(buf1val)>1:
							set_mark_repetition(buf1key)

		return list(map(lambda x: x.dump(), rcd_datas))

	def post_analyze(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
		df_post = df.sort_values(['order_time'], ascending=[True]).reset_index(drop=True)
		return super().post_analyze(df_post, **kwargs)


class SheetRCOnly(SheetWrapper, model_class=RCEventModel):

	def generate_formula(self, sref: Optional[SheetSOE] = None, **kwargs):
		def get_row_position(index: int) -> int:
			try:
				return sref.data.index.get_loc(index) + 2
			except Exception as exc:
				logprint(f'Can not find row position of index {index}!', level='error')
				return 2

		self.header_format.text_wrap = True

		master = kwargs.get('master', 'spectrum')
		formula = dict()
		# Real excel row start with 1 + 1 row header
		range_ = range(self.row_offset + 1, self.data_rows + self.row_offset + 1)
		create_hyperlink = bool('navigation' in self.data.columns and sref is not None)

		formula_expr = {
			'execution': f'=({self.xlcell_var("feedback_time", abs_col=True)}-{self.xlcell_var("order_time", abs_col=True)})*24*3600',
			'final_result': xlf.if_(
				self.xlcell_var('marked_success', abs_col=True) + '="*"',
				'"SUCCESS"',
				xlf.if_(
					self.xlcell_var('marked_failed', abs_col=True) + '="*"',
					'"FAILED"',
					self.xlcell_var('pre_result', abs_col=True)
				),
			eq=True),
		}

		if create_hyperlink:
			xy_list = self.data['navigation'].tolist()
			iord, ifbk = tuple(zip(*xy_list))
			if master=='survalent':
				last_column = xl_col_to_name(len(sref.fields_order)-1)
				cell_order_iloc = self.xlcell_var('order_iloc')
				cell_fback_iloc = self.xlcell_var('feedback_iloc')

				formula_expr['feedback_time'] = f'=INDIRECT("\'{sref.sheet_name}\'!{sref.xlcolumn["timestamp"]}"&{cell_fback_iloc})+(INDIRECT("\'{sref.sheet_name}\'!{sref.xlcolumn["ms"]}"&{cell_fback_iloc})/86400000)'
				formula_expr['navigation'] = f'=HYPERLINK("#{sref.sheet_name}!A"&{cell_order_iloc}&":{last_column}"&{cell_fback_iloc}, "CARI >>")'

				# Directly set on formula data
				formula['order_iloc'] = list(map(get_row_position, iord))
				formula['feedback_iloc'] = list(map(get_row_position, ifbk))
			else:
				# Directly set on formula data
				formula['navigation'] = list(map(
					lambda x, y: xl_hyperlink_to_range(sref, x, y, text='CARI >>'), xy_list
				))

		for column, expr in formula_expr.items():
			formula[column] = list(map(lambda r: expr.format(row=r), range_))

		self.formula = pd.DataFrame(data=formula)

	def get_footer_data(self, **kwargs) -> pd.DataFrame:
		footer_formula = dict()
		footer_formula['operator'] = [
			'TOTAL RC (RAW)',
			'SUCCESS (RAW)',
			'FAILED (RAW)',
			'SUCCESS RATE',
		]
		footer_formula['pre_result'] = [
			xlf.counta(self.xlcolumn_range['pre_result'], eq=True),
			xlf.countif(self.xlcolumn_range['pre_result'], '"SUCCESS"', eq=True),
			xlf.countif(self.xlcolumn_range['pre_result'], '"FAILED"', eq=True),
			'=ROUND(' + xlf.div(f'${self.xlcolumn["pre_result"]}${self.data_rows+3}', f'${self.xlcolumn["pre_result"]}${self.data_rows+2}', default=0) + '*100, 2)'
		]
		return pd.DataFrame(data=footer_formula, columns=model_fieldnames(self.model_class)).fillna('')


class SheetStation(SheetWrapper, model_class=RCB1Model):

	def generate_formula(self, sref: Optional[SheetRCOnly] = None, **kwargs):
		def averageifs(xlrange: str):
			return xlf.if_(
				self.xlcell_var('success', abs_col=True) + '=0',
				'0',
				xlf.averageifs(xlrange, *rule_base, rule_success),
				eq=True
			)

		self.header_format.text_wrap = True

		# Real excel row start with 1 + 1 row header
		range_ = range(self.row_offset + 1, self.data_rows + self.row_offset + 1)

		rule_b1 = xlf.range_criteria(sref.get_xlcolumn_range('b1', isexternal=True), self.xlcell_var('b1', abs_col=True))
		rule_repetition = xlf.range_criteria(sref.get_xlcolumn_range('rep_flag', isexternal=True), '""')
		rule_unused = xlf.range_criteria(sref.get_xlcolumn_range('marked_unused', isexternal=True), '""')
		rule_success = xlf.range_criteria(sref.get_xlcolumn_range('final_result', isexternal=True), '"SUCCESS"')
		rule_failed = xlf.range_criteria(sref.get_xlcolumn_range('final_result', isexternal=True), '"FAILED"')
		rule_base = [rule_b1, rule_repetition, rule_unused]

		formula_expr = {
			'occurences':  xlf.countifs(*rule_base, eq=True),
			'success':  xlf.countifs(*rule_base, rule_success, eq=True),
			'failed':  xlf.countifs(*rule_base, rule_failed, eq=True),
			'success_rate':  xlf.div(self.xlcell_var('success', abs_col=True), self.xlcell_var('occurences', abs_col=True), default=0, eq=True),
			'execution_avg':  averageifs(sref.get_xlcolumn_range('execution', True)),
			'termination_avg':  averageifs(sref.get_xlcolumn_range('termination', True)),
			'txrx_avg':  averageifs(sref.get_xlcolumn_range('txrx', True)),
		}

		formula = dict()
		for column, expr in formula_expr.items():
			formula[column] = list(map(lambda r: expr.format(row=r), range_))

		self.formula = pd.DataFrame(data=formula)

	def get_footer_data(self, **kwargs) -> pd.DataFrame:
		row = dict()
		row['occurences'] = xlf.sum(self.xlcolumn_range['occurences'], eq=True)
		row['success'] = xlf.sum(self.xlcolumn_range['success'], eq=True)
		row['failed'] = xlf.sum(self.xlcolumn_range['failed'], eq=True)
		row['success_rate'] = xlf.div(
			f'${self.xlcolumn["success"]}${self.data_rows+2}',
			f'${self.xlcolumn["occurences"]}${self.data_rows+2}',
			default=0,
			eq=True
		)
		return pd.DataFrame(data=[row], columns=model_fieldnames(self.model_class)).fillna('')


class SheetBay(SheetWrapper, model_class=RCB3Model):

	def generate_formula(self, sref: Optional[SheetRCOnly] = None, **kwargs):
		self.header_format.text_wrap = True

		range_ = range(self.row_offset + 1, self.data_rows + self.row_offset + 1)
		cell_thd = kwargs.get('cell_threshold', '""')

		# Define excel column name for each data column
		cell_success = self.xlcell_var('success', abs_col=True)
		cell_failed = self.xlcell_var('failed', abs_col=True)
		cell_open_failed = self.xlcell_var('open_failed', abs_col=True)
		cell_open_success = self.xlcell_var('open_success', abs_col=True)
		cell_close_failed = self.xlcell_var('close_failed', abs_col=True)
		cell_close_success = self.xlcell_var('close_success', abs_col=True)
		cell_occurences = self.xlcell_var('occurences', abs_col=True)
		cell_total_occurences = f'${self.xlcolumn["occurences"]}${self.data_rows+2}'

		cell_threshold = xlf.iferror(f'VALUE({cell_thd})', 0)

		rule_b1 = xlf.range_criteria(sref.get_xlcolumn_range('b1', True), self.xlcell_var('b1', abs_col=True))
		rule_b2 = xlf.range_criteria(sref.get_xlcolumn_range('b2', True), self.xlcell_var('b2', abs_col=True))
		rule_b3 = xlf.range_criteria(sref.get_xlcolumn_range('b3', True), self.xlcell_var('b3', abs_col=True))
		rule_repetition = xlf.range_criteria(sref.get_xlcolumn_range('rep_flag', True), '""')
		rule_unused = xlf.range_criteria(sref.get_xlcolumn_range('marked_unused', True), '""')
		rule_success = xlf.range_criteria(sref.get_xlcolumn_range('final_result', True), '"SUCCESS"')
		rule_failed = xlf.range_criteria(sref.get_xlcolumn_range('final_result', True), '"FAILED"')
		rule_base = [rule_b1, rule_b2, rule_b3, rule_repetition, rule_unused]

		tag_open = xlf.if_(
			xlf.div(
				f'{cell_open_failed}^2',
				f'({cell_open_failed}+{cell_open_success})',
				default=0
			) + f'>{cell_threshold}',
			'"O"', '""'
		)
		tag_close = xlf.if_(
			xlf.div(
				f'{cell_close_failed}^2',
				f'({cell_close_failed}+{cell_close_success})',
				default=0
			) + f'>{cell_threshold}',
			'"C"', '""'
		)
		formula_expr = {
			'occurences': xlf.countifs(*rule_base, eq=True),
			'success': xlf.countifs(*rule_base, rule_success, eq=True),
			'failed': xlf.countifs(*rule_base, rule_failed, eq=True),
			'success_rate': xlf.div(cell_success, cell_occurences, default=0, eq=True),
			'contribution': xlf.div(cell_occurences, cell_total_occurences, default=0, eq=True),	# <rc occur>/<total rc occur>
			'reduction': xlf.div(cell_failed, cell_total_occurences, default=0, eq=True),	# <rc failed>/<total rc occur>
			'tagging': f'={tag_open}&{tag_close}'
		}
		for status in ('open', 'close'):
			for result in ('success', 'failed'):
				rule_status = xlf.range_criteria(sref.get_xlcolumn_range('status', True), f'"{status.title()}"')
				rule_result = xlf.range_criteria(sref.get_xlcolumn_range('final_result', True), f'"{result.upper()}"')
				formula_expr[status+'_'+result] = xlf.countifs(*rule_base, rule_status, rule_result, eq=True)

		formula = dict()
		for column, expr in formula_expr.items():
			formula[column] = list(map(lambda r: expr.format(row=r), range_))

		self.formula = pd.DataFrame(data=formula)

	def get_footer_data(self, **kwargs):
		row = dict()
		row['success_rate'] = xlf.div(
			f'${self.xlcolumn["success"]}${self.data_rows+2}',
			f'${self.xlcolumn["occurences"]}${self.data_rows+2}',
			default=0,
			eq=True
		)
		for column in ['occurences', 'success', 'failed', 'open_success', 'open_failed', 'close_success', 'close_failed', 'contribution', 'reduction']:
			row[column] = xlf.sum(self.get_xlcolumn_range(column), eq=True)

		return pd.DataFrame(data=[row], columns=model_fieldnames(self.model_class)).fillna('')


class SheetOperator(SheetWrapper, model_class=RCOperatorModel):

	def generate_formula(self, sref: Optional[SheetRCOnly] = None, **kwargs):
		self.header_format.text_wrap = True

		range_ = range(self.row_offset + 1, self.data_rows + self.row_offset + 1)

		rule_operator = xlf.range_criteria(sref.get_xlcolumn_range('operator', True), self.xlcell_var('operator', abs_col=True))
		rule_repetition = xlf.range_criteria(sref.get_xlcolumn_range('rep_flag', True), '""')
		rule_unused = xlf.range_criteria(sref.get_xlcolumn_range('marked_unused', True), '""')
		rule_success = xlf.range_criteria(sref.get_xlcolumn_range('final_result', True), '"SUCCESS"')
		rule_failed = xlf.range_criteria(sref.get_xlcolumn_range('final_result', True), '"FAILED"')
		rule_base = [rule_operator, rule_repetition, rule_unused]

		formula_expr = {
			'occurences': xlf.countifs(*rule_base, eq=True),
			'success': xlf.countifs(*rule_base, rule_success, eq=True),
			'failed': xlf.countifs(*rule_base, rule_failed, eq=True),
			'success_rate': xlf.div(self.xlcell_var('success', abs_col=True), self.xlcell_var('occurences', abs_col=True), default=0, eq=True),
		}

		formula = dict()
		for column, expr in formula_expr.items():
			formula[column] = list(map(lambda r: expr.format(row=r), range_))

		self.formula = pd.DataFrame(data=formula)

	def get_footer_data(self, **kwargs):
		row = dict()
		row['occurences'] = xlf.sum(self.xlcolumn_range['occurences'], eq=True)
		row['success'] = xlf.sum(self.xlcolumn_range['success'], eq=True)
		row['failed'] = xlf.sum(self.xlcolumn_range['failed'], eq=True)
		row['success_rate'] = xlf.div(
			f'${self.xlcolumn["success"]}${self.data_rows+2}',
			f'${self.xlcolumn["occurences"]}${self.data_rows+2}',
			default=0,
			eq=True
		)
		return pd.DataFrame(data=[row], columns=model_fieldnames(self.model_class)).fillna('')


class RCD(Availability):
	config: RCDConfig
	result: AvRCDResult

	def init_core(self) -> RCDCore:
		return RCDCore(self.config)

	def get_result(self, df: pd.DataFrame, start_date: Optional[DateSpec] = None, end_date: Optional[DateSpec] = None) -> AvRCDResult:
		if isinstance(start_date, datetime.datetime) and isinstance(end_date, datetime.datetime):
			pass
		elif isinstance(start_date, datetime.date) and isinstance(end_date, datetime.date):
			start_date = datetime.datetime.combine(start_date, datetime.time())
			end_date = datetime.datetime.combine(end_date, datetime.time(23, 59, 59, 999999))
		else:
			columns = ['order_time', 'feedback_time']
			# Get date min and max from dataframe
			start_date = df[columns].min(axis=1).min(axis=0).to_pydatetime()
			end_date = df[columns].max(axis=1).max(axis=0).to_pydatetime()

		df_filter = df[(df['order_time']>=start_date) & (df['order_time']<=end_date)]
		rcd_data = RCDData(df_filter, start_date=start_date, end_date=end_date, config=self.config)
		return AvRCDResult(data=rcd_data)

	def post_read_file(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
		columns = df.columns.tolist()
		# Remove navigation column, not used in cummulative loads
		try_remove(columns, 'navigation')
		if self.config.master=='survalent':
			try_remove(columns, 'order_iloc')
			try_remove(columns, 'feedback_iloc')

		# Exclude footer data
		df_rc = df[~(df['order_time'].isna()) & ~(df['feedback_time'].isna())]
		df_rc = df_rc[columns]\
			.fillna('')\
			.reset_index(drop=True)
		self.data = df_rc
		return super().post_read_file(df_rc, **kwargs)

	def get_xlsheet(self, **infokwargs) -> Dict[str, SheetWrapper]:
		sheets: List[SheetWrapper] = list()
		sheet1 = None
		filename = infokwargs.get('filename', '<undefined>')

		if isinstance(self.core.data, SOEData):
			sheet1 = SheetSOE('HIS_MESSAGES', data=self.core.data.his, master=self.config.master)
			sheets.append(sheet1)

		sheet2 = SheetRCOnly('RC_ONLY', data=self.result.data.all, master=self.config.master)
		sheet3 = SheetStation('GI', data=self.result.data.station, master=self.config.master)
		sheet4 = SheetBay('BAY', data=self.result.data.bay, master=self.config.master)
		sheet5 = SheetOperator('DISPATCHER', data=self.result.data.operator, master=self.config.master)
		sheet6 = SheetInfo(
			'Info',
			info_data=[
				SheetInfo.sub_title('SETTING'),
				('RC Element', ', '.join(self.config.elements)),
				('Repetition', 'Included' if self.config.include_repetition else 'Excluded'),
				('Threshold', self.config.reduction_ratio_threshold),
				SheetInfo.blank_row(),
				SheetInfo.sub_title('SUMMARY'),
				('Success Rate', f'=ROUND({sheet4.xlcell(sheet4.data_rows+1, "success", isexternal=True)}/{sheet4.xlcell(sheet4.data_rows+1, "occurences", isexternal=True)}*100,2)&"%"'),
				('Success Rate (Close)', f'=ROUND({sheet4.xlcell(sheet4.data_rows+1, "close_success", isexternal=True)}/({sheet4.xlcell(sheet4.data_rows+1, "close_success", isexternal=True)}+{sheet4.xlcell(sheet4.data_rows+1, "close_failed", isexternal=True)})*100,2)&"%"'),
				('Success Rate (Open)', f'=ROUND({sheet4.xlcell(sheet4.data_rows+1, "open_success", isexternal=True)}/({sheet4.xlcell(sheet4.data_rows+1, "open_success", isexternal=True)}+{sheet4.xlcell(sheet4.data_rows+1, "open_failed", isexternal=True)})*100,2)&"%"'),
				SheetInfo.blank_row(),
				SheetInfo.sub_title('STATISTICS'),
				('Total RC', xlf.counta(sheet2.get_xlcolumn_range('tag', isexternal=True), eq=True)),
				('Total Valid RC', f'={sheet4.xlcell(sheet4.data_rows+1, "occurences", isexternal=True)}'),
				('Total Repetition', xlf.counta(sheet2.get_xlcolumn_range('rep_flag', isexternal=True), eq=True)),
				('Marked', xlf.sum(xlf.counta(sheet2.get_xlcolumn_range("marked_unused", isexternal=True)), xlf.counta(sheet2.get_xlcolumn_range("marked_success", isexternal=True)), xlf.counta(sheet2.get_xlcolumn_range("marked_failed", isexternal=True)), eq=True)),
				('Unused-marked', xlf.counta(sheet2.get_xlcolumn_range('marked_unused', isexternal=True), eq=True)),
				('Success-marked', xlf.counta(sheet2.get_xlcolumn_range('marked_success', isexternal=True), eq=True)),
				('Failed-marked', xlf.counta(sheet2.get_xlcolumn_range('marked_failed', isexternal=True), eq=True)),
				('Tag Recommendations', xlf.countif_or(sheet4.get_xlcolumn_range('tagging', isexternal=True), 'O', 'C', 'OC', eq=True)),
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
		sheet2.generate_formula(sheet1, master=self.config.master)
		sheet3.generate_formula(sheet2)
		sheet4.generate_formula(sheet2, cell_threshold=sheet6.xlcell(12, 'value', isexternal=True))
		sheet5.generate_formula(sheet2)
		sheets.extend([sheet2, sheet3, sheet4, sheet5, sheet6])
		return {sheet.sheet_name: sheet for sheet in sheets}

	def get_properties(self) -> Dict[str, str]:
		return super().get_properties(title='Keberhasilan Remote Control SCADA')

