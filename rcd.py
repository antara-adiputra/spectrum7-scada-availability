import asyncio, datetime, os, re, time
from concurrent.futures import ProcessPoolExecutor, as_completed
from io import BytesIO
from types import MappingProxyType
from typing import Any, Dict, List, Callable, Iterable, Optional, Tuple, TypeAlias, Union

import config
import numpy as np
import pandas as pd
from xlsxwriter.utility import xl_col_to_name
from core import BaseAvailability, XLSExportMixin
from filereader import RCFileReader, SpectrumFileReader, SurvalentFileReader
from global_parameters import RCD_BOOK_PARAM, RCD_COLUMNS
from lib import ProcessError, calc_time, get_datetime, get_execution_duration, get_termination_duration, immutable_dict, join_datetime, nested_dict, progress_bar
from ofdb import SpectrumOfdbClient
from test import *
from worker import run_cpu_bound


FilePaths: TypeAlias = List[str]
FileDict: TypeAlias = Dict[str, BytesIO]
CalcResult : TypeAlias = Dict[str, Dict[str, Any]]
Cell: TypeAlias = Tuple[int, str]
CellUpdate: TypeAlias = Dict[Cell, Any]
KeyPair: TypeAlias = Tuple[str, str]
ListLikeDataFrame: TypeAlias = List[Dict[str, Any]]
SOEBay: TypeAlias = Union[Iterable[str], Tuple[str, str, str]]


class _Export(XLSExportMixin):
	_sheet_parameter: MappingProxyType[str, dict[str, Any]] = immutable_dict(RCD_BOOK_PARAM)
	rc_element: List[str]
	check_repetition: bool
	result: Dict[str, Dict[str, Any]]
	reduction_ratio_threshold: int
	output_prefix: str = 'RCD'

	def get_sheet_info_data(self, **kwargs):
		"""Define extra information into sheet "Info"."""
		info_data = super().get_sheet_info_data(**kwargs)
		extra_info = [
			*info_data,
			('', ''),
			('SETTING', ''),
			('RC Element', ', '.join(self.rc_element)),
			('RC Repetition', 'last-occurrence-only' if self.check_repetition else 'calculate-all'),
			('Threshold (default=1)', self.reduction_ratio_threshold),
			('', ''),
			('SUMMARY', ''),
			('Success Percentage', self.result['overall']['percentage']),
			('Success Percentage (Close)', nested_dict(self.result, ['statistic', 'operation', 'close_success_percentage'])),
			('Success Percentage (Open)', nested_dict(self.result, ['statistic', 'operation', 'open_success_percentage'])),
			('', ''),
			('STATISTICS', ''),
			('Marked', nested_dict(self.result, ['statistic', 'marked', 'total'])),
			('Unused-marked', nested_dict(self.result, ['statistic', 'marked', 'unused'])),
			('Success-marked', nested_dict(self.result, ['statistic', 'marked', 'success'])),
			('Failed-marked', nested_dict(self.result, ['statistic', 'marked', 'failed']))
		]

		return extra_info


class _SOEAnalyzer(BaseAvailability):
	"""Base class for analyze IFS from SOE.

	Args:
		data : SOE data

	Accepted kwargs:
		**
	"""
	_highest_index: int
	_lowest_index: int
	_feedback_tags: Tuple[str] = ('RC', 'NE', 'R*', 'N*')
	_order_tags: Tuple[str] = ('OR', 'O*')
	t_monitor: Dict[str, int] = {'CB': 15, 'BI1': 30, 'BI2': 30}
	t_transition: Dict[str, int] = {'CB': 1, 'BI1': 16, 'BI2': 16}
	t_search: int = 3*60*60
	success_mark: str = '**success**'
	failed_mark: str = '**failed**'
	unused_mark: str = '**unused**'
	soe_all: pd.DataFrame

	def __init__(self, data: Optional[pd.DataFrame] = None, calculate_bi: bool = False, check_repetition: bool = True, **kwargs):
		self._analyzed: bool = False
		self._is_valid: bool = False
		self.rc_element: List[str] = ['CB']
		self.check_repetition: bool = check_repetition
		self.soe_all = data
		if calculate_bi: self.rc_element += ['BI1', 'BI2']
		super().__init__(**kwargs)

	def initialize(self) -> None:
		"""Set class attributes into intial value"""
		self.init_analyze()
		super().initialize()

	def init_analyze(self) -> None:
		"""Set class attributes into intial value"""
		self._analyzed = False
		self._is_valid = False
		self.cd_qualities = dict()
		self.cso_qualities = dict()
		self.lr_qualities = dict()

	def soe_setup(self, **kwargs) -> Union[pd.DataFrame, Exception]:
		"""Apply sorting and filtering on "soe_all" to get cleaned data."""
		start = kwargs.get('start')
		stop = kwargs.get('stop')

		if isinstance(self.soe_all, pd.DataFrame):
			self._is_valid = True
			# Can be filtered with date
			if isinstance(start, datetime.datetime) and isinstance(stop, datetime.datetime):
				t0, t1 = self.set_range(start=start, stop=stop)
			else:
				# Parameter start or stop is not valid datetime or None
				t0, t1 = self.set_range(start=self.soe_all['Time stamp'].min(), stop=self.soe_all['Time stamp'].max())

			df = self.soe_all.copy()
			df = df[(df['Time stamp']>=t0) & (df['Time stamp']<=t1)].sort_values(['System time stamp', 'System milliseconds', 'Time stamp', 'Milliseconds'], ascending=[True, True, True, True]).reset_index(drop=True)
			orders = self.get_orders()
			# Get min index and max index of df
			self._lowest_index, self._highest_index = df.index.min(), df.index.max()
			soe_ifs = df[(df['A']=='') & (df['B1']=='IFS') & (df['B2']=='RTU_P1') & (df['Tag']=='')]
			# Get IFS name matching
			self.ifs_list = soe_ifs['B3'].unique()
			self.b1_list, self.b2_list, self.b3_list = orders['B1'].unique(), orders['B2'].unique(), orders['B3'].unique()
			# Filter IFS messages only if related to RC Event
			self.soe_ifs = soe_ifs[soe_ifs['B3'].isin(self.b1_list)]
			# Filter His. Messages only if related to RC Event's B1, B2 and B3
			# df = df[(df['A']=='') & (df['B1'].isin(self.b1_list)) & (df['B2'].isin(self.b2_list)) & (df['B3'].isin(self.b3_list))]
			soe_by_bay = df[(df['A']=='') & (df['B1'].isin(self.b1_list)) & (df['B2'].isin(self.b2_list)) & (df['B3'].isin(self.b3_list))]
			# Reset comment column and search for order tag for RC element
			soe_ctrl = soe_by_bay[(soe_by_bay['Element'].isin(self.rc_element)) & (soe_by_bay['Status'].isin(['Open', 'Close', 'Dist.']))].copy()
			soe_ctrl['RC Order'] = np.where((soe_ctrl['Element'].isin(self.rc_element)) & (soe_ctrl['Tag'].isin(self.order_tags)), 'REMOTE', '')
			soe_ctrl['RC Feedback'] = ''
			soe_ctrl['Comment'] = ''
			# Split into DataFrames for each purposes, not reset index
			self.soe_ctrl = soe_ctrl
			self.soe_cd = soe_by_bay[soe_by_bay['Element']=='CD'].copy()
			self.soe_lr = soe_by_bay[soe_by_bay['Element']=='LR'].copy()
			self.soe_sync = soe_by_bay[soe_by_bay['Element']=='CSO'].copy()
			self.soe_prot = soe_by_bay[soe_by_bay['Element'].isin(['CBTR', 'MTO'])].copy()
			return df
		else:
			raise ProcessError('SOEAnalyzeError', 'Input data tidak valid.', f'soe_all={type(self.soe_all)}')

	def get_rcd_result(self, df: pd.DataFrame, bay: SOEBay) -> Tuple[ListLikeDataFrame, CellUpdate]:
		"""Analyze all RCD events.

		Args:
			df : dataframe source
			bay : couple of bay name

		Result:
			List of dict-like RC information and dict of values tobe updated in SOE
		"""
		b1, b2, b3 = bay
		df_bay: pd.DataFrame = df[(df['B1']==b1) & (df['B2']==b2) & (df['B3']==b3)]
		iorder_list: np.ndarray = df_bay.loc[df_bay['Tag'].isin(self.order_tags)].index.values
		bay_rcd: ListLikeDataFrame = list()
		soe_upd: CellUpdate = dict()
		buffer1: Dict[KeyPair, List[int]] = dict()
		buffer2: Dict[KeyPair, List[int]] = dict()
		# No RCD for this bay, skip check
		if len(iorder_list)==0: return bay_rcd
		date_origin = df_bay.loc[iorder_list[0], 'Time stamp']

		def find_status(iorder: int, ifback: int, istatus: int, status_anomaly: bool = False):
			row_order = df_bay.loc[iorder]
			row_status = df_bay.loc[istatus]
			t_delta = get_execution_duration(row_order, row_status)
			t_exec = join_datetime(row_status['Time stamp'], row_status['Milliseconds'])
			rc_elm = row_order['Element']
			rc_sts = row_order['Status']
			lr_st, lr_q = self.check_remote_status(t1=t_exec, b1=b1, b2=b2, b3=b3)
			cd_st, cd_q = self.check_enable_status(t1=t_exec, b1=b1)
			st_list = list()
			txt_res = ''
			idx_last = None
			comment = self.soe_ctrl.loc[istatus, 'Comment'].split('\n')

			if cd_q=='good': st_list.append(f'CD={cd_st}')

			if rc_sts=='Close':
				cso_st, cso_q = self.check_synchro_interlock(t1=t_exec, b1=b1, b2=b2, b3=b3)
				prot = self.check_protection_interlock(t1=t_exec, b1=b1, b2=b2, b3=b3)

				if cso_q=='good': st_list.append(f'CSO={cso_st}')

				if prot: st_list.append(f'{prot}=Appeared')

			txt_sts = f'({", ".join(st_list)})' if len(st_list)>0 else ''

			if row_status['Status']==rc_sts or status_anomaly:
				# Valid status or anomaly
				if lr_st=='Remote' and t_delta<=self.t_monitor[rc_elm]:
					txt_res = f'Potensi RC sukses{" tapi status " + rc_sts + " anomali" if status_anomaly else ""} ({t_delta}s){" " + txt_sts if txt_sts else ""}'
				else:
					txt_res = f'Eksekusi lokal GI{" tapi status " + rc_sts + " anomali" if status_anomaly else ""}{" " + txt_sts if txt_sts else ""}'
			else:
				# Inverted status occured
				if lr_st=='Remote' and t_delta<=self.t_monitor[rc_elm]:
					txt_res = f'RC {rc_sts} tapi status balikan {row_status["Status"]}. Perlu ditelusuri!'
				# else:
				# 	anomaly_status = True

			if istatus>ifback: idx_last = istatus

			if txt_res and txt_res not in comment:
				comment.append(txt_res)
				# self.soe_ctrl.loc[istatus, 'Comment'] = '\n'.join(comment)
			
			if (istatus, 'Comment') in soe_upd:
				soe_upd[(istatus, 'Comment')].append(comment)
			else:
				soe_upd[(istatus, 'Comment')] = comment

			return idx_last, txt_res

		def annotate_repetition(key: Tuple):
			for m in range(len(buffer1[key])):
				if m==len(buffer1[key])-1:
					comment_text = f'Percobaan RC ke-{m+1} (terakhir)'
				else:
					comment_text = f'Percobaan RC ke-{m+1}'
					# Give flag
					bay_rcd[buffer2[key][m]]['Rep. Flag'] = '*'

				bay_rcd[buffer2[key][m]]['Annotations'].insert(0, comment_text)

				if (buffer1[key][m], 'Comment') in soe_upd:
					soe_upd[(buffer1[key][m], 'Comment')].append(comment_text)
				else:
					soe_upd[(buffer1[key][m], 'Comment')] = [comment_text]

		for x, idx in enumerate(iorder_list):
			idx_order = idx
			t_order, t_tx = get_datetime(df_bay.loc[idx])
			t_feedback = t_tx
			elm, sts, tag, opr = df_bay.loc[idx, ['Element', 'Status', 'Tag', 'Operator']]
			idx_result = idx
			txt_cd_anomaly = 'Status CD anomali'
			txt_lr_anomaly = 'Status LR anomali'
			txt_ts_anomaly = 'Anomali timestamp RTU'
			soe_comment = self.soe_ctrl.loc[idx, 'Comment'].split('\n')
			data = {
				'Order Time': t_order,
				'Feedback Time': t_feedback,
				'B1': b1,
				'B2': b2,
				'B3': b3,
				'Element': elm,
				'Status': sts,
				'Tag': tag,
				'Operator': opr,
				'Pre Result': '',
				'Execution (s)': 0,
				'Termination (s)': 0,
				'TxRx (s)': 0,
				'Rep. Flag': '',
				'Marked Unused': '',
				'Marked Success': '',
				'Marked Failed': '',
				'Final Result': '',
				'Annotations': list(),
				'Navigation': (idx_order, idx_result)
			}
			rc_result = 'UNCERTAIN'
			active_prot = ''

			# Check IFS before RC
			ifs_status_0, ifs_name_0 = self.check_ifs_status(t1=t_order, b1=b1)
			if ifs_status_0=='Down':
				txt_ifs_before_rc = f'RC dalam kondisi IFS "{ifs_name_0}" {ifs_status_0}'
				data['Annotations'].append(txt_ifs_before_rc)
				soe_comment.append(txt_ifs_before_rc)

			# Check LR before RC
			lr_status_0, lr_quality_0 = self.check_remote_status(t1=t_order, b1=b1, b2=b2, b3=b3)
			if lr_quality_0=='good' and lr_status_0=='Local':
				txt_rc_at_local = f'Status LR {lr_status_0}'
				data['Annotations'].append(txt_rc_at_local)
				if txt_rc_at_local not in soe_comment: soe_comment.append(txt_rc_at_local)
			elif lr_quality_0=='bad':
				data['Annotations'].append(txt_lr_anomaly)
				if txt_lr_anomaly not in soe_comment: soe_comment.append(txt_lr_anomaly)

			# Check CD before RC
			cd_status_0, cd_quality_0 = self.check_enable_status(t1=t_order, b1=b1)
			if cd_quality_0=='good' and cd_status_0=='Disable':
				txt_rc_at_disable = f'Status CD {cd_status_0}'
				data['Annotations'].append(txt_rc_at_disable)
				if txt_rc_at_disable not in soe_comment: soe_comment.append(txt_rc_at_disable)
			elif cd_quality_0=='bad':
				data['Annotations'].append(txt_cd_anomaly)
				if txt_cd_anomaly not in soe_comment: soe_comment.append(txt_cd_anomaly)

			# Check CSO & Protection before RC
			if sts=='Close':
				cso_status_0, cso_quality_0 = self.check_synchro_interlock(t1=t_order, b1=b1, b2=b2, b3=b3)
				if cso_quality_0=='good':
					txt_rc_at_cso = f'Status CSO {cso_status_0}'
					data['Annotations'].append(txt_rc_at_cso)
					if txt_rc_at_cso not in soe_comment: soe_comment.append(txt_rc_at_cso)
				elif cso_quality_0=='bad':
					txt_cso_anomaly = 'Status CSO anomali'
					data['Annotations'].append(txt_cso_anomaly)
					if txt_cso_anomaly not in soe_comment: soe_comment.append(txt_cso_anomaly)

				active_prot = self.check_protection_interlock(t1=t_order, b1=b1, b2=b2, b3=b3)
				if active_prot:
					txt_prot_active = f'Proteksi {active_prot} sedang aktif'
					data['Annotations'].append(txt_prot_active)
					if txt_prot_active not in soe_comment: soe_comment.append(txt_prot_active)

			# Sampling dataframe within t_search time
			df_range = df_bay[(join_datetime(df_bay['System time stamp'], df_bay['System milliseconds'])>=t_order) & (join_datetime(df_bay['System time stamp'], df_bay['System milliseconds'])<=join_datetime(t_order, self.t_search*1000)) & (df_bay['Element']==elm)]

			# Get first feedback
			df_result = df_range[(df_range['Status']==sts) & (df_range['Tag'].isin(self.feedback_tags))][:1]

			if df_result.shape[0]>0:
				# Continue check feedback
				row_result = df_result.iloc[0]
				idx_result = row_result.name

				if 'R' in row_result['Tag']:
					rc_result = 'SUCCESS'
				else:
					rc_result = 'FAILED'

				t_feedback, t_receive = get_datetime(row_result)
				t_exec = get_execution_duration(df_bay.loc[idx], row_result)
				t_term = get_termination_duration(df_bay.loc[idx], row_result)
				t_txrx = t_term - t_exec
				idx_last = idx_result
				data['Feedback Time'] = t_feedback
				data['Execution (s)'] = t_exec
				data['Termination (s)'] = t_term
				data['TxRx (s)'] = t_txrx
				data['Navigation'] = (idx_order, idx_result)

				# Check if t_feedback leading t_order
				if t_exec<0 or t_txrx<0:
					data['Annotations'].append(txt_ts_anomaly)
					row_result_comment = self.soe_ctrl.loc[idx_result, 'Comment'].split('\n')

					if txt_ts_anomaly not in row_result_comment: row_result_comment.append(txt_ts_anomaly)

					if (idx_result, 'Comment') in soe_upd:
						soe_upd[(idx_result, 'Comment')].append(row_result_comment)
					else:
						soe_upd[(idx_result, 'Comment')] = row_result_comment
			else:
				# Cut operation if no feedback found
				# Return order index with status UNCERTAIN
				idx_last = idx_order

			final_result = rc_result

			if rc_result=='FAILED':
				status_anomaly = False
				no_status_changes = False

				# Check IFS after RC
				if ifs_status_0=='Up':
					ifs_status1, ifs_name1 = self.check_ifs_status(t1=t_feedback, b1=b1)
					if ifs_status1=='Down':
						txt_ifs_after_rc = f'IFS "{ifs_name1}" {ifs_status1} sesaat setelah RC'
						data['Annotations'].append(txt_ifs_after_rc)
						soe_comment.append(txt_ifs_after_rc)

				# Only Tag [OR, O*, RC, R*, ""] would pass
				df_failed = df_range[(join_datetime(df_range['System time stamp'], df_range['System milliseconds'])>t_order) & ((df_range['Tag'].isin(list(self.order_tags) + ['RC', 'R*'])) | (df_range['Tag']==''))]

				# Check for normal status occurences
				if df_failed[(df_failed['Status'].isin(['Close', 'Open'])) & (df_failed['Tag']=='')].shape[0]>0:
					df_status_normal = df_failed[df_failed['Status'].isin(['Close', 'Open'])]
					first_change = df_status_normal.iloc[0]

					if first_change['Tag']=='':
						# Status changes after RC order
						ind, note = find_status(idx_order, idx_result, first_change.name, status_anomaly)

						if ind is not None: idx_last = ind

						if note: data['Annotations'].append(note)
					else:
						# Another RC order tag
						no_status_changes = True
				else:
					status_anomaly = True

				# Check for anomaly status occurences
				if (no_status_changes or status_anomaly) and df_failed[df_failed['Status']=='Dist.'].shape[0]>0:
					isfeedback = True
					first_dist_change = df_failed[df_failed['Status']=='Dist.'].iloc[0]
					# Sampling for next order
					df_next_order = df_failed[df_failed['Tag'].isin(self.order_tags)]

					if df_next_order.shape[0]>0:
						# Check if dist. status occured after another RC order
						if df_next_order.iloc[0].name<first_dist_change.name: isfeedback = False

					if isfeedback:
						# Anomaly status occured
						ind, note = find_status(idx_order, idx_result, first_dist_change.name, status_anomaly)

						if ind is not None: idx_last = ind

						if note: data['Annotations'].append(note)

			# Copy User Comment if any
			user_comment = df_range.loc[df_range.index<=idx_last, 'User comment'].to_list()
			for cmt in user_comment:
				if cmt and '**' not in cmt:
					# Eleminate unnecessary character
					txt = re.sub('^\W*|\s*$', '', cmt)
					data['Annotations'].append(txt)

			# Event marked by user
			if self.unused_mark in df_range['User comment'].to_list() or 'notused' in df_range['User comment'].to_list():
				data['Annotations'].append('User menandai RC dianulir**')
				data['Marked Unused'] = '*'
			elif self.success_mark in df_range['User comment'].to_list():
				final_result = 'SUCCESS'
				data['Annotations'].append('User menandai RC sukses**')
				data['Marked Success'] = '*'
			elif self.failed_mark in df_range['User comment'].to_list():
				final_result = 'FAILED'
				data['Annotations'].append('User menandai RC gagal**')
				data['Marked Failed'] = '*'

			soe_upd[(idx_order, 'Comment')] = soe_comment
			soe_upd[(idx_result, 'RC Feedback')] = rc_result
			data['Pre Result'] = rc_result
			data['Final Result'] = final_result
			# Append data into list
			bay_rcd.append(data)

			if self.check_repetition:
				# Check repetition
				key = (elm, sts)
				if date_origin.year==t_tx.year and date_origin.month==t_tx.month and date_origin.day==t_tx.day:
					# If in the same day and not last iteration
					if key in buffer1:
						# Element & status already in buffer, append buffer
						buffer1[key] += [idx]
						buffer2[key] += [x]

						if final_result=='SUCCESS':
							# Comment to mark as last RC repetition
							annotate_repetition(key)
							del buffer1[key]
							del buffer2[key]

					else:
						if final_result in ['FAILED', 'UNCERTAIN']:
							buffer1[key] = [idx]
							buffer2[key] = [x]
				else:
					for _key, _val in buffer1.items():
						if len(_val)>1: annotate_repetition(_key)	# Comment to mark as multiple RC event in 1 day

					# If dates are different, set date_origin
					date_origin = t_tx
					# Reset buffer
					buffer1.clear()
					buffer2.clear()
					# Insert into new buffer
					if final_result in ['FAILED', 'UNCERTAIN']:
						buffer1[key] = [idx]
						buffer2[key] = [x]

				if x==len(iorder_list)-1:
					for _key, _val in buffer1.items():
						if len(_val)>1: annotate_repetition(_key)

		for rcd in bay_rcd:
			rcd['Annotations'] = '\n'.join(list(map(lambda x: f'- {x}', rcd['Annotations'])))

		return bay_rcd, soe_upd

	def analyze_rcd_bays(self, df: pd.DataFrame, bays: List[SOEBay], *args, **kwargs) -> Tuple[ListLikeDataFrame, CellUpdate]:
		"""Run analyze RCD of bays.

		Args:
			df : dataframe
			bays : list of bay name

		Result:
			Pair of RC list and dict of SOE update
		"""
		rcd_list: ListLikeDataFrame = list()
		soe_dict: CellUpdate = dict()
		## Using simple synchronous program, execution improvement upto 3.3x
		## In this case, Simple synchronous program will minimize overhead compared with ThreadPoolExecutor
		for bay in bays:
			bay_rcd, soe_upd = self.get_rcd_result(df, bay)
			rcd_list += bay_rcd
			soe_dict.update(soe_upd)

		return rcd_list, soe_dict

	def analyze_rcd_multiprocess(self, df: pd.DataFrame, bays: List[SOEBay], callback: Optional[Callable] = None, *args, **kwargs) -> Tuple[ListLikeDataFrame, CellUpdate]:
		"""Run analyze with multiple Processes.

		Args:
			df : dataframe
			bays : list of bay name

		Result:
			Pair of RC list and dict of SOE update
		"""
		data_list: ListLikeDataFrame = list()
		soe_update: CellUpdate = dict()
		n = kwargs.get('nprocess', os.cpu_count())
		chunksize = kwargs.get('chunksize', len(bays)//n + 1)	# The fastest process duration proven from some tests
		if callable(callback):
			cb = callback
		else:
			cb = progress_bar
		# ProcessPoolExecutor create new instance on different processes, so modifying instance in each process will not change instance in main process. Value returned must be "serializable".
		with ProcessPoolExecutor(n) as ppe:
			futures = list()

			for i in range(0, len(bays), chunksize):
				bay_segment = bays[i:(i+chunksize)]
				future = ppe.submit(self.analyze_rcd_bays, df, bay_segment)
				futures.append(future)

			for x, future in enumerate(as_completed(futures)):
				result_list, soe_dict = future.result()
				data_list.extend(result_list)
				soe_update.update(soe_dict)
				self.progress.update(len(result_list)/self.get_order_count())
				# Call callback function
				cb(value=(x+1)/len(futures), name='analyze')

		return data_list, soe_update

	def analyze_rcd_synchronous(self, df: pd.DataFrame, bays: List[SOEBay], callback: Optional[Callable] = None, *args, **kwargs) -> Tuple[ListLikeDataFrame, CellUpdate]:
		"""Run analyze synchronously.

		Args:
			df : dataframe
			bays : list of bay name

		Result:
			Pair of RC list and dict of SOE update
		"""
		data_list: ListLikeDataFrame = list()
		soe_update: CellUpdate = dict()
		if callable(callback):
			cb = callback
		else:
			cb = progress_bar

		for x, bay in enumerate(bays):
			result_list, soe_dict = self.get_rcd_result(df, bay)
			data_list.extend(result_list)
			soe_update.update(soe_dict)
			# Call callback function
			cb(value=(x+1)/len(bays), name='analyze')

		return data_list, soe_update

	def run_analyze_with_function(self, fn: Callable, *args, **kwargs) -> pd.DataFrame:
		"""Run RC analyze with given function.

		Args:
			fn : function which will be used to run analyze

		Result:
			Dataframe of RC events
		"""
		# Pre-analyze initialization
		self.init_analyze()
		df = self.soe_setup(**kwargs)
		bay_list = self.get_bays()
		self.progress.init('Analisa RC', raw_max_value=self.get_order_count())
		# Result variable
		print(f'\nMenganalisa {self.get_order_count()} event RC...')
		# Execute given function
		data_list, soe_update = fn(df=df, bays=bay_list, *args, **kwargs)

		for cell in soe_update:
			val = soe_update[cell]

			if isinstance(val, (list, tuple)):
				self.soe_ctrl.loc[cell[0], cell[1]] = '\n'.join([s for s in val if s])
			else:
				self.soe_ctrl.loc[cell[0], cell[1]] = val

		# Create new DataFrame from list of dict data
		df_rcd = pd.DataFrame(data=data_list).sort_values(['Order Time'], ascending=[True]).reset_index(drop=True)
		self.rc_list = data_list
		self.post_process = pd.concat([self.soe_ctrl, self.soe_lr, self.soe_cd, self.soe_sync, self.soe_prot, self.soe_ifs], copy=False).drop_duplicates(keep='first').sort_values(['Time stamp', 'Milliseconds'])
		self._analyzed = True
		return df_rcd[RCD_COLUMNS + ['Navigation']]

	def fast_analyze(self, start: Optional[datetime.datetime] = None, stop: Optional[datetime.datetime] = None, *args, **kwargs) -> pd.DataFrame:
		"""Optimized RC analyze function using multiple Process.

		Args:
			start : oldest date limit
			stop : newest date limit

		Result:
			Dataframe of RC events
		"""
		return self.run_analyze_with_function(self.analyze_rcd_multiprocess, *args, start=start, stop=stop, **kwargs)

	def analyze(self, start: Optional[datetime.datetime] = None, stop: Optional[datetime.datetime] = None, *args, **kwargs) -> pd.DataFrame:
		"""Basic RC analyze function.

		Args:
			start : oldest date limit
			stop : newest date limit

		Result:
			Dataframe of RC events
		"""
		return self.run_analyze_with_function(self.analyze_rcd_synchronous, *args, start=start, stop=stop, **kwargs)

	async def async_analyze(self, start: Optional[datetime.datetime] = None, stop: Optional[datetime.datetime] = None, *args, **kwargs) -> pd.DataFrame:
		"""Asynchronous RC analyzer function using multiple Process to work concurrently.

		Args:
			start : oldest date limit
			stop : newest date limit

		Result:
			Dataframe of RC events
		"""
		# Result variable
		data_list: ListLikeDataFrame = list()
		soe_update: CellUpdate = dict()

		def done(_f: asyncio.Future):
			result_list, soe_dict = _f.result()
			data_list.extend(result_list)
			soe_update.update(soe_dict)
			self.progress.update(len(data_list)/self.get_order_count())

		# Pre-analyze initialization
		self.init_analyze()
		df = self.soe_setup(start=start, stop=stop, **kwargs)
		bay_list = self.get_bays()
		n = kwargs.get('nprocess', os.cpu_count())
		chunksize = kwargs.get('chunksize', len(bay_list)//n + 1)	# The fastest process duration proven from some tests
		self.progress.init('Menganalisa RC', raw_max_value=self.get_order_count())

		async with asyncio.TaskGroup() as tg:
			for i in range(0, len(bay_list), chunksize):
				bay_segment = bay_list[i:(i+chunksize)]
				task = tg.create_task(run_cpu_bound(self.analyze_rcd_bays, df, bay_segment))
				task.add_done_callback(done)

		for cell in soe_update:
			val = soe_update[cell]

			if isinstance(val, (list, tuple)):
				self.soe_ctrl.loc[cell[0], cell[1]] = '\n'.join([s for s in val if s])
			else:
				self.soe_ctrl.loc[cell[0], cell[1]] = val

		# Create new DataFrame from list of dict data
		df_rcd = pd.DataFrame(data=data_list).sort_values(['Order Time'], ascending=[True]).reset_index(drop=True)
		self.rc_list = data_list
		self.post_process = pd.concat([self.soe_ctrl, self.soe_lr, self.soe_cd, self.soe_sync, self.soe_prot, self.soe_ifs], copy=False)\
			.drop_duplicates(keep='first')\
			.sort_values(['Time stamp', 'Milliseconds'])
		self._analyzed = True
		return df_rcd[RCD_COLUMNS + ['Navigation']]

	def check_enable_status(self, **data) -> Tuple[str, str]:
		"""Check CD (Control Disable) status on an event parameterized in a dict (data).

		Args:
			data : must contains t1 & b1

		Result:
			CD status and CD quality
		"""
		# Initialize
		cd_status = 'Enable'
		b1 = data['b1']
		t_ord = data['t1']
		df_cd = self.soe_cd[(self.soe_cd['B1']==b1) & (self.soe_cd['Element']=='CD') & (self.soe_cd['Tag']=='')]
		# Check CD quality in program buffer
		if b1 in self.cd_qualities:
			cd_quality = self.cd_qualities[b1]
		else:
			# Do CD value check
			cd_values = df_cd['Status'].values
			if 'Enable' in cd_values and 'Disable' in cd_values:
				# CD quality good (Enable and Disable exists)
				cd_quality = 'good'
			elif ('Enable' in cd_values or 'Disable' in cd_values) and 'Dist.' in cd_values:
				# CD quality bad (Dist. or one of Enable or Disable only)
				cd_quality = 'bad'
			else:
				# CD quality unknown, no status changes occured
				cd_quality = 'uncertain'

			# Save in buffer
			self.cd_qualities[b1] = cd_quality

		if cd_quality in ['good', 'bad']:
			# If quality good, filter only valid status
			if cd_quality=='good': df_cd = df_cd[df_cd['Status'].isin(['Enable', 'Disable'])]

			if df_cd[join_datetime(df_cd['System time stamp'], df_cd['System milliseconds'])<t_ord].shape[0]>0:
				# CD status changes occured before
				cd_last_change = df_cd[join_datetime(df_cd['System time stamp'], df_cd['System milliseconds'])<t_ord].iloc[-1]
				cd_status = 'Enable' if cd_last_change['Status']=='Enable' else 'Disable'
			else:
				# CD status changes occured after
				cd_first_change = df_cd[join_datetime(df_cd['System time stamp'], df_cd['System milliseconds'])>=t_ord].iloc[0]
				cd_status = 'Disable' if cd_first_change['Status']=='Enable' else 'Enable'

		return cd_status, cd_quality

	def check_ifs_status(self, **data) -> Tuple[str, str]:
		"""Check IFS status on an event parameterized in a dict (data).

		Args:
			data : must contains t1 & b1

		Result:
			IFS status and IFS name
		"""
		# Initialize
		t_hyst = 2*60
		ifs_status = 'Up'
		# Change here if using ifs_name_matching
		ifs_name = data['b1']
		t_ord = data['t1']

		if ifs_name:
			df_ifs = self.soe_ifs[(self.soe_ifs['B1']=='IFS') & (self.soe_ifs['B2']=='RTU_P1') & (self.soe_ifs['B3']==ifs_name) & (self.soe_ifs['Tag']=='')]
			if df_ifs.shape[0]>0:
				if df_ifs[join_datetime(df_ifs['System time stamp'], df_ifs['System milliseconds'])<t_ord].shape[0]>0:
					# IFS status changes occured before
					ifs_last_change = df_ifs[join_datetime(df_ifs['System time stamp'], df_ifs['System milliseconds'])<t_ord].iloc[-1]
					ifs_status = 'Down' if ifs_last_change['Status']=='Down' else 'Up'
				else:
					# IFS status changes occured after
					ifs_first_change = df_ifs[join_datetime(df_ifs['System time stamp'], df_ifs['System milliseconds'])>=t_ord].iloc[0]
					t_delta = round((join_datetime(ifs_first_change['System time stamp'], ifs_first_change['System milliseconds'])-t_ord).total_seconds(), 1)
					if abs(t_delta)<t_hyst:
						ifs_status = f'transisi menuju Down ({t_delta}s)' if ifs_first_change['Status']=='Down' else f'transisi menuju Up ({t_delta}s)'
					else:
						ifs_status = f'Up' if ifs_first_change['Status']=='Down' else f'Down'

		return ifs_status, ifs_name

	def check_protection_interlock(self, **data) -> str:
		"""Check protection signal on an event parameterized in a dict (data).

		Args:
			data : must contains t1, b1, b2, b3

		Result:
			Active protection signal
		"""
		# Initialize
		active_prot = ''
		index = -1
		t_ord = data['t1']
		b1 = data['b1']
		b2 = data['b2']
		b3 = data['b3']
		df_prot = self.soe_prot[(self.soe_prot['Tag']=='') & (self.soe_prot['B1']==b1) & (self.soe_prot['B2']==b2) & (self.soe_prot['B3']==b3) & (self.soe_prot['Element'].isin(['CBTR', 'MTO']))]

		if df_prot[join_datetime(df_prot['System time stamp'], df_prot['System milliseconds'])<t_ord].shape[0]>0:
			# Latched protection Appeared before
			prot_last_appear = df_prot[join_datetime(df_prot['System time stamp'], df_prot['System milliseconds'])<t_ord].iloc[-1]
			if prot_last_appear['Status']=='Appeared':
				active_prot = prot_last_appear['Element']
				index = prot_last_appear.name

		return active_prot

	def check_remote_status(self, **data) -> Tuple[str, str]:
		"""Check LR (Local/Remote) status on an event parameterized in a dict (data).

		Args:
			data : must contains t1, b1, b2, b3

		Result:
			LR status and LR quality
		"""
		# Initialize
		lr_status = 'Remote'
		t_ord = data['t1']
		b1 = data['b1']
		b2 = data['b2']
		b3 = data['b3']
		df_lr = self.soe_lr[(self.soe_lr['Tag']=='') & (self.soe_lr['B1']==b1) & (self.soe_lr['B2']==b2) & (self.soe_lr['B3']==b3) & (self.soe_lr['Element']=='LR')]
		# Check LR quality in program buffer
		if (b1, b2, b3) in self.lr_qualities:
			lr_quality = self.lr_qualities[(b1, b2, b3)]
		else:
			# Do LR value check
			lr_values = df_lr['Status'].values
			if 'Remote' in lr_values and 'Local' in lr_values:
				# LR quality good (Remote and Local exists)
				lr_quality = 'good'
			elif ('Remote' in lr_values or 'Local' in lr_values) and 'Dist.' in lr_values:
				# LR quality bad (Dist. or one of Remote or Local only)
				lr_quality = 'bad'
			else:
				# LR quality unknown, no status changes occured
				lr_quality = 'uncertain'
			# Save in buffer
			self.lr_qualities[(b1, b2, b3)] = lr_quality

		if lr_quality in ['good', 'bad']:
			# If quality good, filter only valid status
			if lr_quality=='good': df_lr = df_lr[df_lr['Status'].isin(['Remote', 'Local'])]

			if df_lr[join_datetime(df_lr['System time stamp'], df_lr['System milliseconds'])<t_ord].shape[0]>0:
				# LR status changes occured before
				lr_last_change = df_lr[join_datetime(df_lr['System time stamp'], df_lr['System milliseconds'])<t_ord].iloc[-1]
				lr_status = 'Remote' if lr_last_change['Status']=='Remote' else 'Local'
			else:
				# LR status changes occured after
				lr_first_change = df_lr[join_datetime(df_lr['System time stamp'], df_lr['System milliseconds'])>=t_ord].iloc[0]
				lr_status = 'Local' if lr_first_change['Status']=='Remote' else 'Remote'

		return lr_status, lr_quality

	def check_synchro_interlock(self, **data) -> Tuple[str, str]:
		"""Check CSO (Check Synchro Override) status on an event parameterized in a dict (data).

		Args:
			data : must contains t1, b1, b2, b3

		Result:
			CSO status and CSO quality
		"""
		# Initialize
		cso_status = 'Off'
		t_ord = data['t1']
		b1 = data['b1']
		b2 = data['b2']
		b3 = data['b3']
		df1 = self.soe_sync[(self.soe_sync['Tag']=='') & (self.soe_sync['B1']==b1) & (self.soe_sync['B2']==b2) & (self.soe_sync['B3']==b3) & (self.soe_sync['Element']=='CSO')]

		if (b1, b2, b3) in self.cso_qualities:
			# Check CSO quality in program buffer
			cso_quality = self.cso_qualities[(b1, b2, b3)]
		else:
			# Do CSO value check
			cso_values = df1['Status'].values
			if 'On' in cso_values and 'Off' in cso_values:
				# CSO quality good (On and Off exists)
				cso_quality = 'good'
			elif ('On' in cso_values or 'Off' in cso_values) and 'Dist.' in cso_values:
				# CSO quality bad (Dist. or One of On or Off Only)
				cso_quality = 'bad'
			else:
				# CSO quality unknown, no status changes occured
				cso_quality = 'uncertain'
			# Save in buffer
			self.cso_qualities[(b1, b2, b3)] = cso_quality

		if cso_quality in ['good', 'bad']:
			# If quality good, filter only valid status
			if cso_quality=='good': df1 = df1[df1['Status'].isin(['On', 'Off'])]

			if df1[join_datetime(df1['System time stamp'], df1['System milliseconds'])<t_ord].shape[0]>0:
				# CSO status changes occured before
				cso_last_change = df1[join_datetime(df1['System time stamp'], df1['System milliseconds'])<t_ord].iloc[-1]
				cso_status = 'On' if cso_last_change['Status']=='On' else 'Off'
			else:
				# CSO status changes occured after
				cso_first_change = df1[join_datetime(df1['System time stamp'], df1['System milliseconds'])>=t_ord].iloc[0]
				cso_status = 'Off' if cso_first_change['Status']=='On' else 'On'

		return cso_status, cso_quality

	def get_bays(self) -> np.ndarray:
		"""Get unique bay name as list."""
		columns = ['B1', 'B2', 'B3']
		df = self.soe_all
		t0, t1 = self.get_range()
		unique_bays = df.loc[(df['A']=='') & (df['Element'].isin(self.rc_element)) & (df['Tag'].isin(self.order_tags)) & (df['Time stamp']>=t0) & (df['Time stamp']<=t1), columns].drop_duplicates(subset=columns, keep='first')
		return unique_bays.values

	def get_order_count(self) -> int:
		"""Get total RC order."""
		return self.get_orders().shape[0]

	def get_orders(self) -> pd.DataFrame:
		"""Get RC order-tagged only from source."""
		df = self.soe_all.copy()
		t0, t1 = self.get_range()
		# Get His. Messages with order tag only
		orders = df[(df['A']=='') & (df['Element'].isin(self.rc_element)) & (df['Tag'].isin(self.order_tags)) & (df['Time stamp']>=t0) & (df['Time stamp']<=t1)]
		return orders


	@property
	def analyzed(self):
		return self._analyzed

	@property
	def feedback_tags(self):
		return self._feedback_tags

	@property
	def highest_index(self):
		return getattr(self, '_highest_index', None)

	@property
	def is_valid(self):
		return self._is_valid

	@property
	def lowest_index(self):
		return getattr(self, '_lowest_index', None)

	@property
	def order_tags(self):
		return self._order_tags


class _RCDBaseCalculation(BaseAvailability):
	"""Base class for Success Remote Control (RCD) SCADA calculation.

	Args:
		data : analyzed data input

	Accepted kwargs:
		**
	"""
	name: str = 'Remote Control SCADA'
	reduction_ratio_threshold: int = 1
	rcd_all: pd.DataFrame
	station: pd.DataFrame
	bay: pd.DataFrame
	operator: pd.DataFrame

	def __init__(self, data: Optional[pd.DataFrame] = None, calculate_bi: bool = False, check_repetition: bool = True, **kwargs):
		self._calculated: bool = False
		self.check_repetition: bool = check_repetition
		self.rc_element: List[str] = ['CB']
		self.rcd_all = data
		if calculate_bi: self.rc_element += ['BI1', 'BI2']
		super().__init__(**kwargs)

	def initialize(self) -> None:
		"""Re-initiate parameter"""
		self.init_calculate()
		super().initialize()

	def init_calculate(self) -> None:
		"""Re-initiate parameter"""
		self._calculated = False
		self.station = None
		self.bay = None
		self.operator = None

	@calc_time
	def _calculate(self, **kwargs) -> CalcResult:
		"""Base of calculation process.

		Result:
			Calculation summary as dict
		"""
		start = kwargs.get('start')
		stop = kwargs.get('stop')

		if isinstance(self.rcd_all, pd.DataFrame):
			# Can be filtered with date
			if isinstance(start, datetime.datetime) and isinstance(stop, datetime.datetime):
				t0, t1 = self.set_range(start=start, stop=stop)
			else:
				# Parameter start or stop is not valid datetime or None
				t0, t1 = self.set_range(start=self.rcd_all['Order Time'].min(), stop=self.rcd_all['Order Time'].max())
		else:
			raise AttributeError('Data input tidak valid.', name='rcd_all', obj=self)

		result = self.get_result(**kwargs)
		return result

	def calculate(self, start: Optional[datetime.datetime] = None, stop: Optional[datetime.datetime] = None, *args, **kwargs) -> CalcResult:
		"""Calculate RCD availability.

		Args:
			start : oldest date limit
			stop : newest date limit

		Result:
			Calculation result as dict and process duration time
		"""
		self.init_calculate()
		result, t = self._calculate(start=start, stop=stop, **kwargs)
		self.result = result
		self._process_date = datetime.datetime.now()
		self._process_duration = round(t, 3)
		return result

	def rcd_setup(self, **kwargs) -> pd.DataFrame:
		"""Apply sorting and filtering on "rcd_all" to get cleaned data."""
		t0, t1 = self.get_range()
		prepared = self.rcd_all[(self.rcd_all['Order Time']>=t0) & (self.rcd_all['Order Time']<=t1)].copy()
		# Filter only rows with not unused-marked
		prepared = prepared.loc[prepared['Marked Unused']=='']
		# Filter only rows without repetition-marked
		if self.check_repetition:
			prepared = prepared.loc[prepared['Rep. Flag']=='']
		return prepared

	def generate_reference(self, soe: pd.DataFrame, rcd: pd.DataFrame) -> np.ndarray:
		"""Create excel hyperlink of each RC event in sheet "RC_ONLY" to cell range in sheet "HIS_MESSAGES".

		Args:
			soe : dataframe of SOE
			rcd : dataframe of RCD

		Result:
			List of excel hyperlink
		"""
		navs = list()
		errors = 0

		try:
			for idx_start, idx_stop in rcd['Navigation']:
				try:
					hyperlink = f'=HYPERLINK("#HIS_MESSAGES!A{soe.index.get_loc(idx_start)+2}:T{soe.index.get_loc(idx_stop)+2}","CARI >>")'
				except Exception:
					errors += 1
					hyperlink = f'=HYPERLINK("#ERROR!{idx_start}:{idx_stop}","ERROR!!")'

				navs.append(hyperlink)
		except Exception:
			errors += 1

		if errors>0: print(f'Terjadi {errors} error saat generate hyperlink.')
		return np.array(navs)

	def get_result(self, **kwargs) -> CalcResult:
		"""Get aggregate data as per Station, Bay, and Operator, then return list of Excel worksheet name and Dataframe wrapped into dictionaries."""
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

		t0, t1 = self.get_range()
		df_range = self.rcd_all.loc[(self.rcd_all['Order Time']>=t0) & (self.rcd_all['Order Time']<=t1)]
		df_used = self.rcd_setup()

		self.pre_process = df_range
		self.station = self.group_station(df_used)
		self.bay = self.group_bay(df_used)
		self.operator = self.group(df_used, ['Operator'])
		self._calculated = True

		# Calculate overall success rate
		rc_all = df_range.shape[0]
		rc_unused = df_range[df_range['Marked Unused']=='*'].shape[0]
		rc_valid = df_used.shape[0]
		rc_repetition = df_range[df_range['Rep. Flag']=='*'].shape[0]
		rc_close = df_used[df_used['Status']=='Close'].shape[0]
		rc_open = df_used[df_used['Status']=='Open'].shape[0]
		rc_marked = df_range[(df_range['Marked Unused']=='*') | (df_range['Marked Success']=='*') | (df_range['Marked Failed']=='*')].shape[0]
		rc_marked_failed = df_range[df_range['Marked Failed']=='*'].shape[0]
		rc_marked_success = df_range[df_range['Marked Success']=='*'].shape[0]
		rc_failed = self.bay['RC Failed'].sum()
		rc_failed_close = self.bay['Close Failed'].sum()
		rc_failed_open = self.bay['Open Failed'].sum()
		rc_success = self.bay['RC Success'].sum()
		rc_success_close = self.bay['Close Success'].sum()
		rc_success_open = self.bay['Open Success'].sum()
		rc_percentage = round(div(rc_success, rc_valid)*100, 2)
		rc_percentage_close = round(div(rc_success_close, rc_close)*100, 2)
		rc_percentage_open = round(div(rc_success_open, rc_open)*100, 2)
		return {
			'overall': {
				'total': rc_valid,
				'success': rc_success,
				'failed': rc_failed,
				'uncertain': rc_valid - rc_success - rc_failed,
				'percentage': f'{rc_percentage}%'
			},
			'statistic': {
				'total_event': rc_all,
				'total_repetition': rc_repetition,
				'total_valid': rc_valid,
				'marked': {
					'unused': rc_unused,
					'success': rc_marked_success,
					'failed': rc_marked_failed,
					'total': rc_marked
				},
				'operation': {
					'close': rc_close,
					'close_failed': rc_failed_close,
					'close_success': rc_success_close,
					'close_success_percentage': f'{rc_percentage_close}%',
					'open': rc_open,
					'open_failed': rc_failed_open,
					'open_success': rc_success_open,
					'open_success_percentage': f'{rc_percentage_open}%'
				}
			}
		}

	def group(self, df: pd.DataFrame, columns: List) -> pd.DataFrame:
		"""Base function to get aggregation values based on defined "columns".

		Args:
			df : data input
			columns : list of columns as reference

		Result:
			Grouped data
		"""
		groupby_columns = columns + ['Final Result']
		rc_count = df[groupby_columns].groupby(columns, as_index=False).count().rename(columns={'Final Result': 'RC Occurences'})
		rc_success = df.loc[(df['Final Result']=='SUCCESS'), groupby_columns].groupby(columns, as_index=False).count().rename(columns={'Final Result': 'RC Success'})
		rc_failed = df.loc[(df['Final Result']=='FAILED'), groupby_columns].groupby(columns, as_index=False).count().rename(columns={'Final Result': 'RC Failed'})
		df_groupby = rc_count.merge(right=rc_success, how='left', on=columns).merge(right=rc_failed, how='left', on=columns).fillna(0)
		df_groupby['Success Rate'] = np.round(df_groupby['RC Success']/df_groupby['RC Occurences'], 4)
		return df_groupby

	def group_station(self, df:pd.DataFrame) -> pd.DataFrame:
		"""Get aggregation values based on columns Station (B1).

		Args:
			df : data input

		Result:
			Grouped data
		"""
		columns = ['B1']
		groupby_columns = columns + ['Execution (s)', 'Termination (s)', 'TxRx (s)']
		df_groupby = self.group(df, columns)
		df_tmp = df.loc[df['Final Result']=='SUCCESS', groupby_columns].groupby(columns, as_index=False).mean().round(3).rename(columns={'Execution (s)': 'Execution Avg.', 'Termination (s)': 'Termination Avg.', 'TxRx (s)': 'TxRx Avg.'})
		df_groupby = df_groupby.merge(right=df_tmp, how='left', on=columns).fillna(0)
		return df_groupby

	def group_bay(self, df:pd.DataFrame) -> pd.DataFrame:
		"""Get aggregation values based on Bay columns reference (B1, B2, B3).

		Args:
			df : data input

		Result:
			Grouped data
		"""
		columns = ['B1', 'B2', 'B3']
		groupby_columns = columns + ['Final Result']
		df_groupby = None
		# Assign column 'Open Success', 'Open Failed', 'Close Success', 'Close Failed'
		for status in ['Open', 'Close']:
			for result in ['Success', 'Failed']:
				df_tmp = df.loc[(df['Final Result']==result.upper()) & (df['Status']==status), groupby_columns].groupby(columns, as_index=False).count().rename(columns={'Final Result': f'{status} {result}'})
				df_groupby = df_groupby.merge(right=df_tmp, how='outer', on=columns) if isinstance(df_groupby, pd.DataFrame) else df_tmp

		df_groupby = self.group(df, columns).merge(right=df_groupby, how='left', on=columns).fillna(0)
		total_rc = df_groupby['RC Occurences']
		df_groupby['Contribution'] = df_groupby['RC Occurences'].map(lambda x: x/total_rc)
		df_groupby['Reduction'] = df_groupby['RC Failed'].map(lambda y: y/total_rc)
		df_groupby['Tagging'] = ''
		return df_groupby

	def prepare_export(self, generate_formula: bool = False, **kwargs):
		"""Apply excel formulas to output file."""
		if not self.calculated: raise SyntaxError('Jalankan calculate() terlebih dahulu!')

		forsurvalent = SurvalentFileReader in self.mro
		# print(f'Debug: Survalent mode={"YES" if forsurvalent else "NO"}')
		df_rc = self.pre_process.copy()
		df_gi = self.station.copy()
		df_bay = self.bay.copy()
		df_opr = self.operator.copy()

		if forsurvalent:
			# Applied only on Survalent SOE
			df_rc['Order Row (Helper)'] = ''
			df_rc['Feedback Row (Helper)'] = ''

		if generate_formula:
			rc_columns = df_rc.columns.to_list()
			gi_columns = df_gi.columns.to_list()
			bay_columns = df_bay.columns.to_list()
			opr_columns = df_opr.columns.to_list()
			rlen = df_rc.shape[0]
			glen = df_gi.shape[0]
			blen = df_bay.shape[0]
			olen = df_opr.shape[0]

			# Threshold cell location in Sheet Info B12
			thd_var = 'IFERROR(VALUE(Info!$B$12), 0)'

			def rule_lookup(xcol, key=None):
				if key:
					return f'RC_ONLY!${xr[xcol]}$2:${xr[xcol]}${rlen+1}, {key}'
				else:
					return f'RC_ONLY!${xr[xcol]}$2:${xr[xcol]}${rlen+1}'

			def ruleset(*rules):
				return ', '.join(rules)

			def countifs(*rules):
				return f'COUNTIFS({ruleset(*rules)})'

			def averageifs(range, *rules):
				return f'IFERROR(AVERAGEIFS({range}, {ruleset(*rules)}), 0)'

			# Create dict of excel column label
			xr = {col: xl_col_to_name(rc_columns.index(col)) for col in rc_columns}
			xg = {col: xl_col_to_name(gi_columns.index(col)) for col in gi_columns}
			xb = {col: xl_col_to_name(bay_columns.index(col)) for col in bay_columns}
			xo = {col: xl_col_to_name(opr_columns.index(col)) for col in opr_columns}

			rc_update = {
				'Execution (s)': [],
				'Final Result': []
			}
			gi_update = {
				'RC Occurences': [],
				'RC Success': [],
				'RC Failed': [],
				'Success Rate': [],
				'Execution Avg.': [],
				'Termination Avg.': [],
				'TxRx Avg.': []
			}
			bay_update = {
				'RC Occurences': [],
				'RC Success': [],
				'RC Failed': [],
				'Success Rate': [],
				'Open Success': [],
				'Open Failed': [],
				'Close Success': [],
				'Close Failed': [],
				'Contribution': [],
				'Reduction': [],
				'Tagging': []
			}
			opr_update = {
				'RC Occurences': [],
				'RC Success': [],
				'RC Failed': [],
				'Success Rate': []
			}

			# Define excel formula rule
			rule_repetition = rule_lookup('Rep. Flag', '""')
			rule_unused = rule_lookup('Marked Unused', '""')

			# Apply excel formula as string
			# Sheet RC_ONLY
			if 'Navigation' in rc_columns and not forsurvalent:
				# Apply navigation hyperlink on sheet RC_ONLY
				rc_update['Navigation'] = self.generate_reference(soe=kwargs.get('soe'), rcd=df_rc)

			if forsurvalent:
				# Applied only on Survalent SOE
				rc_update['Feedback Time'] = []
				rc_update['Navigation'] = []
				rc_update['Order Row (Helper)'] = []
				rc_update['Feedback Row (Helper)'] = []

			for rowr in range(rlen):
				ir = rowr + 2
				rc_update['Execution (s)'].append(f'=(${xr["Feedback Time"]}{ir}-${xr["Order Time"]}{ir})*24*3600')
				rc_update['Final Result'].append(f'=IF(${xr["Marked Success"]}{ir}="*", "SUCCESS", IF(${xr["Marked Failed"]}{ir}="*", "FAILED", ${xr["Pre Result"]}{ir}))')

				if forsurvalent:
					rc_update['Feedback Time'].append(f'=INDIRECT("\'HIS_MESSAGES\'!B"&${xr["Feedback Row (Helper)"]}{ir})+(INDIRECT("\'HIS_MESSAGES\'!C"&${xr["Feedback Row (Helper)"]}{ir})/86400000)')
					rc_update['Navigation'].append(f'=HYPERLINK("#HIS_MESSAGES!A"&{xr["Order Row (Helper)"]}{ir}&":T"&{xr["Feedback Row (Helper)"]}{ir},"CARI >>")')
					rc_update['Order Row (Helper)'].append(kwargs.get('soe').index.get_loc(df_rc.loc[rowr, 'Navigation'][0]) + 2)
					rc_update['Feedback Row (Helper)'].append(kwargs.get('soe').index.get_loc(df_rc.loc[rowr, 'Navigation'][1]) + 2)

			rc_result = {
				'Operator': [
					'TOTAL RC (RAW)',
					'SUCCESS (RAW)',
					'FAILED (RAW)',
					'SUCCESS RATE'
				],
				'Pre Result': [
					f'=COUNTA(${xr["Pre Result"]}$2:${xr["Pre Result"]}${rlen+1})',
					f'=COUNTIF(${xr["Pre Result"]}2:{xr["Pre Result"]}${rlen+1}, "SUCCESS")',
					f'=COUNTIF(${xr["Pre Result"]}2:{xr["Pre Result"]}${rlen+1}, "FAILED")',
					f'=ROUND(IFERROR(${xr["Pre Result"]}${rlen+3}/${xr["Pre Result"]}${rlen+2}, 0)*100, 2)'
				]
			}
			df_rc_result = pd.DataFrame(data=rc_result)
			# Sheet GI
			for rowg in range(glen):
				ig = rowg + 2
				rule_b1 = rule_lookup('B1', f'${xg["B1"]}{ig}')
				rules = [rule_b1, rule_repetition, rule_unused]
				gi_update['RC Occurences'].append('=' + countifs(*rules))
				gi_update['RC Success'].append('=' + countifs(*rules, rule_lookup('Final Result', '"SUCCESS"')))
				gi_update['RC Failed'].append('=' + countifs(*rules, rule_lookup('Final Result', '"FAILED"')))
				gi_update['Success Rate'].append(f'=IFERROR(${xg["RC Success"]}{ig}/${xg["RC Occurences"]}{ig}, 0)')
				gi_update['Execution Avg.'].append(f'=IF(${xg["RC Success"]}{ig}=0, 0, ' + averageifs(rule_lookup('Execution (s)'), *rules, rule_lookup('Final Result', '"SUCCESS"')) + ')')
				gi_update['Termination Avg.'].append(f'=IF(${xg["RC Success"]}{ig}=0, 0, ' + averageifs(rule_lookup('Termination (s)'), *rules, rule_lookup('Final Result', '"SUCCESS"')) + ')')
				gi_update['TxRx Avg.'].append(f'=IF(${xg["RC Success"]}{ig}=0, 0,  ' + averageifs(rule_lookup('TxRx (s)'), *rules, rule_lookup('Final Result', '"SUCCESS"')) + ')')

			gi_result = {
				'RC Occurences': [f'=SUM(${xg["RC Occurences"]}$2:${xg["RC Occurences"]}${glen+1})'],
				'RC Success': [f'=SUM(${xg["RC Success"]}$2:${xg["RC Success"]}${glen+1})'],
				'RC Failed': [f'=SUM(${xg["RC Failed"]}$2:${xg["RC Failed"]}${glen+1})'],
				'Success Rate': [f'=IFERROR(${xg["RC Success"]}{glen+2}/${xg["RC Occurences"]}{glen+2}, 0)'],
			}
			df_gi_result = pd.DataFrame(data=gi_result)
			# Sheet BAY
			for rowb in range(blen):
				ib = rowb + 2
				rule_b1 = rule_lookup('B1', f'${xb["B1"]}{ib}')
				rule_b2 = rule_lookup('B2', f'${xb["B2"]}{ib}')
				rule_b3 = rule_lookup('B3', f'${xb["B3"]}{ib}')
				rules = [rule_b1, rule_b2, rule_b3, rule_repetition, rule_unused]
				bay_update['RC Occurences'].append('=' + countifs(*rules))
				bay_update['RC Success'].append('=' + countifs(*rules, rule_lookup('Final Result', '"SUCCESS"')))
				bay_update['RC Failed'].append('=' + countifs(*rules, rule_lookup('Final Result', '"FAILED"')))
				bay_update['Success Rate'].append(f'=IFERROR(${xb["RC Success"]}{ib}/${xb["RC Occurences"]}{ib}, 0)')
				for status in ['Open', 'Close']:
					for result in ['Success', 'Failed']:
						bay_update[f'{status} {result}'].append('=' + countifs(*rules, rule_lookup('Status', f'"{status}"'), rule_lookup('Final Result', f'"{result.upper()}"')))
				bay_update['Contribution'].append(f'=IFERROR(${xb["RC Occurences"]}{ib}/${xb["RC Occurences"]}${blen+2}, 0)')	# <rc occur>/<total rc occur>
				bay_update['Reduction'].append(f'=${xb["RC Failed"]}{ib}/${xb["RC Occurences"]}${blen+2}')	# <rc failed>/<total rc occur>
				bay_update['Tagging'].append(f'=IF(IFERROR(${xb["Open Failed"]}{ib}^2/(${xb["Open Failed"]}{ib}+${xb["Open Success"]}{ib}), 0)>{thd_var}, "O", "") & IF(IFERROR(${xb["Close Failed"]}{ib}^2/(${xb["Close Failed"]}{ib}+${xb["Close Success"]}{ib}), 0)>{thd_var}, "C", "")')

			bay_result = {
				'RC Occurences': [f'=SUM(${xb["RC Occurences"]}$2:${xb["RC Occurences"]}${blen+1})'],
				'RC Success': [f'=SUM(${xb["RC Success"]}$2:${xb["RC Success"]}${blen+1})'],
				'RC Failed': [f'=SUM(${xb["RC Failed"]}$2:${xb["RC Failed"]}${blen+1})'],
				'Success Rate': [f'=IFERROR(${xb["RC Success"]}{blen+2}/${xb["RC Occurences"]}{blen+2}, 0)'],
				'Open Success': [f'=SUM(${xb["Open Success"]}$2:${xb["Open Success"]}${blen+1})'],
				'Open Failed': [f'=SUM(${xb["Open Failed"]}$2:${xb["Open Failed"]}${blen+1})'],
				'Close Success': [f'=SUM(${xb["Close Success"]}$2:${xb["Close Success"]}${blen+1})'],
				'Close Failed': [f'=SUM(${xb["Close Failed"]}$2:${xb["Close Failed"]}${blen+1})'],
			}
			df_bay_result = pd.DataFrame(data=bay_result)
			# Sheet DISPATCHER
			for rowo in range(olen):
				io = rowo + 2
				rule_operator = rule_lookup('Operator', f'${xo["Operator"]}{io}')
				rules = [rule_operator, rule_repetition, rule_unused]
				opr_update['RC Occurences'].append('=' + countifs(*rules))
				opr_update['RC Success'].append('=' + countifs(*rules, rule_lookup('Final Result', '"SUCCESS"')))
				opr_update['RC Failed'].append('=' + countifs(*rules, rule_lookup('Final Result', '"FAILED"')))
				opr_update['Success Rate'].append(f'=IFERROR(${xo["RC Success"]}{io}/${xo["RC Occurences"]}{io}, 0)')

			opr_result = {
				'RC Occurences': [f'=SUM(${xo["RC Occurences"]}$2:${xo["RC Occurences"]}${olen+1})'],
				'RC Success': [f'=SUM(${xo["RC Success"]}$2:${xo["RC Success"]}${olen+1})'],
				'RC Failed': [f'=SUM(${xo["RC Failed"]}$2:${xo["RC Failed"]}${olen+1})'],
				'Success Rate': [f'=IFERROR(${xo["RC Success"]}{olen+2}/${xo["RC Occurences"]}{olen+2}, 0)'],
			}
			df_opr_result = pd.DataFrame(data=opr_result)

			# Loop through columns to update Dataframe
			# Using <df>.update(pd.Dataframe(<df_update>)) may cause unwanted warning because of incompatible dtype set
			for rccol in rc_update: df_rc[rccol] = np.array(rc_update[rccol])
			for gicol in gi_update: df_gi[gicol] = np.array(gi_update[gicol])
			for baycol in bay_update: df_bay[baycol] = np.array(bay_update[baycol])
			for oprcol in opr_update: df_opr[oprcol] = np.array(opr_update[oprcol])

			# Update summary information
			self.result['statistic']['total_repetition'] = f'=COUNTIF({rule_repetition.split(",")[0]}, "*")'
			self.result['statistic']['total_valid'] = f'=COUNTIFS({rule_unused}, {rule_repetition})'
			self.result['statistic']['marked']['unused'] = f'=COUNTIF({rule_unused.split(",")[0]}, "*")'
			self.result['statistic']['marked']['success'] = f'=COUNTIF(RC_ONLY!${xr["Marked Success"]}$2:{xr["Marked Success"]}${rlen+1}, "*")'
			self.result['statistic']['marked']['failed'] = f'=COUNTIF(RC_ONLY!${xr["Marked Failed"]}$2:{xr["Marked Failed"]}${rlen+1}, "*")'
			self.result['statistic']['marked']['total'] = f'=COUNTIF(RC_ONLY!${xr["Marked Unused"]}$2:{xr["Marked Failed"]}${rlen+1}, "*")'
			self.result['statistic']['operation']['close_success_percentage'] = f'=ROUND(IFERROR(BAY!${xb["Close Success"]}${blen+2}/(BAY!${xb["Close Success"]}${blen+2}+BAY!${xb["Close Failed"]}${blen+2}), 0)*100, 2) & "%"'
			self.result['statistic']['operation']['open_success_percentage'] = f'=ROUND(IFERROR(BAY!${xb["Open Success"]}${blen+2}/(BAY!${xb["Open Success"]}${blen+2}+BAY!${xb["Open Failed"]}${blen+2}), 0)*100, 2) & "%"'
			self.result['overall']['percentage'] = f'=ROUND(BAY!${xb["Success Rate"]}${blen+2}*100, 2) & "%"'

		return {
			'RC_ONLY': (df_rc, df_rc_result),
			'GI': (df_gi, df_gi_result),
			'BAY': (df_bay, df_bay_result),
			'DISPATCHER': (df_opr, df_opr_result)
		}

	def print_result(self) -> None:
		"""Print summary in terminal."""
		width, height = os.get_terminal_size()
		# Check if RC Event has been analyzed
		if self.calculated==False:
			return print('Tidak dapat menampilkan hasil Kalkulasi RC. Jalankan fungsi "calculate()" terlebih dahulu.')

		df_gi = self.station.copy()
		df_gi['Success Rate'] = df_gi['Success Rate'].map(lambda x: round(x*100, 2))
		df_bay = self.bay.copy()
		df_bay['Success Rate'] = df_bay['Success Rate'].map(lambda x: round(x*100, 2))
		df_dispa = self.operator.copy()
		df_dispa['Success Rate'] = df_dispa['Success Rate'].map(lambda x: round(x*100, 2))
		context = {
			'date_end': self.t1.strftime("%d-%m-%Y"),
			'date_start': self.t0.strftime("%d-%m-%Y"),
			'df_bay': df_bay.sort_values(['Success Rate', 'RC Occurences'], ascending=[True, False]).iloc[0:5, [0, 1, 2, 3, 6]].rename(columns={'RC Occurences': 'Jumlah', 'Success Rate': 'Persentase'}).to_string(index=False),
			'df_bay_count': 5 if df_bay.shape[0]>5 else df_bay.shape[0],
			'df_dispa': df_dispa.sort_values(['Success Rate', 'RC Occurences'], ascending=[True, False]).iloc[0:5, [0, 1, 4]].rename(columns={'RC Occurences': 'Jumlah', 'Success Rate': 'Persentase'}).to_string(index=False),
			'df_dispa_count': 5 if df_dispa.shape[0]>5 else df_dispa.shape[0],
			'df_gi': df_gi.sort_values(['Success Rate', 'RC Occurences'], ascending=[True, False]).iloc[0:5, [0, 1, 4]].rename(columns={'RC Occurences': 'Jumlah', 'Success Rate': 'Persentase'}).to_string(index=False),
			'df_gi_count': 5 if df_gi.shape[0]>5 else df_gi.shape[0],
			'element': ', '.join(self.rc_element),
			'rc_total': self.result['overall']['total'],
			'rc_percentage': self.result['overall']['percentage'],
			'width': width
		}
		print(summary_template(**context))

	@property
	def calculated(self):
		return self._calculated

	@property
	def process_date(self):
		return getattr(self, '_process_date', None)

	@property
	def process_duration(self):
		return getattr(self, '_process_duration', None)


class RCD(_RCDBaseCalculation, _Export):

	def __init__(self, data: Optional[pd.DataFrame] = None, calculate_bi: bool = False, check_repetition: bool = True, **kwargs):
		kwargs.update(data=data, calculate_bi=calculate_bi, check_repetition=check_repetition)
		super().__init__(**kwargs)

	def calculate(self, start: Optional[datetime.datetime] = None, stop: Optional[datetime.datetime] = None, *args, **kwargs) -> CalcResult:
		result = super().calculate(start, stop, *args, **kwargs)
		print(f'Perhitungan RC {self.t0.strftime("%d-%m-%Y")} s/d {self.t1.strftime("%d-%m-%Y")} selesai. (durasi={self.process_duration:.2f}s, error={len(self.errors)})')
		return result

	async def async_calculate(self, start: Optional[datetime.datetime] = None, stop: Optional[datetime.datetime] = None, *args, **kwargs) -> CalcResult:
		await asyncio.sleep(0)
		self.progress.init('Menghitung RC')
		result = super().calculate(start, stop, *args, **kwargs)
		self.progress.update(1.0)
		print(f'Perhitungan RC {self.t0.strftime("%d-%m-%Y")} s/d {self.t1.strftime("%d-%m-%Y")} selesai. (durasi={self.process_duration:.2f}s, error={len(self.errors)})')
		return result


class SOEtoRCD(_RCDBaseCalculation, _SOEAnalyzer, _Export):

	def __init__(self, data: Optional[pd.DataFrame] = None, calculate_bi: bool = False, check_repetition: bool = True, **kwargs):
		kwargs.update(data=data, calculate_bi=calculate_bi, check_repetition=check_repetition)
		super().__init__(**kwargs)

	def calculate(
			self,
			start: Optional[datetime.datetime] = None,
			stop: Optional[datetime.datetime] = None,
			force: bool = False,
			fast: bool = True,
			*args,
			**kwargs
		) -> CalcResult:

		time_start = time.time()
		fast_analyze = getattr(self, 'fast_analyze', None)
		analyze = getattr(self, 'analyze', None)

		if callable(fast_analyze) and fast:
			self.rcd_all = fast_analyze(*args, force=force, **kwargs)
		elif callable(analyze):
			self.rcd_all = analyze(*args, force=force, **kwargs)
		else:
			raise AttributeError('Atttribute error.', name='fast_analyze() / analyze()', obj=self.__class__.__name__)

		result = super().calculate(start, stop, *args, **kwargs)
		delta_time = time.time() - time_start
		self._process_duration = round(delta_time, 3)
		print(f'Perhitungan RC {self.t0.strftime("%d-%m-%Y")} s/d {self.t1.strftime("%d-%m-%Y")} selesai. (durasi={delta_time:.2f}s, error={len(self.errors)})')
		return result

	async def async_calculate(
			self,
			start: Optional[datetime.datetime] = None,
			stop: Optional[datetime.datetime] = None,
			force: bool = False,
			*args,
			**kwargs
		) -> CalcResult:
		"""Coroutine of calculation function which used to work together with async_analyze.

		Args:
			start : oldest date limit
			stop : newest date limit

		Result:
			Calculation result as dict and process duration time
		"""
		time_start = time.time()
		self.rcd_all = await self.async_analyze(*args, force=force, **kwargs)
		self.progress.init('Menghitung RC')
		result = super().calculate(start, stop, *args, **kwargs)
		self.progress.update(1.0)
		delta_time = time.time() - time_start
		self._process_duration = round(delta_time, 3)
		print(f'Perhitungan RC {self.t0.strftime("%d-%m-%Y")} s/d {self.t1.strftime("%d-%m-%Y")} selesai. (durasi={delta_time:.2f}s, error={len(self.errors)})')
		return result

	def prepare_export(self, generate_formula: bool = False, **kwargs):
		"""Applying excel formulas to output file.

		Args:
			generate_formula : either formula will be generated or not

		Result:
			Dict of sheet name & data
		"""
		if not self.analyzed: raise SyntaxError('Jalankan calculate() terlebih dahulu!')
		return {
			'HIS_MESSAGES': self.post_process,
			**super().prepare_export(soe=self.post_process, generate_formula=generate_formula, **kwargs)
		}


class RCDCollective(RCFileReader, RCD):
	__params__: set = {'calculate_bi', 'check_repetition', 'reduction_ratio_threshold'}

	def __init__(self, files: Union[str, FilePaths, FileDict, None] = None, **kwargs):
		super().__init__(files, **kwargs)

	def load(self, **kwargs):
		rcd_all = super().load(**kwargs)
		self.rcd_all = rcd_all
		return rcd_all
	
	async def async_load(self, **kwargs):
		rcd_all = await super().async_load(**kwargs)
		self.rcd_all = rcd_all
		return rcd_all


class RCDFromFile(SpectrumFileReader, SOEtoRCD):
	__params__: set = {'calculate_bi', 'check_repetition', 'success_mark', 'failed_mark', 'unused_mark', 'reduction_ratio_threshold'}

	def __init__(self, files: Union[str, FilePaths, FileDict, None] = None, **kwargs):
		super().__init__(files, **kwargs)

	def load(self, **kwargs):
		soe_all = super().load(**kwargs)
		self.soe_all = soe_all
		return soe_all

	async def async_load(self, **kwargs):
		soe_all = await super().async_load(**kwargs)
		self.soe_all = soe_all
		return soe_all


class RCDFromFile2(SurvalentFileReader, SOEtoRCD):
	__params__: set = {'calculate_bi', 'check_repetition', 'success_mark', 'failed_mark', 'unused_mark', 'reduction_ratio_threshold'}

	def __init__(self, files: Union[str, FilePaths, FileDict, None] = None, **kwargs):
		super().__init__(files, **kwargs)

	def load(self, **kwargs):
		soe_all = super().load(**kwargs)
		self.soe_all = soe_all
		return soe_all

	async def async_load(self, **kwargs):
		soe_all = await super().async_load(**kwargs)
		self.soe_all = soe_all
		return soe_all


class RCDFromOFDB(SpectrumOfdbClient, SOEtoRCD):
	__params__: set = {'calculate_bi', 'check_repetition', 'success_mark', 'failed_mark', 'unused_mark', 'reduction_ratio_threshold'}

	def __init__(self, date_start: Optional[datetime.datetime] = None, date_stop: Optional[datetime.datetime] = None, **kwargs):
		super().__init__(date_start, date_stop, **kwargs)


def summary_template(**kwargs):
	width = kwargs.get('width', 40) - 1
	return f"""
{'='*width}
Rangkuman RC {kwargs.get('element')} tanggal {kwargs.get('date_start')} s/d {kwargs.get('date_end')}
{'='*width}
- Jumlah RC	: {kwargs.get('rc_total')}
- RC Sukses	: {kwargs.get('rc_percentage')}


> TOP {kwargs.get('df_gi_count')} GI DENGAN RASIO SUKSES TERENDAH
{'-'*width}
{kwargs.get('df_gi')}


> TOP {kwargs.get('df_bay_count')} BAY DENGAN RASIO SUKSES TERENDAH
{'-'*width}
{kwargs.get('df_bay')}


> TOP {kwargs.get('df_dispa_count')} DISPATCHER DENGAN RASIO SUKSES TERENDAH
{'-'*width}
{kwargs.get('df_dispa')}

"""
	

def rc_analyze_file(**params):
	handler = RCDFromFile
	filepaths = 'sample/sample_rcd*.xlsx'
	title = 'RCD'
	return test_analyze(handler, title=title, filepaths=filepaths)

def rc_analyze_file2(**params):
	handler = RCDFromFile2
	filepaths = 'sample/survalent/sample_soe*.XLSX'
	title = 'RCD'
	master = 'Survalent'
	return test_analyze(handler, title=title, filepaths=filepaths, master=master)

def rc_collective(**params):
	handler = RCDCollective
	filepaths = 'sample/sample_rcd*.xlsx'
	title = 'RCD'
	return test_collective(handler, title=title, filepaths=filepaths)


if __name__=='__main__':
	test_list = [
		('Test analisa file SOE Spectrum', rc_analyze_file),
		('Test analisa file SOE Survalent', rc_analyze_file2),
		('Test menggabungkan file', rc_collective)
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