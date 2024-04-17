import os, re, time
from datetime import datetime
from typing import Union

import numpy as np
import pandas as pd
from xlsxwriter.utility import xl_col_to_name
from filereader import RCFileReader, SpectrumFileReader, SurvalentFileReader
from global_parameters import RCD_BOOK_PARAM, RCD_COLUMNS
from lib import BaseExportMixin, get_datetime, get_execution_duration, get_termination_duration, join_datetime, immutable_dict, progress_bar
from ofdb import SpectrumOfdbClient


class _Export(BaseExportMixin):
	_sheet_parameter = immutable_dict(RCD_BOOK_PARAM)
	output_prefix = 'RCD'

	def get_sheet_info_data(self, **kwargs):
		"""
		"""

		info_data = super().get_sheet_info_data(**kwargs)
		extra_info = [
			*info_data,
			('', ''),
			('SETTING', ''),
			('RC Element', ', '.join(self.rc_element)),
			('RC Repetition', 'last-occurrence-only' if self.check_repetition else 'calculate-all'),
			('Threshold (default=1)', self.threshold_variable),
			('', ''),
			('SUMMARY', ''),
			('Success Percentage', self.result['overall']['percentage']),
			('Success Percentage (Close)', self.result['statistic']['operation']['close_success_percentage']),
			('Success Percentage (Open)', self.result['statistic']['operation']['open_success_percentage']),
			('', ''),
			('STATISTICS', ''),
			('Marked', self.result['statistic']['marked']['total']),
			('Unused-marked', self.result['statistic']['marked']['unused']),
			('Success-marked', self.result['statistic']['marked']['success']),
			('Failed-marked', self.result['statistic']['marked']['failed'])
		]

		return extra_info


class _SOEAnalyzer:
	_feedback_tags = ['RC', 'NE', 'R*', 'N*']
	_order_tags = ['OR', 'O*']
	t_monitor = {'CB': 15, 'BI1': 30, 'BI2': 30}
	t_transition = {'CB': 1, 'BI1': 16, 'BI2': 16}
	t_search = 3*60*60
	success_mark = '**success**'
	failed_mark = '**failed**'
	unused_mark = '**unused**'
	keep_duplicate = 'last'

	def __init__(self, data:pd.DataFrame=None, calculate_bi:bool=False, check_repetition:bool=True, **kwargs):
		self.rc_element = ['CB']
		self.check_repetition = check_repetition

		if calculate_bi: self.rc_element += ['BI1', 'BI2']

		if data is not None: self.soe_all = data

		self._initialize()
		self._soe_setup()
		super().__init__(calculate_bi=calculate_bi, check_repetition=check_repetition, **kwargs)

	def _initialize(self):
		"""
		"""

		self._analyzed = False
		self._cd_qualities = {}
		self._cso_qualities = {}
		self._lr_qualities = {}
		self._rc_indexes = []

	def _set_range(self, start:datetime, stop:datetime):
		"""
		Set start and stop date of query data.
		"""

		dt0 = start.replace(hour=0, minute=0, second=0, microsecond=0)
		dt1 = stop.replace(hour=23, minute=59, second=59, microsecond=999999)

		self._t0 = dt0
		self._t1 = dt1

		return dt0, dt1

	def _soe_setup(self):
		"""
		Preparing Dataframe joins from multiple file with sorting and filtering to get clean data.
		* INPUT
		input :
		* OUTPUT
		df :
		* SET
		date_start :
		date_stop :
		highest_index :
		lowest_index :
		order_messages : Dataframe filter for His. Messages with "Order" tag only
		b*_list : list of unique value of field B1, B2, B3
		soe_ifs : Dataframe filter for His. Messages of IFS Status only
		ifs_list :
		ifs_name_matching :
		soe_ctrl : Dataframe of RC element status changes only
		soe_cd : Dataframe of CD status only
		soe_lr : Dataframe of LR status only
		soe_sync : Dataframe of CSO status only
		soe_prot : Dataframe of protection alarm only
		"""

		if isinstance(self.soe_all, pd.DataFrame):
			self._is_valid = True
			df = self.soe_all.copy()
			df = df.sort_values(['System time stamp', 'System milliseconds', 'Time stamp', 'Milliseconds'], ascending=[True, True, True, True]).reset_index(drop=True)
			orders = self.get_orders()

			# Get min index and max index of df
			self._lowest_index, self._highest_index = df.index.min(), df.index.max()

			soe_ifs = df[(df['A']=='') & (df['B1']=='IFS') & (df['B2']=='RTU_P1') & (df['Tag']=='')]
			# Get IFS name matching
			self._ifs_list = soe_ifs['B3'].unique()
			self._b1_list, self._b2_list, self._b3_list = orders['B1'].unique(), orders['B2'].unique(), orders['B3'].unique()
			# Filter IFS messages only if related to RC Event
			self.soe_ifs = soe_ifs[soe_ifs['B3'].isin(self.b1_list)]

			# Filter His. Messages only if related to RC Event's B1, B2 and B3
			df = df[(df['A']=='') & (df['B1'].isin(self.b1_list)) & (df['B2'].isin(self.b2_list)) & (df['B3'].isin(self.b3_list))]

			# Reset comment column and search for order tag for RC element
			soe_ctrl = df[(df['Element'].isin(self.rc_element)) & (df['Status'].isin(['Open', 'Close', 'Dist.']))].copy()
			soe_ctrl['RC Order'] = np.where((soe_ctrl['Element'].isin(self.rc_element)) & (soe_ctrl['Tag'].isin(self.order_tags)), 'REMOTE', '')
			soe_ctrl['RC Feedback'] = ''
			soe_ctrl['Comment'] = ''

			# Split into DataFrames for each purposes, not reset index
			self.soe_ctrl = soe_ctrl
			self.soe_cd = df[df['Element']=='CD'].copy()
			self.soe_lr = df[df['Element']=='LR'].copy()
			self.soe_sync = df[df['Element']=='CSO'].copy()
			self.soe_prot = df[df['Element'].isin(['CBTR', 'MTO'])].copy()
		else:
			self._is_valid = False
			raise ValueError('Dataframe tidak valid')

	def analyze(self, start:datetime=None, stop:datetime=None):
		"""
		Loop through RC event to analyze set of probability and infromation about how every RC event occured (Failed or Success). In case of loop optimization, we split the main Dataframe (df) into parts of categories.
		* INPUT
		analyzed_rc_rows : list of tuple of analyzed RC event to create new Dataframe
		note_list : list of annotation of each analyzed RC event
		rc_order_index : list of index of RC order
		multiple_rc_at_same_time : crossing of multiple RC Event
		date_origin : date refference as comparator
		buffer1 : buffer for storing index of df_rc, to get RC repetition in one day
		buffer1 : buffer for storing index of df_result, to get RC repetition in one day
		* OUTPUT
		rcd
		* SET
		analyzed_rc_rows
		soe
		note_list
		"""

		rc_list = []
		note_list = []
		buffer1, buffer2 = {}, {}

		orders = self.get_orders(start=start, stop=stop)
		rc_order_index = orders.index.to_list()
		date_origin = self.soe_ctrl.loc[rc_order_index[0], 'Time stamp']

		print(f'\nMenganalisa {len(rc_order_index)} kejadian RC...')
		for x, index in enumerate(rc_order_index):
			progress_bar((x+1)/len(rc_order_index))

			# index_0 = index of RC order Tag, index_1 = index of RC Feedback Tag, index_2 = index of next RC order Tag
			rc_order = self.soe_ctrl.loc[index]
			date_rc, b1, b2, b3, elm, sts = rc_order.loc[['Time stamp', 'B1', 'B2', 'B3', 'Element', 'Status']]
			index_0 = index
			index_2 = rc_order_index[x + 1] if x<len(rc_order_index)-1 else self.highest_index
			index_1, result = self.get_result(rc_order, rc_list, note_list)
			bufkey = (b1, b2, b3, elm, sts)

			# if result in ['FAILED', 'UNCERTAIN']: print(x, bufkey)
			# Check RC repetition
			if self.check_repetition:
				if (date_rc.year==date_origin.year and date_rc.month==date_origin.month and date_rc.day==date_origin.day) and x<len(rc_order_index)-1:
					# If in the same day and not last iteration
					if bufkey in buffer1:
						# Bufkey already in buffer, append buffer
						buffer1[bufkey] += [index_0]
						buffer2[bufkey] += [x]

						if result=='SUCCESS':
							# Comment to mark as last RC repetition
							for m in range(len(buffer1[bufkey])):
								if m==len(buffer1[bufkey])-1:
									comment_text = f'Percobaan RC ke-{m+1} (terakhir)'
								else:
									comment_text = f'Percobaan RC ke-{m+1}'
									# Give flag
									rc_list[buffer2[bufkey][m]]['Rep. Flag'] = '*'
								self.soe_ctrl.at[buffer1[bufkey][m], 'Comment'] += f'{comment_text}\n'
								note_list[buffer2[bufkey][m]].insert(0, comment_text)

							del buffer1[bufkey]
							del buffer2[bufkey]

					else:
						if result in ['FAILED', 'UNCERTAIN']:
							buffer1[bufkey] = [index_0]
							buffer2[bufkey] = [x]
				else:
					# If dates are different, set date_origin
					date_origin = date_rc

					for bkey, bval in buffer1.items():
						if len(bval)>1:
							# Comment to mark as multiple RC event in 1 day
							for n in range(len(bval)):
								if n==len(bval)-1:
									comment_text = f'Percobaan RC ke-{n+1} (terakhir)'
								else:
									comment_text = f'Percobaan RC ke-{n+1}'
									# Give flag
									rc_list[buffer2[bkey][n]]['Rep. Flag'] = '*'
								self.soe_ctrl.at[bval[n], 'Comment'] += f'{comment_text}\n'
								note_list[buffer2[bkey][n]].insert(0, comment_text)

					# Reset buffer
					buffer1, buffer2 = {}, {}
					# Insert into new buffer
					if result in ['FAILED', 'UNCERTAIN']:
						buffer1[bufkey] = [index_0]
						buffer2[bufkey] = [x]

			self._rc_indexes.append((index_0, index_1, result))

		rcd = pd.DataFrame(data=rc_list)
		rcd['Annotations'] = list(map(lambda x: '\n'.join(list(map(lambda y: f'- {y}', x))), note_list))
		self.rc_list = rc_list
		self.post_process = pd.concat([self.soe_ctrl, self.soe_lr, self.soe_cd, self.soe_sync, self.soe_prot, self.soe_ifs], copy=False).drop_duplicates(keep='first').sort_values(['Time stamp', 'Milliseconds'])
		self._analyzed = True

		return rcd[RCD_COLUMNS + ['Navigation']]

	def check_enable_status(self, data:dict):
		"""
		Check CD status on an event parameterized in a dict (data), then return CD status and CD quality into one set.
		* INPUT
		data : dict of an event, required field [System Timestamp, B1]
		* OUTPUT
		cd_status :
		cd_quality :
		* SET
		cd_qualities :
		"""

		# Initialize
		cd_status = 'Enable'
		df_cd = self.soe_cd[(self.soe_cd['B1']==data['b1']) & (self.soe_cd['Element']=='CD') & (self.soe_cd['Tag']=='')]

		# Check CD quality in program buffer
		if data['b1'] in self.cd_qualities:
			cd_quality = self.cd_qualities[data['b1']]
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
			self._cd_qualities[data['b1']] = cd_quality

		if cd_quality in ['good', 'bad']:
			# If quality good, filter only valid status
			if cd_quality=='good': df_cd = df_cd[df_cd['Status'].isin(['Enable', 'Disable'])]
			
			if df_cd[join_datetime(df_cd['System time stamp'], df_cd['System milliseconds'])<data['t1']].shape[0]>0:
				# CD status changes occured before
				cd_last_change = df_cd[join_datetime(df_cd['System time stamp'], df_cd['System milliseconds'])<data['t1']].iloc[-1]
				cd_status = 'Enable' if cd_last_change['Status']=='Enable' else 'Disable'
			else:
				# CD status changes occured after
				cd_first_change = df_cd[join_datetime(df_cd['System time stamp'], df_cd['System milliseconds'])>=data['t1']].iloc[0]
				cd_status = 'Disable' if cd_first_change['Status']=='Enable' else 'Enable'
				
		return cd_status, cd_quality

	def check_ifs_status(self, data:dict):
		"""
		Check IFS status on an event parameterized in a dict (data), then return IFS status and IFS name into one set.
		* INPUT
		data : dict of an event, required field [System Timestamp, B1]
		name_dict : dict of name matching between name in SOE event messages field B1 and name in IFS event messages field B3
		* OUTPUT
		ifs_status :
		ifs_name :
		"""

		# Initialize
		t_hyst = 2*60
		ifs_status = 'Up'
		# Change here if using ifs_name_matching
		ifs_name = data['b1']

		if ifs_name:
			df_ifs = self.soe_ifs[(self.soe_ifs['B1']=='IFS') & (self.soe_ifs['B2']=='RTU_P1') & (self.soe_ifs['B3']==ifs_name) & (self.soe_ifs['Tag']=='')]
			if df_ifs.shape[0]>0:
				if df_ifs[join_datetime(df_ifs['System time stamp'], df_ifs['System milliseconds'])<data['t1']].shape[0]>0:
					# IFS status changes occured before
					ifs_last_change = df_ifs[join_datetime(df_ifs['System time stamp'], df_ifs['System milliseconds'])<data['t1']].iloc[-1]
					ifs_status = 'Down' if ifs_last_change['Status']=='Down' else 'Up'
				else:
					# IFS status changes occured after
					ifs_first_change = df_ifs[join_datetime(df_ifs['System time stamp'], df_ifs['System milliseconds'])>=data['t1']].iloc[0]
					t_delta = round((join_datetime(ifs_first_change['System time stamp'], ifs_first_change['System milliseconds'])-data['t1']).total_seconds(), 1)
					if abs(t_delta)<t_hyst:
						ifs_status = f'transisi menuju down ({t_delta}s)' if ifs_first_change['Status']=='Down' else f'transisi menuju Up ({t_delta}s)'
					else:
						ifs_status = f'Up' if ifs_first_change['Status']=='Down' else f'Down'

		return ifs_status, ifs_name

	def check_protection_interlock(self, data:dict):
		"""
		Check protection status on an event parameterized in a dict (data), then return active protection or "" (empty string) if none.
		* INPUT
		data : dict of an event, required field [System Timestamp, B1, B2, B3]
		* OUTPUT
		prot_isactive
		"""

		# Initialize
		prot_isactive = ''
		index = -1
		df_prot = self.soe_prot[(self.soe_prot['Tag']=='') & (self.soe_prot['B1']==data['b1']) & (self.soe_prot['B2']==data['b2']) & (self.soe_prot['B3']==data['b3']) & (self.soe_prot['Element'].isin(['CBTR', 'MTO']))]

		if df_prot[join_datetime(df_prot['System time stamp'], df_prot['System milliseconds'])<data['t1']].shape[0]>0:
			# Latched protection Appeared before
			prot_last_appear = df_prot[join_datetime(df_prot['System time stamp'], df_prot['System milliseconds'])<data['t1']].iloc[-1]
			if prot_last_appear['Status']=='Appeared':
				prot_isactive = prot_last_appear['Element']
				index = prot_last_appear.name

		return prot_isactive

	def check_remote_status(self, data:dict):
		"""
		Check LR status on an event parameterized in a dict (data), then return LR status and LR quality into one set.
		* INPUT
		data : dict of an event, required field [System Timestamp, B1, B2, B3]
		* OUTPUT
		lr_status :
		lr_quality :
		* SET
		lr_qualities : dict that store checked LR quality
		"""

		# Initialize
		lr_status = 'Remote'
		df_lr = self.soe_lr[(self.soe_lr['Tag']=='') & (self.soe_lr['B1']==data['b1']) & (self.soe_lr['B2']==data['b2']) & (self.soe_lr['B3']==data['b3']) & (self.soe_lr['Element']=='LR')]

		# Check LR quality in program buffer
		if (data['b1'], data['b2'], data['b3']) in self.lr_qualities:
			lr_quality = self.lr_qualities[(data['b1'], data['b2'], data['b3'])]
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
			self._lr_qualities[(data['b1'], data['b2'], data['b3'])] = lr_quality

		if lr_quality in ['good', 'bad']:
			# If quality good, filter only valid status
			if lr_quality=='good': df_lr = df_lr[df_lr['Status'].isin(['Remote', 'Local'])]

			if df_lr[join_datetime(df_lr['System time stamp'], df_lr['System milliseconds'])<data['t1']].shape[0]>0:
				# LR status changes occured before
				lr_last_change = df_lr[join_datetime(df_lr['System time stamp'], df_lr['System milliseconds'])<data['t1']].iloc[-1]
				lr_status = 'Remote' if lr_last_change['Status']=='Remote' else 'Local'
			else:
				# LR status changes occured after
				lr_first_change = df_lr[join_datetime(df_lr['System time stamp'], df_lr['System milliseconds'])>=data['t1']].iloc[0]
				lr_status = 'Local' if lr_first_change['Status']=='Remote' else 'Remote'

		return lr_status, lr_quality

	def check_synchro_interlock(self, data:dict):
		"""
		Check CSO status on an event parameterized in a dict (data), then return CSO status and CSO quality into one set.
		* INPUT
		data : dict of an event, required field [System Timestamp, B1, B2, B3]
		* OUTPUT
		cso_status :
		cso_quality :
		* SET
		cso_qualities :
		"""

		# Initialize
		cso_status = 'Off'
		df1 = self.soe_sync[(self.soe_sync['Tag']=='') & (self.soe_sync['B1']==data['b1']) & (self.soe_sync['B2']==data['b2']) & (self.soe_sync['B3']==data['b3']) & (self.soe_sync['Element']=='CSO')]

		if (data['b1'], data['b2'], data['b3']) in self.cso_qualities:
			# Check CSO quality in program buffer
			cso_quality = self.cso_qualities[(data['b1'], data['b2'], data['b3'])]
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
			self._cso_qualities[(data['b1'], data['b2'], data['b3'])] = cso_quality

		if cso_quality in ['good', 'bad']:
			# If quality good, filter only valid status
			if cso_quality=='good': df1 = df1[df1['Status'].isin(['On', 'Off'])]

			if df1[join_datetime(df1['System time stamp'], df1['System milliseconds'])<data['t1']].shape[0]>0:
				# CSO status changes occured before
				cso_last_change = df1[join_datetime(df1['System time stamp'], df1['System milliseconds'])<data['t1']].iloc[-1]
				cso_status = 'On' if cso_last_change['Status']=='On' else 'Off'
			else:
				# CSO status changes occured after
				cso_first_change = df1[join_datetime(df1['System time stamp'], df1['System milliseconds'])>=data['t1']].iloc[0]
				cso_status = 'Off' if cso_first_change['Status']=='On' else 'On'

		return cso_status, cso_quality

	def get_orders(self, start:datetime=None, stop:datetime=None):
		"""
		"""

		df = self.soe_all.copy()
		# Can be filtered with date
		if isinstance(start, datetime) and isinstance(stop, datetime):
			t0, t1 = self._set_range(start=start, stop=stop)
		else:
			t0, t1 = self._set_range(start=df['Time stamp'].min(), stop=df['Time stamp'].max())

		# Get His. Messages with order tag only
		orders = df[(df['A']=='') & (df['Element'].isin(self.rc_element)) & (df['Tag'].isin(self.order_tags)) & (df['Time stamp']>=t0) & (df['Time stamp']<=t1)]

		return orders

	def get_result(self, order:pd.Series, rc_list:list, note_list:list):
		"""
		Get result of RC Event execution ended within t_search duration.
		* INPUT
		index : index of RC Event with order tag
		* OUTPUT
		result_idx : index of RC Event with feedback tag
		rc_result : RC Event result Success / Failed / Uncertain
		* SET
		analyzed_rc_rows
		rcd
		soe
		note_list
		"""
		
		order_idx = order.name
		t_order, t_transmit = get_datetime(order)
		# t_order, t_transmit = get_datetime(self.soe_ctrl.loc[index])
		b1, b2, b3, elm, sts, tag, dis = order.loc[['B1', 'B2', 'B3', 'Element', 'Status', 'Tag', 'Operator']]
		# b1, b2, b3, elm, sts, tag, dis = self.soe_ctrl.loc[index, ['B1', 'B2', 'B3', 'Element', 'Status', 'Tag', 'Operator']]
		rc_result, t0, t1, t2 = 'UNCERTAIN', 0, 0, 0
		t_feedback = t_transmit
		prot_isactive = ''
		annotation = []
		mark_success, mark_failed, mark_unused = False, False, False

		txt_cd_anomaly = 'Status CD anomali'
		txt_lr_anomaly = 'Status LR anomali'
		txt_timestamp_anomaly = 'Anomali timestamp RTU'

		ifs_status_0, ifs_name_0 = self.check_ifs_status(data={'t1': t_order, 'b1': b1})
		if ifs_status_0=='Down':
			txt_ifs_before_rc = f'RC dalam kondisi IFS "{ifs_name_0}" {ifs_status_0}'
			annotation.append(txt_ifs_before_rc)
			self.soe_ctrl.at[order_idx, 'Comment'] += f'{txt_ifs_before_rc}\n'
		
		# TEST PROGRAMM
		invert_status = {'Open': 'Close', 'Close': 'Open'}

		# Notes for LR
		lr_status_0, lr_quality_0 = self.check_remote_status(data={'t1': t_order, 'b1': b1, 'b2': b2, 'b3': b3})
		if lr_quality_0=='good' and lr_status_0=='Local':
			txt_rc_at_local = f'Status LR {lr_status_0}'
			annotation.append(txt_rc_at_local)
			if txt_rc_at_local not in self.soe_ctrl.loc[order_idx, 'Comment']: self.soe_ctrl.at[order_idx, 'Comment'] += f'{txt_rc_at_local}\n'
		elif lr_quality_0=='bad':
			annotation.append(txt_lr_anomaly)
			if txt_lr_anomaly not in self.soe_ctrl.loc[order_idx, 'Comment']: self.soe_ctrl.at[order_idx, 'Comment'] += f'{txt_lr_anomaly}\n'

		# Notes for CD
		cd_status_0, cd_quality_0 = self.check_enable_status(data={'t1': t_order, 'b1': b1})
		if cd_quality_0=='good' and cd_status_0=='Disable':
			txt_rc_at_disable = f'Status CD {cd_status_0}'
			annotation.append(txt_rc_at_disable)
			if txt_rc_at_disable not in self.soe_ctrl.loc[order_idx, 'Comment']: self.soe_ctrl.at[order_idx, 'Comment'] += f'{txt_rc_at_disable}\n'
		elif cd_quality_0=='bad':
			annotation.append(txt_cd_anomaly)
			if txt_cd_anomaly not in self.soe_ctrl.loc[order_idx, 'Comment']: self.soe_ctrl.at[order_idx, 'Comment'] += f'{txt_cd_anomaly}\n'
			
		# Notes for CSO and protection status
		if sts=='Close':
			cso_status_0, cso_quality_0 = self.check_synchro_interlock(data={'t1': t_order, 'b1': b1, 'b2': b2, 'b3': b3})
			if cso_quality_0=='good':
				txt_rc_at_cso = f'Status CSO {cso_status_0}'
				annotation.append(txt_rc_at_cso)
				if txt_rc_at_cso not in self.soe_ctrl.loc[order_idx, 'Comment']: self.soe_ctrl.at[order_idx, 'Comment'] += f'{txt_rc_at_cso}\n'
			elif cso_quality_0=='bad':
				txt_cso_anomaly = 'Status CSO anomali'
				annotation.append(txt_cso_anomaly)
				if txt_cso_anomaly not in self.soe_ctrl.loc[order_idx, 'Comment']: self.soe_ctrl.at[order_idx, 'Comment'] += f'{txt_cso_anomaly}\n'
				
			prot_isactive = self.check_protection_interlock(data={'t1': t_order, 'b1': b1, 'b2': b2, 'b3': b3})
			if prot_isactive:
				txt_prot_active = f'Proteksi {prot_isactive} sedang aktif'
				annotation.append(txt_prot_active)
				if txt_prot_active not in self.soe_ctrl.loc[order_idx, 'Comment']: self.soe_ctrl.at[order_idx, 'Comment'] += f'{txt_prot_active}\n'

		# Sampling dataframe within t_search time
		df_range = self.soe_ctrl[(join_datetime(self.soe_ctrl['System time stamp'], self.soe_ctrl['System milliseconds'])>=t_order) & (join_datetime(self.soe_ctrl['System time stamp'], self.soe_ctrl['System milliseconds'])<=join_datetime(t_order, self.t_search*1000)) & (self.soe_ctrl['B1']==b1) & (self.soe_ctrl['B2']==b2) & (self.soe_ctrl['B3']==b3) & (self.soe_ctrl['Element']==elm)]

		# Get first feedback
		result_range = df_range[(df_range['Status']==sts) & (df_range['Tag'].isin(self.feedback_tags))][:1]

		if result_range.shape[0]>0:
			# Continue check feedback
			result_row = result_range.iloc[0]
			result_idx = result_row.name
			if 'R' in result_row['Tag']:
				rc_result = 'SUCCESS'
			else:
				rc_result = 'FAILED'

			t_feedback, t_receive = get_datetime(result_row)
			t0 = get_execution_duration(order, result_row)
			t1 = get_termination_duration(order, result_row)
			t2 = t1 - t0
			last_idx = result_idx

			# Check if t_feedback leading t_order
			if t0<0 or t2<0:
				annotation.append(txt_timestamp_anomaly)
				if txt_timestamp_anomaly not in self.soe_ctrl.loc[result_idx, 'Comment']: self.soe_ctrl.at[result_idx, 'Comment'] += f'{txt_timestamp_anomaly}\n'
		else:
			# Cut operation if no feedback found
			# Return order index with status UNCERTAIN
			last_idx = order_idx
			result_idx = order_idx

		final_result = rc_result

		if rc_result=='FAILED':
			anomaly_status = False
			no_status_changes = False

			# Check for IFS
			if ifs_status_0=='Up':
				ifs_status1, ifs_name1 = self.check_ifs_status(data={'t1': t_feedback, 'b1': b1})
				if ifs_status1=='Down':
					txt_ifs_after_rc = f'IFS "{ifs_name1}" {ifs_status1} sesaat setelah RC'
					annotation.append(txt_ifs_after_rc)
					self.soe_ctrl.at[order_idx, 'Comment'] += f'{txt_ifs_after_rc}\n'

			# Only Tag [OR, O*, RC, R*, ""] would pass
			df_failed = df_range[(join_datetime(df_range['System time stamp'], df_range['System milliseconds'])>t_order) & ((df_range['Tag'].isin(self.order_tags + ['RC', 'R*'])) | (df_range['Tag']==''))]

			# Check for normal status occurences
			if df_failed[(df_failed['Status'].isin(['Close', 'Open'])) & (df_failed['Tag']=='')].shape[0]>0:
				df_status_normal = df_failed[df_failed['Status'].isin(['Close', 'Open'])]
				first_change = df_status_normal.iloc[0]
				
				if first_change['Tag']=='':
					# Status changes after RC order
					t_delta = get_execution_duration(df_range.loc[order_idx], first_change)

					txt_list = []
					t_executed = join_datetime(*first_change.loc[['Time stamp', 'Milliseconds']].to_list())
					lr_status_1, lr_quality_1 = self.check_remote_status(data={'t1': t_executed, 'b1': b1, 'b2': b2, 'b3': b3})
					cd_status_1, cd_quality_1 = self.check_enable_status(data={'t1': t_executed, 'b1': b1})
					# if cd_quality_1=='good': txt_list.append(f'CD={cd_status_1}')
					
					if sts=='Close':
						cso_status_1, cso_quality_1 = self.check_synchro_interlock(data={'t1': t_executed, 'b1': b1, 'b2': b2, 'b3': b3})
						# if cso_quality_1=='good': txt_list.append(f'CSO={cso_status_1}')
						prot_isactive = self.check_protection_interlock(data={'t1': t_executed, 'b1': b1, 'b2': b2, 'b3': b3})
						if prot_isactive: txt_list.append(f'{prot_isactive}=Appeared')

					txt_additional = f' ({", ".join(txt_list)})' if len(txt_list)>0 else ''
					
					txt_status_result = ''
					if first_change['Status']==sts:
						# Valid status exists
						if lr_status_1=='Remote' and t_delta<=self.t_monitor[elm]:
							txt_status_result = f'Potensi RC sukses ({t_delta}s){txt_additional}'
						else:
							txt_status_result = f'Eksekusi lokal GI{txt_additional}'
					else:
						# Inverted status occured
						if lr_status_1=='Remote' and t_delta<=self.t_monitor[elm]:
							txt_status_result = f'RC {sts} tapi status balikan {first_change["Status"]}. Perlu ditelusuri!'
						else:
							anomaly_status = True

					if first_change.name>result_idx: last_idx = first_change.name
					if txt_status_result:
						annotation.append(txt_status_result)
						self.soe_ctrl.at[first_change.name, 'Comment'] += f'{txt_status_result}\n'
				else:
					# Another RC order tag
					no_status_changes = True
			else:
				anomaly_status = True

			# Check for anomaly status occurences
			if (no_status_changes or anomaly_status) and df_failed[df_failed['Status']=='Dist.'].shape[0]>0:
				isfeedback = True
				first_dist_change = df_failed[df_failed['Status']=='Dist.'].iloc[0]

				# Sampling for next order
				df_next_order = df_failed[df_failed['Tag'].isin(self.order_tags)]

				if df_next_order.shape[0]>0:
					# Check if dist. status occured after another RC order
					if df_next_order.iloc[0].name<first_dist_change.name: isfeedback = False

				if isfeedback:
					# Anomaly status occured
					t_delta = get_execution_duration(df_range.loc[order_idx], first_dist_change)

					txt_list = []
					t_executed = join_datetime(*first_dist_change.loc[['Time stamp', 'Milliseconds']].to_list())
					lr_status_1, lr_quality_1 = self.check_remote_status(data={'t1': t_executed, 'b1': b1, 'b2': b2, 'b3': b3})
					cd_status_1, cd_quality_1 = self.check_enable_status(data={'t1': t_executed, 'b1': b1})
					# if cd_quality_1=='good': txt_list.append(f'CD={cd_status_1}')
					
					if sts=='Close':
						cso_status_1, cso_quality_1 = self.check_synchro_interlock(data={'t1': t_executed, 'b1': b1, 'b2': b2, 'b3': b3})
						# if cso_quality_1=='good': txt_list.append(f'CSO={cso_status_1}')
						prot_isactive = self.check_protection_interlock(data={'t1': t_executed, 'b1': b1, 'b2': b2, 'b3': b3})
						if prot_isactive: txt_list.append(f'{prot_isactive}=Appeared')

					txt_additional = f' ({", ".join(txt_list)})' if len(txt_list)>0 else ''
					
					if lr_status_1=='Remote' and t_delta<=self.t_monitor[elm]:
						txt_status_anomaly = f'Potensi RC sukses, tapi status {sts} anomali ({t_delta}s){txt_additional}'
					else:
						txt_status_anomaly = f'Eksekusi lokal GI, tapi status {sts} anomali{txt_additional}'

					if first_dist_change.name>result_idx: last_idx = first_dist_change.name
					annotation.append(txt_status_anomaly)
					self.soe_ctrl.at[first_dist_change.name, 'Comment'] += f'{txt_status_anomaly}\n'

		# Copy User Comment if any
		user_comment = df_range.loc[df_range.index<=last_idx, 'User comment'].to_list()
		for cmt in user_comment:
			if cmt and '**' not in cmt:
				# Eleminate unnecessary character
				txt = re.sub('^\W*|\s*$', '', cmt)
				annotation.append(txt)

		# Event marked by user
		if self.unused_mark in df_range['User comment'].to_list() or 'notused' in df_range['User comment'].to_list():
			annotation.append('User menandai RC dianulir**')
			mark_unused = True
		elif self.success_mark in df_range['User comment'].to_list():
			final_result = 'SUCCESS'
			annotation.append('User menandai RC sukses**')
			mark_success = True
		elif self.failed_mark in df_range['User comment'].to_list():
			final_result = 'FAILED'
			annotation.append('User menandai RC gagal**')
			mark_failed = True

		self.soe_ctrl.loc[result_idx, 'RC Feedback'] = rc_result
		note_list.append(annotation)
		rc_list.append({
			'Order Time': t_order,
			'Feedback Time': t_feedback,
			'B1': b1,
			'B2': b2,
			'B3': b3,
			'Element': elm,
			'Status': sts,
			'Tag': tag,
			'Operator': dis,
			'Pre Result': rc_result,
			'Execution (s)': t0,
			'Termination (s)': t1,
			'TxRx (s)': t2,
			'Rep. Flag': '',
			'Marked Unused': '*' if mark_unused else '',
			'Marked Success': '*' if mark_success else '',
			'Marked Failed': '*' if mark_failed else '',
			'Final Result': final_result,
			'Navigation': (order_idx, result_idx)
		})

		return result_idx, final_result

	@property
	def analyzed(self):
		return self._analyzed

	@property
	def b1_list(self):
		return self._b1_list

	@property
	def b2_list(self):
		return self._b2_list

	@property
	def b3_list(self):
		return self._b3_list

	@property
	def cd_qualities(self):
		return self._cd_qualities

	@property
	def cso_qualities(self):
		return self._cso_qualities

	@property
	def feedback_tags(self):
		return self._feedback_tags

	@property
	def highest_index(self):
		return self._highest_index

	@property
	def ifs_list(self):
		return self._ifs_list

	@property
	def is_valid(self):
		return self._is_valid

	@property
	def lowest_index(self):
		return self._lowest_index

	@property
	def lr_qualities(self):
		return self._lr_qualities

	@property
	def order_tags(self):
		return self._order_tags

	@property
	def rc_indexes(self):
		return self._rc_indexes


class _RCDBaseCalculation:
	name = 'Remote Control SCADA'
	keep_duplicate = 'last'
	threshold_variable = 1

	def __init__(self, data:pd.DataFrame=None, calculate_bi:bool=False, check_repetition:bool=True, **kwargs):
		self._calculated = False
		self.check_repetition = check_repetition
		self.station = None
		self.bay = None
		self.operator = None
		self.rc_element = ['CB']

		if calculate_bi: self.rc_element += ['BI1', 'BI2']

		if data is not None: self.rcd_all = data

		if hasattr(self, 'rcd_all'): self.calculate(start=kwargs.get('start'), stop=kwargs.get('stop'))

	def _calculate(self, start:datetime, stop:datetime):
		"""
		Get aggregate data as per Station, Bay, and Operator, then return list of Excel worksheet name and Dataframe wrapped into dictionaries.
		* INPUT
		df : Dataframe of the analyzed RC result
		* OUTPUT
		summary : Immutable dictionary
		* SET
		station : Dataframe of grouped by Station (B1)
		bay : Dataframe of grouped by Bay (B1, B2, B3)
		operator : Dataframe of grouped by Dispatcher (Operator)
		"""

		def div(x, y, default:int=0):
			"""
			Formula : <x>/<y>, if error return <default> value
			"""
			try:
				if y==0:
					z = default
				else:
					z = x / y
			except Exception:
				z = default
			return z

		if isinstance(self.rcd_all, pd.DataFrame):
			# Can be filtered with date
			if isinstance(start, datetime) and isinstance(stop, datetime):
				t0, t1 = self._set_range(start=start, stop=stop)
			else:
				t0, t1 = self._set_range(start=self.rcd_all['Order Time'].min(), stop=self.rcd_all['Order Time'].max())
		else:
			raise AttributeError('Invalid data input.', name='rcd_all', obj=self)

		df = self.rcd_all.loc[(self.rcd_all['Order Time']>=t0) & (self.rcd_all['Order Time']<=t1)]
		df_pre = self._rcd_setup(df)

		self.pre_process = df
		self.station = self.group_station(df_pre)
		self.bay = self.group_bay(df_pre)
		self.operator = self.group(df_pre, ['Operator'])
		self._calculated = True

		# Calculate overall success rate
		rc_all = df.shape[0]
		rc_unused = df[df['Marked Unused']=='*'].shape[0]
		rc_valid = df_pre.shape[0]
		rc_repetition = df[df['Rep. Flag']=='*'].shape[0]
		rc_close = df_pre[df_pre['Status']=='Close'].shape[0]
		rc_open = df_pre[df_pre['Status']=='Open'].shape[0]
		rc_marked = df[(df['Marked Unused']=='*') | (df['Marked Success']=='*') | (df['Marked Failed']=='*')].shape[0]
		rc_marked_failed = df[df['Marked Failed']=='*'].shape[0]
		rc_marked_success = df[df['Marked Success']=='*'].shape[0]
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

	def _rcd_setup(self, df:pd.DataFrame, **kwargs):
		"""
		"""

		prepared = df.copy()

		# Filter only rows with not unused-marked
		prepared = prepared.loc[prepared['Marked Unused']=='']

		# Filter only rows without repetition-marked
		if self.check_repetition:
			prepared = prepared.loc[prepared['Rep. Flag']=='']

		return prepared

	def _set_attr(self, **kwargs):
		"""
		"""

		for key, val in kwargs.items():
			setattr(self, key, val)

	def _set_range(self, start:datetime, stop:datetime):
		"""
		Set start and stop date of query data.
		"""

		dt0 = start.replace(hour=0, minute=0, second=0, microsecond=0)
		dt1 = stop.replace(hour=23, minute=59, second=59, microsecond=999999)

		self._t0 = dt0
		self._t1 = dt1

		return dt0, dt1

	def calculate(self, start:datetime=None, stop:datetime=None):
		"""
		Extension of _calculate function.
		"""

		process_date = datetime.now()
		process_begin = time.time()
		self.result = self._calculate(start=start, stop=stop)
		process_stop = time.time()
		process_duration = round(process_stop - process_begin, 2)

		# Set attributes
		self._process_date = process_date
		self._process_begin = process_begin
		self._process_stop = process_stop
		self._process_duration = process_duration

	def generate_reference(self, soe:pd.DataFrame, rcd:pd.DataFrame):
		"""
		"""

		navs = []
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

	def group(self, df:pd.DataFrame, columns:list):
		"""
		Return DataFrame of aggregation values which used in all grouped Dataframe with groupby_columns as Columns parameter.
		"""

		groupby_columns = columns + ['Final Result']
		rc_count = df[groupby_columns].groupby(columns, as_index=False).count().rename(columns={'Final Result': 'RC Occurences'})
		rc_success = df.loc[(df['Final Result']=='SUCCESS'), groupby_columns].groupby(columns, as_index=False).count().rename(columns={'Final Result': 'RC Success'})
		rc_failed = df.loc[(df['Final Result']=='FAILED'), groupby_columns].groupby(columns, as_index=False).count().rename(columns={'Final Result': 'RC Failed'})

		df_groupby = rc_count.merge(right=rc_success, how='left', on=columns).merge(right=rc_failed, how='left', on=columns).fillna(0)
		df_groupby['Success Rate'] = np.round(df_groupby['RC Success']/df_groupby['RC Occurences'], 4)

		return df_groupby

	def group_station(self, df:pd.DataFrame):
		"""
		Return DataFrame for Station (columns = B1)
		"""

		columns = ['B1']
		groupby_columns = columns + ['Execution (s)', 'Termination (s)', 'TxRx (s)']
		df_groupby = self.group(df, columns)

		df_tmp = df.loc[df['Final Result']=='SUCCESS', groupby_columns].groupby(columns, as_index=False).mean().round(3).rename(columns={'Execution (s)': 'Execution Avg.', 'Termination (s)': 'Termination Avg.', 'TxRx (s)': 'TxRx Avg.'})
		df_groupby = df_groupby.merge(right=df_tmp, how='left', on=columns).fillna(0)

		return df_groupby

	def group_bay(self, df:pd.DataFrame):
		"""
		Return DataFrame for Bay (columns = B1, B2, B3)
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

	def prepare_export(self, generate_formula:bool=False, **kwargs):
		"""
		Applying excel formulas to output file
		"""

		if not self.calculated: raise SyntaxError('Jalankan calculate() terlebih dahulu!')

		df_rc = self.pre_process.copy()
		df_gi = self.station.copy()
		df_bay = self.bay.copy()
		df_opr = self.operator.copy()

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
			df_rc['Final Result'] = np.array([f'=IF(${xr["Marked Success"]}{row+2}="*", "SUCCESS",' +
						f'IF(${xr["Marked Failed"]}{row+2}="*", "FAILED",' +
						f'${xr["Pre Result"]}{row+2}))' for row in range(rlen)])
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

			if 'Navigation' in rc_columns:
				# Apply navigation hyperlink on sheet RC_ONLY
				df_rc['Navigation'] = self.generate_reference(soe=kwargs.get('soe'), rcd=df_rc)
			# Sheet GI
			for rowg in range(glen):
				rule_b1 = rule_lookup('B1', f'${xg["B1"]}{rowg+2}')
				rules = [rule_b1, rule_repetition, rule_unused]
				gi_update['RC Occurences'].append('=' + countifs(*rules))
				gi_update['RC Success'].append('=' + countifs(*rules, rule_lookup('Final Result', '"SUCCESS"')))
				gi_update['RC Failed'].append('=' + countifs(*rules, rule_lookup('Final Result', '"FAILED"')))
				gi_update['Success Rate'].append(f'=IFERROR(${xg["RC Success"]}{rowg+2}/${xg["RC Occurences"]}{rowg+2}, 0)')
				gi_update['Execution Avg.'].append(f'=IF(${xg["RC Success"]}{rowg+2}=0, 0, ' + averageifs(rule_lookup('Execution (s)'), *rules, rule_lookup('Final Result', '"SUCCESS"')) + ')')
				gi_update['Termination Avg.'].append(f'=IF(${xg["RC Success"]}{rowg+2}=0, 0, ' + averageifs(rule_lookup('Termination (s)'), *rules, rule_lookup('Final Result', '"SUCCESS"')) + ')')
				gi_update['TxRx Avg.'].append(f'=IF(${xg["RC Success"]}{rowg+2}=0, 0,  ' + averageifs(rule_lookup('TxRx (s)'), *rules, rule_lookup('Final Result', '"SUCCESS"')) + ')')

			gi_result = {
				'RC Occurences': [f'=SUM(${xg["RC Occurences"]}$2:${xg["RC Occurences"]}${glen+1})'],
				'RC Success': [f'=SUM(${xg["RC Success"]}$2:${xg["RC Success"]}${glen+1})'],
				'RC Failed': [f'=SUM(${xg["RC Failed"]}$2:${xg["RC Failed"]}${glen+1})'],
				'Success Rate': [f'=IFERROR(${xg["RC Success"]}{glen+2}/${xg["RC Occurences"]}{glen+2}, 0)'],
			}
			df_gi_result = pd.DataFrame(data=gi_result)
			# Sheet BAY
			for rowb in range(blen):
				rule_b1 = rule_lookup('B1', f'${xb["B1"]}{rowb+2}')
				rule_b2 = rule_lookup('B2', f'${xb["B2"]}{rowb+2}')
				rule_b3 = rule_lookup('B3', f'${xb["B3"]}{rowb+2}')
				rules = [rule_b1, rule_b2, rule_b3, rule_repetition, rule_unused]
				bay_update['RC Occurences'].append('=' + countifs(*rules))
				bay_update['RC Success'].append('=' + countifs(*rules, rule_lookup('Final Result', '"SUCCESS"')))
				bay_update['RC Failed'].append('=' + countifs(*rules, rule_lookup('Final Result', '"FAILED"')))
				bay_update['Success Rate'].append(f'=IFERROR(${xb["RC Success"]}{rowb+2}/${xb["RC Occurences"]}{rowb+2}, 0)')
				for status in ['Open', 'Close']:
					for result in ['Success', 'Failed']:
						bay_update[f'{status} {result}'].append('=' + countifs(*rules, rule_lookup('Status', f'"{status}"'), rule_lookup('Final Result', f'"{result.upper()}"')))
				bay_update['Contribution'].append(f'=IFERROR(${xb["RC Occurences"]}{rowb+2}/${xb["RC Occurences"]}${blen+2}, 0)')	# <rc occur>/<total rc occur>
				bay_update['Reduction'].append(f'=${xb["RC Failed"]}{rowb+2}/${xb["RC Occurences"]}${blen+2}')	# <rc failed>/<total rc occur>
				bay_update['Tagging'].append(f'=IF(IFERROR(${xb["Open Failed"]}{rowb+2}^2/(${xb["Open Failed"]}{rowb+2}+${xb["Open Success"]}{rowb+2}), 0)>{thd_var}, "O", "") & IF(IFERROR(${xb["Close Failed"]}{rowb+2}^2/(${xb["Close Failed"]}{rowb+2}+${xb["Close Success"]}{rowb+2}), 0)>{thd_var}, "C", "")')

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
				rule_operator = rule_lookup('Operator', f'${xo["Operator"]}{rowo+2}')
				rules = [rule_operator, rule_repetition, rule_unused]
				opr_update['RC Occurences'].append('=' + countifs(*rules))
				opr_update['RC Success'].append('=' + countifs(*rules, rule_lookup('Final Result', '"SUCCESS"')))
				opr_update['RC Failed'].append('=' + countifs(*rules, rule_lookup('Final Result', '"FAILED"')))
				opr_update['Success Rate'].append(f'=IFERROR(${xo["RC Success"]}{rowo+2}/${xo["RC Occurences"]}{rowo+2}, 0)')

			opr_result = {
				'RC Occurences': [f'=SUM(${xo["RC Occurences"]}$2:${xo["RC Occurences"]}${olen+1})'],
				'RC Success': [f'=SUM(${xo["RC Success"]}$2:${xo["RC Success"]}${olen+1})'],
				'RC Failed': [f'=SUM(${xo["RC Failed"]}$2:${xo["RC Failed"]}${olen+1})'],
				'Success Rate': [f'=IFERROR(${xo["RC Success"]}{olen+2}/${xo["RC Occurences"]}{olen+2}, 0)'],
			}
			df_opr_result = pd.DataFrame(data=opr_result)

			# Update new DataFrame
			df_gi.update(pd.DataFrame(gi_update))
			df_bay.update(pd.DataFrame(bay_update))
			df_opr.update(pd.DataFrame(opr_update))

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

	def print_result(self):
		"""
		Print summary in terminal
		"""

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
	def process_begin(self):
		return self._process_begin

	@property
	def process_date(self):
		return self._process_date

	@property
	def process_duration(self):
		return self._process_duration

	@property
	def process_stop(self):
		return self._process_stop

	@property
	def t0(self):
		return self._t0

	@property
	def t1(self):
		return self._t1


class RCD(_Export, _RCDBaseCalculation):

	def __init__(self, data:pd.DataFrame=None, calculate_bi:bool=False, check_repetition:bool=True, **kwargs):
		super().__init__(data, calculate_bi, check_repetition, **kwargs)


class SOEtoRCD(_Export, _SOEAnalyzer, _RCDBaseCalculation):

	def __init__(self, data:pd.DataFrame=None, calculate_bi:bool=False, check_repetition:bool=True, **kwargs):
		super().__init__(data, calculate_bi, check_repetition, **kwargs)

	def calculate(self, start:datetime=None, stop:datetime=None, force:bool=False):
		"""
		Override calculate function.
		"""

		process_date = datetime.now()
		process_begin = time.time()

		if not hasattr(self, 'rcd_all') or force:
			# Must be analyzed first and pass to rcd_all
			self.rcd_all = self.analyze(start=start, stop=stop)

		self.result = self._calculate(start=start, stop=stop)
		process_stop = time.time()
		process_duration = round(process_stop - process_begin, 2)

		# Set attributes
		self._process_date = process_date
		self._process_begin = process_begin
		self._process_stop = process_stop
		self._process_duration = process_duration

	def prepare_export(self, generate_formula:bool=False, **kwargs):
		"""
		Applying excel formulas to output file
		"""

		if not self.analyzed: raise SyntaxError('Jalankan calculate() terlebih dahulu!')

		soe = self.post_process.loc[(self.post_process['Time stamp']>=self.t0) & (self.post_process['Time stamp']<=self.t1)]
		# Define soe as reference on generating hyperlink in prepare_export()
		kwargs.update(soe=soe, generate_formula=generate_formula)

		return {
			'HIS_MESSAGES': soe,
			**super().prepare_export(**kwargs)
		}


class RCDCollective(RCFileReader, RCD):

	def __init__(self, filepaths:Union[str, list], **kwargs):
		super().__init__(filepaths, **kwargs)


class RCDFromOFDB(SpectrumOfdbClient, SOEtoRCD):

	def __init__(self, date_start:datetime, date_stop:datetime=None, **kwargs):
		super().__init__(date_start, date_stop, **kwargs)


class RCDFromFile(SpectrumFileReader, SOEtoRCD):

	def __init__(self, filepaths:Union[str, list], **kwargs):
		super().__init__(filepaths, **kwargs)


class RCDFromFile2(SurvalentFileReader, SOEtoRCD):

	def __init__(self, filepaths:Union[str, list], **kwargs):
		super().__init__(filepaths, **kwargs)


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

def test_analyze_file(**params):
	print(' TEST ANALYZE RCD '.center(60, '#'))
	rc = RCDFromFile('sample/sample_rc*.xlsx')
	rc.calculate()
	if 'y' in input('Export hasil test? [y/n]'):
		rc.to_excel('test_analyze_rcd_spectrum')
	return rc

def test_analyze_file2(**params):
	print(' TEST ANALYZE RCD '.center(60, '#'))
	rc = RCDFromFile2('sample/survalent/sample_soe*.xlsx')
	rc.calculate()
	if 'y' in input('Export hasil test? [y/n]'):
		rc.to_excel('test_analyze_rcd_survalent')
	return rc

def test_collective_file(**params):
	print(' TEST COLLECTIVE RCD '.center(60, '#'))
	rc = RCDCollective('sample/sample_rc*.xlsx')
	rc.calculate()
	if 'y' in input('Export hasil test? [y/n]'):
		rc.to_excel('test_collective_rcd')
	return rc


if __name__=='__main__':
	test_list = [
		('Test analisa file SOE Spectrum', test_analyze_file),
		('Test analisa file SOE Survalent', test_analyze_file2),
		('Test menggabungkan file', test_collective_file)
	]
	ans = input('Confirm troubleshooting? [y/n]  ')
	if ans=='y':
		print('\r\n'.join([f'  {no+1}.'.ljust(6) + tst[0] for no, tst in enumerate(test_list)]))
		choice = int(input(f'\r\nPilih modul test [1-{len(test_list)}] :  ')) - 1
		if choice in range(len(test_list)):
			rc = test_list[choice][1]()
		else:
			print('Pilihan tidak valid!')