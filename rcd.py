import os, platform, re, time
import numpy as np
import pandas as pd
import xlsxwriter
from datetime import date, datetime, timedelta
from filereader import SpectrumFileReader
from glob import glob
from global_parameters import RCD_BOOK_PARAM
from lib import get_datetime, get_execution_duration, get_termination_duration, join_datetime, immutable_dict, progress_bar
from ofdb import SpectrumOfdbClient
from pathlib import Path
from typing import Union
from xlsxwriter.utility import xl_col_to_name


class RCAnalyzer:

	def __init__(self, input:Union[SpectrumFileReader, SpectrumOfdbClient], calculate_bi=False, check_repetition=True, **kwargs):
		self._feedback_tags = ['RC', 'NE', 'R*', 'N*']
		self._order_tags = ['OR', 'O*']
		self._sheet_parameter = immutable_dict(RCD_BOOK_PARAM)
		self._analyzed = False
		self._cd_qualities = {}
		self._cso_qualities = {}
		self._lr_qualities = {}
		self.analyzed_messages = None
		self.analyzed_rc = None
		self.analyzed_station = None
		self.analyzed_bay = None
		self.analyzed_operator = None
		self.t_monitor = {'CB': 15, 'BI1': 30, 'BI2': 30}
		self.t_transition = {'CB': 1, 'BI1': 16, 'BI2': 16}
		self.t_search = 3*60*60
		self.rc_element = ['CB']
		self.success_mark = '**success**'
		self.failed_mark = '**failed**'
		self.unused_mark = '**unused**'
		self.keep_duplicate = 'last'
		self.check_repetition = check_repetition
		self.threshold_variable = 1

		if calculate_bi: self.rc_element += ['BI1', 'BI2']

		if hasattr(input, 'date_range'):
			self._date_start = input.date_range[0].to_pydatetime() if isinstance(input.date_range[0], pd.Timestamp) else input.date_range[0]
			self._date_stop = input.date_range[1].to_pydatetime() if isinstance(input.date_range[1], pd.Timestamp) else input.date_range[1]
			self.all_messages = input.soe_all.copy()
		else:
			raise AttributeError('Variable "date_range" belum diset.')

		self.base_dir = Path(__file__).parent.resolve()
		self.output_dir = self.base_dir / 'output'
		self.output_extension = 'xlsx'
		self.output_filename = f'RC_Output_{"_".join(self.rc_element)}_{self.date_start.strftime("%Y%m%d")}-{self.date_stop.strftime("%Y%m%d")}'
		self.sources = input.sources

		# Set date_range if defined in kwargs
		if 'date_start' in kwargs:
			self._date_start = kwargs['date_start']
			if 'date_stop' in kwargs:
				self._date_stop = kwargs['date_stop']

	def analyze(self):
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
		return_value : list of tuple of RC order index, RC feedback, and RC result pairs
		* SET
		analyzed_rc_rows
		analyzed_rc
		analyzed_messages
		note_list
		"""

		rc_list = []
		note_list = []
		rc_order_index = self.order_messages.index.to_list()
		date_origin = self.rc_messages.loc[rc_order_index[0], 'Time stamp']
		buffer1, buffer2 = {}, {}
		return_value = []

		print(f'\nMenganalisa {self.order_messages.shape[0]} kejadian RC...')
		for x, index in enumerate(rc_order_index):
			progress_bar((x+1)/len(rc_order_index))

			# index_0 = index of RC order Tag, index_1 = index of RC Feedback Tag, index_2 = index of next RC order Tag
			rc_order = self.rc_messages.loc[index]
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
								self.rc_messages.at[buffer1[bufkey][m], 'Comment'] += f'{comment_text}\n'
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
								self.rc_messages.at[bval[n], 'Comment'] += f'{comment_text}\n'
								note_list[buffer2[bkey][n]].insert(0, comment_text)

					# Reset buffer
					buffer1, buffer2 = {}, {}
					# Insert into new buffer
					if result in ['FAILED', 'UNCERTAIN']:
						buffer1[bufkey] = [index_0]
						buffer2[bufkey] = [x]

			return_value.append((index_0, index_1, result))
			# if result in ['FAILED', 'UNCERTAIN']: print(buffer1, '\n', buffer2)

		self.rc_list = rc_list
		self.analyzed_messages = pd.concat([self.rc_messages, self.lr_messages, self.cd_messages, self.sync_messages, self.prot_messages, self.ifs_messages], copy=False).drop_duplicates(keep='first').sort_values(['Time stamp', 'Milliseconds'])
		self.analyzed_rc = pd.DataFrame(data=rc_list)
		self.analyzed_rc['Annotations'] = list(map(lambda x: '\n'.join(list(map(lambda y: f'- {y}', x))), note_list))
		self._analyzed = True

		return return_value

	def calculate(self):
		"""
		"""
		
		try:
			self.filtered_messages = self.setup()
		except ValueError:
			return print('Program terhenti dengan error.')
		
		if self.is_valid:
			self._process_date = date.today()
			self._process_begin = time.time()

			# Check RC event count
			if self.order_messages.shape[0]>0:
				self._rc_indexes = self.analyze()
				self._summary = self.summary()
			else:
				return print('Tidak terdeteksi event RC.')
		
			self._process_stop = time.time()
			self._process_duration = round(self.process_stop - self.process_begin, 2)

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
		df_cd = self.cd_messages[(self.cd_messages['B1']==data['b1']) & (self.cd_messages['Element']=='CD') & (self.cd_messages['Tag']=='')]

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
			df_ifs = self.ifs_messages[(self.ifs_messages['B1']=='IFS') & (self.ifs_messages['B2']=='RTU_P1') & (self.ifs_messages['B3']==ifs_name) & (self.ifs_messages['Tag']=='')]
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
		df_prot = self.prot_messages[(self.prot_messages['Tag']=='') & (self.prot_messages['B1']==data['b1']) & (self.prot_messages['B2']==data['b2']) & (self.prot_messages['B3']==data['b3']) & (self.prot_messages['Element'].isin(['CBTR', 'MTO']))]

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
		df_lr = self.lr_messages[(self.lr_messages['Tag']=='') & (self.lr_messages['B1']==data['b1']) & (self.lr_messages['B2']==data['b2']) & (self.lr_messages['B3']==data['b3']) & (self.lr_messages['Element']=='LR')]

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
		df1 = self.sync_messages[(self.sync_messages['Tag']=='') & (self.sync_messages['B1']==data['b1']) & (self.sync_messages['B2']==data['b2']) & (self.sync_messages['B3']==data['b3']) & (self.sync_messages['Element']=='CSO')]

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

	def export_result(self, filename:str='', as_formula:bool=True):
		"""
		Export analyzed RC result into Excel file.
		* INPUT
		filename : set custom filename or leave blank ("") to use autogenerated name instead
		* OUTPUT
		export_result :
		"""
		
		# Check if RC Event has been analyzed
		if self.analyzed==False:
			return print('Tidak dapat export data Kalkulasi RC. Jalankan fungsi "calculate()" terlebih dahulu.')

		# Check required data
		try:
			worksheets = self.prepare_export(generate_formula=as_formula)
			statistic = self.result['statistic']
			overall = self.result['overall']
		except (AttributeError, KeyError, ValueError):
			return print('Tidak dapat export file RC.')
		
		# self.analyzed_messages.sort_values(['Time stamp', 'Milliseconds'], inplace=True)

		# Check target directory of output file
		if not os.path.isdir(self.output_dir): os.mkdir(self.output_dir)

		if filename:
			# Use defined filename
			if '.' in filename: filename = filename.split('.')[0]
		else:
			# Use default filename instead
			filename = self.output_filename
		
		file_list = glob(f'{self.output_dir}/{filename}*.{self.output_extension}')
		if len(file_list)>0: filename += f'_rev{len(file_list)}'

		output_file_properties = {
			'title': f'Hasil kalkulasi RC {" ".join(self.rc_element)} tanggal {self.date_start.strftime("%d-%m-%Y")} s/d {self.date_stop.strftime("%d-%m-%Y")}',
			'subject': 'Kalkulasi RC',
			'author': f'Python {platform.python_version()}',
			'manager': 'Fasop SCADA',
			'company': 'PLN UP2B Sistem Makassar',
			'category': 'Excel Automation',
			'comments': 'File digenerate otomatis oleh program Kalkulasi RC'
		}

		# Create excel file
		with xlsxwriter.Workbook(self.output_dir / f'{filename}.{self.output_extension}') as wb:
			# Set excel workbook file properties
			wb.set_properties(output_file_properties)
			
			# Write column to 
			# rc_only_extension_col = [f'=HYPERLINK("#HIS_MESSAGES!A{df_historical.index.get_loc(istart)+2}:T{df_historical.index.get_loc(istop)+2}","  CARI >> ")' for istart, istop, iresult in index_list]
			rc_only_extension_col = []
			for name, sheet in worksheets.items():
				self.worksheet_writer(wb, name, sheet, rc_only_extension_col)

			# Write worksheet info
			ws_info = wb.add_worksheet('Info')
			rows = [
				('Source File', self.sources),
				('Output File', f'{filename}.{self.output_extension}'),
				('RC Date Range', f'{self.date_start.strftime("%d-%m-%Y")} s/d {self.date_stop.strftime("%d-%m-%Y")}'),
				('Processed Date', datetime.now().strftime('%d-%m-%Y %H:%M:%S')),
				('Execution Time', f'{self.process_duration}s'),
				('PC', platform.node()),
				('User', os.getlogin()),
				('', ''),
				('SETTING', ''),
				('RC Element', ', '.join(self.rc_element)),
				('RC Repetition', 'last-occurrence-only' if self.check_repetition else 'calculate-all'),
				('Threshold (default=1)', self.threshold_variable),
				('', ''),
				('SUMMARY', ''),
				('Success Percentage', overall['percentage']),
				('Success Percentage (Close)', statistic['operation']['close_success_percentage']),
				('Success Percentage (Open)', statistic['operation']['open_success_percentage']),
				('', ''),
				('STATISTICS', ''),
				('Marked', statistic['total_marked']),
				('Unused-marked', statistic['marked']['unused']),
				('Success-marked', statistic['marked']['success']),
				('Failed-marked', statistic['marked']['failed']),
				('RC Event', statistic['total_event']),
				('CD Event', statistic['cd_event']),
				('LR Event', statistic['lr_event']),
				('Sync. Switch Event', statistic['sync_event']),
				('Protection Event', statistic['prot_event']),
				('RTU Up/Down Event', statistic['updown_event']),
			]
			for i, row in enumerate(rows):
				ws_info.write_row(i, 0, row)
			ws_info.set_column(0, 0, 20, wb.add_format({'valign': 'vcenter', 'num_format': '@', 'bold': True}))
			ws_info.set_column(1, 1, 100, wb.add_format({'valign': 'vcenter', 'num_format': '@', 'text_wrap': True}))
			ws_info.autofit()

		print(f'Data berhasil di-export pada "{self.output_dir / filename}.{self.output_extension}".')

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
		analyzed_rc
		analyzed_messages
		note_list
		"""
		
		order_idx = order.name
		t_order, t_transmit = get_datetime(order)
		# t_order, t_transmit = get_datetime(self.rc_messages.loc[index])
		b1, b2, b3, elm, sts, tag, dis = order.loc[['B1', 'B2', 'B3', 'Element', 'Status', 'Tag', 'Operator']]
		# b1, b2, b3, elm, sts, tag, dis = self.rc_messages.loc[index, ['B1', 'B2', 'B3', 'Element', 'Status', 'Tag', 'Operator']]
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
			self.rc_messages.at[order_idx, 'Comment'] += f'{txt_ifs_before_rc}\n'
		
		# TEST PROGRAMM
		invert_status = {'Open': 'Close', 'Close': 'Open'}

		# Notes for LR
		lr_status_0, lr_quality_0 = self.check_remote_status(data={'t1': t_order, 'b1': b1, 'b2': b2, 'b3': b3})
		if lr_quality_0=='good' and lr_status_0=='Local':
			txt_rc_at_local = f'Status LR {lr_status_0}'
			annotation.append(txt_rc_at_local)
			if txt_rc_at_local not in self.rc_messages.loc[order_idx, 'Comment']: self.rc_messages.at[order_idx, 'Comment'] += f'{txt_rc_at_local}\n'
		elif lr_quality_0=='bad':
			annotation.append(txt_lr_anomaly)
			if txt_lr_anomaly not in self.rc_messages.loc[order_idx, 'Comment']: self.rc_messages.at[order_idx, 'Comment'] += f'{txt_lr_anomaly}\n'

		# Notes for CD
		cd_status_0, cd_quality_0 = self.check_enable_status(data={'t1': t_order, 'b1': b1})
		if cd_quality_0=='good' and cd_status_0=='Disable':
			txt_rc_at_disable = f'Status CD {cd_status_0}'
			annotation.append(txt_rc_at_disable)
			if txt_rc_at_disable not in self.rc_messages.loc[order_idx, 'Comment']: self.rc_messages.at[order_idx, 'Comment'] += f'{txt_rc_at_disable}\n'
		elif cd_quality_0=='bad':
			annotation.append(txt_cd_anomaly)
			if txt_cd_anomaly not in self.rc_messages.loc[order_idx, 'Comment']: self.rc_messages.at[order_idx, 'Comment'] += f'{txt_cd_anomaly}\n'
			
		# Notes for CSO and protection status
		if sts=='Close':
			cso_status_0, cso_quality_0 = self.check_synchro_interlock(data={'t1': t_order, 'b1': b1, 'b2': b2, 'b3': b3})
			if cso_quality_0=='good':
				txt_rc_at_cso = f'Status CSO {cso_status_0}'
				annotation.append(txt_rc_at_cso)
				if txt_rc_at_cso not in self.rc_messages.loc[order_idx, 'Comment']: self.rc_messages.at[order_idx, 'Comment'] += f'{txt_rc_at_cso}\n'
			elif cso_quality_0=='bad':
				txt_cso_anomaly = 'Status CSO anomali'
				annotation.append(txt_cso_anomaly)
				if txt_cso_anomaly not in self.rc_messages.loc[order_idx, 'Comment']: self.rc_messages.at[order_idx, 'Comment'] += f'{txt_cso_anomaly}\n'
				
			prot_isactive = self.check_protection_interlock(data={'t1': t_order, 'b1': b1, 'b2': b2, 'b3': b3})
			if prot_isactive:
				txt_prot_active = f'Proteksi {prot_isactive} sedang aktif'
				annotation.append(txt_prot_active)
				if txt_prot_active not in self.rc_messages.loc[order_idx, 'Comment']: self.rc_messages.at[order_idx, 'Comment'] += f'{txt_prot_active}\n'

		# Sampling dataframe within t_search time
		df_range = self.rc_messages[(join_datetime(self.rc_messages['System time stamp'], self.rc_messages['System milliseconds'])>=t_order) & (join_datetime(self.rc_messages['System time stamp'], self.rc_messages['System milliseconds'])<=join_datetime(t_order, self.t_search*1000)) & (self.rc_messages['B1']==b1) & (self.rc_messages['B2']==b2) & (self.rc_messages['B3']==b3) & (self.rc_messages['Element']==elm)]

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
				if txt_timestamp_anomaly not in self.rc_messages.loc[result_idx, 'Comment']: self.rc_messages.at[result_idx, 'Comment'] += f'{txt_timestamp_anomaly}\n'
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
					self.rc_messages.at[order_idx, 'Comment'] += f'{txt_ifs_after_rc}\n'

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
						self.rc_messages.at[first_change.name, 'Comment'] += f'{txt_status_result}\n'
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
					self.rc_messages.at[first_dist_change.name, 'Comment'] += f'{txt_status_anomaly}\n'

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

		self.rc_messages.loc[result_idx, 'RC Feedback'] = rc_result
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
			'Final Result': final_result
		})

		return result_idx, final_result

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

	def prepare_export(self, generate_formula:bool=False):
		"""
		Applying excel formulas to output file
		"""

		df_his = self.analyzed_messages
		df_rc = self.analyzed_rc.copy()
		df_gi = self.analyzed_station.copy()
		df_bay = self.analyzed_bay.copy()
		df_opr = self.analyzed_operator.copy()

		# Apply navigation hyperlink on sheet RC_ONLY
		df_rc['Navigation'] = np.array([f'=HYPERLINK("#HIS_MESSAGES!A{df_his.index.get_loc(istart)+2}:T{df_his.index.get_loc(istop)+2}","CARI >>")' for istart, istop, iresult in self.rc_indexes])

		if generate_formula:
			rc_columns = df_rc.columns.to_list()
			gi_columns = df_gi.columns.to_list()
			bay_columns = df_bay.columns.to_list()
			opr_columns = df_opr.columns.to_list()
			rlen = df_rc.shape[0]
			blen = df_bay.shape[0]

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
				return f'AVERAGEIFS({range}, {ruleset(*rules)})'

			# Create dict of excel column label
			xr = {col: xl_col_to_name(rc_columns.index(col)) for col in rc_columns}
			xg = {col: xl_col_to_name(gi_columns.index(col)) for col in gi_columns}
			xb = {col: xl_col_to_name(bay_columns.index(col)) for col in bay_columns}
			xo = {col: xl_col_to_name(opr_columns.index(col)) for col in opr_columns}

			gi_update = {'RC Occurences': [], 'RC Success': [], 'RC Failed': [], 'Success Rate': [], 'Execution Avg.': [], 'Termination Avg.': [], 'TxRx Avg.': []}
			bay_update = {'RC Occurences': [], 'RC Success': [], 'RC Failed': [], 'Success Rate': [], 'Open Success': [], 'Open Failed': [], 'Close Success': [], 'Close Failed': [], 'Contribution': [], 'Reduction': [], 'Tagging': []}
			opr_update = {'RC Occurences': [], 'RC Success': [], 'RC Failed': [], 'Success Rate': []}
			
			# Define excel formula rule
			rule_repetition = rule_lookup('Rep. Flag', '""')
			rule_unused = rule_lookup('Marked Unused', '""')

			# Apply excel formula as string
			# Sheet RC_ONLY
			df_rc['Final Result'] = np.array([f'=IF(${xr["Marked Success"]}{row+2}="*", "SUCCESS",' +
						f'IF(${xr["Marked Failed"]}{row+2}="*", "FAILED",' +
						f'${xr["Pre Result"]}{row+2}))' for row in range(rlen)])
			# Sheet GI
			for rowg in range(df_gi.shape[0]):
				rule_b1 = rule_lookup('B1', f'${xg["B1"]}{rowg+2}')
				rules = [rule_b1, rule_repetition, rule_unused]
				gi_update['RC Occurences'].append('=' + countifs(*rules))
				gi_update['RC Success'].append('=' + countifs(*rules, rule_lookup('Final Result', '"SUCCESS"')))
				gi_update['RC Failed'].append('=' + countifs(*rules, rule_lookup('Final Result', '"FAILED"')))
				gi_update['Success Rate'].append(f'=${xg["RC Success"]}{rowg+2}/${xg["RC Occurences"]}{rowg+2}')
				gi_update['Execution Avg.'].append(f'=IF(${xg["RC Success"]}{rowg+2}=0, 0, ' + averageifs(rule_lookup('Execution (s)'), *rules, rule_lookup('Final Result', '"SUCCESS"')) + ')')
				gi_update['Termination Avg.'].append(f'=IF(${xg["RC Success"]}{rowg+2}=0, 0, ' + averageifs(rule_lookup('Termination (s)'), *rules, rule_lookup('Final Result', '"SUCCESS"')) + ')')
				gi_update['TxRx Avg.'].append(f'=IF(${xg["RC Success"]}{rowg+2}=0, 0,  ' + averageifs(rule_lookup('TxRx (s)'), *rules, rule_lookup('Final Result', '"SUCCESS"')) + ')')
			# Sheet BAY
			for rowb in range(df_bay.shape[0]):
				rule_b1 = rule_lookup('B1', f'${xb["B1"]}{rowb+2}')
				rule_b2 = rule_lookup('B2', f'${xb["B2"]}{rowb+2}')
				rule_b3 = rule_lookup('B3', f'${xb["B3"]}{rowb+2}')
				rules = [rule_b1, rule_b2, rule_b3, rule_repetition, rule_unused]
				bay_update['RC Occurences'].append('=' + countifs(*rules))
				bay_update['RC Success'].append('=' + countifs(*rules, rule_lookup('Final Result', '"SUCCESS"')))
				bay_update['RC Failed'].append('=' + countifs(*rules, rule_lookup('Final Result', '"FAILED"')))
				bay_update['Success Rate'].append(f'=${xb["RC Success"]}{rowb+2}/${xb["RC Occurences"]}{rowb+2}')
				for status in ['Open', 'Close']:
					for result in ['Success', 'Failed']:
						bay_update[f'{status} {result}'].append('=' + countifs(*rules, rule_lookup('Status', f'"{status}"'), rule_lookup('Final Result', f'"{result.upper()}"')))
				bay_update['Contribution'].append(f'=${xb["RC Occurences"]}{rowb+2}/${xb["RC Occurences"]}${blen+2}')	# <rc occur>/<total rc occur>
				bay_update['Reduction'].append(f'=${xb["RC Failed"]}{rowb+2}/${xb["RC Occurences"]}${blen+2}')	# <rc failed>/<total rc occur>
				bay_update['Tagging'].append(f'=IF(IFERROR(${xb["Open Failed"]}{rowb+2}^2/(${xb["Open Failed"]}{rowb+2}+${xb["Open Success"]}{rowb+2}), 0)>{thd_var}, "O", "") & IF(IFERROR(${xb["Close Failed"]}{rowb+2}^2/(${xb["Close Failed"]}{rowb+2}+${xb["Close Success"]}{rowb+2}), 0)>{thd_var}, "C", "")')
			# Sheet DISPATCHER
			for rowo in range(df_opr.shape[0]):
				rule_operator = rule_lookup('Operator', f'${xo["Operator"]}{rowo+2}')
				rules = [rule_operator, rule_repetition, rule_unused]
				opr_update['RC Occurences'].append('=' + countifs(*rules))
				opr_update['RC Success'].append('=' + countifs(*rules, rule_lookup('Final Result', '"SUCCESS"')))
				opr_update['RC Failed'].append('=' + countifs(*rules, rule_lookup('Final Result', '"FAILED"')))
				opr_update['Success Rate'].append(f'=${xo["RC Success"]}{rowo+2}/${xo["RC Occurences"]}{rowo+2}')

			# Update new DataFrame
			df_gi.update(pd.DataFrame(gi_update))
			df_bay.update(pd.DataFrame(bay_update))
			df_opr.update(pd.DataFrame(opr_update))

			# Update summary information
			self._summary['statistic']['total_repetition'] = f'=COUNTIF({rule_repetition.split(",")[0]}, "*")'
			self._summary['statistic']['total_valid'] = f'=COUNTIFS({rule_unused}, {rule_repetition})'
			self._summary['statistic']['total_marked'] = f'=COUNTIF(RC_ONLY!${xr["Marked Unused"]}$2:{xr["Marked Failed"]}${rlen+1}, "*")'
			self._summary['statistic']['marked']['unused'] = f'=COUNTIF({rule_unused.split(",")[0]}, "*")'
			self._summary['statistic']['marked']['success'] = f'=COUNTIF(RC_ONLY!${xr["Marked Success"]}$2:{xr["Marked Success"]}${rlen+1}, "*")'
			self._summary['statistic']['marked']['failed'] = f'=COUNTIF(RC_ONLY!${xr["Marked Failed"]}$2:{xr["Marked Failed"]}${rlen+1}, "*")'
			self._summary['statistic']['operation']['close_success_percentage'] = f'=ROUND(BAY!${xb["Close Success"]}${blen+2}/(BAY!${xb["Close Success"]}${blen+2}+BAY!${xb["Close Failed"]}${blen+2})*100, 2) & "%"'
			self._summary['statistic']['operation']['open_success_percentage'] = f'=ROUND(BAY!${xb["Open Success"]}${blen+2}/(BAY!${xb["Open Success"]}${blen+2}+BAY!${xb["Open Failed"]}${blen+2})*100, 2) & "%"'
			self._summary['overall']['percentage'] = f'=ROUND(BAY!${xb["Success Rate"]}${blen+2}*100, 2) & "%"'

		return {
			'HIS_MESSAGES': df_his,
			'RC_ONLY': df_rc,
			'GI': df_gi,
			'BAY': df_bay,
			'DISPATCHER': df_opr
		}

	def print_result(self):
		"""
		Print summary in terminal
		"""

		width, height = os.get_terminal_size()
		
		# Check if RC Event has been analyzed
		if self.analyzed==False:
			return print('Tidak dapat menampilkan hasil Kalkulasi RC. Jalankan fungsi "calculate()" terlebih dahulu.')
		
		df_gi = self.analyzed_station.copy()
		df_gi['Success Rate'] = df_gi['Success Rate'].map(lambda x: round(x*100, 2))
		df_bay = self.analyzed_bay.copy()
		df_bay['Success Rate'] = df_bay['Success Rate'].map(lambda x: round(x*100, 2))
		df_dispa = self.analyzed_operator.copy()
		df_dispa['Success Rate'] = df_dispa['Success Rate'].map(lambda x: round(x*100, 2))

		context = {
			'date_end': self.date_stop.strftime("%d-%m-%Y"),
			'date_start': self.date_start.strftime("%d-%m-%Y"),
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

	def setup(self):
		"""
		Preparing Dataframe joins from multiple file with sorting and filtering to get clean data.
		* INPUT
		all_messages :
		* OUTPUT
		df :
		* SET
		date_start :
		date_stop :
		highest_index :
		lowest_index :
		order_messages : Dataframe filter for His. Messages with "Order" tag only
		b*_list : list of unique value of field B1, B2, B3
		ifs_messages : Dataframe filter for His. Messages of IFS Status only
		ifs_list :
		ifs_name_matching :
		rc_messages : Dataframe of RC element status changes only
		cd_messages : Dataframe of CD status only
		lr_messages : Dataframe of LR status only
		sync_messages : Dataframe of CSO status only
		prot_messages : Dataframe of protection alarm only
		"""

		if isinstance(self.all_messages, pd.DataFrame):
			self._is_valid = True
			df = self.all_messages.copy()
			df = df.sort_values(['System time stamp', 'System milliseconds', 'Time stamp', 'Milliseconds'], ascending=[True, True, True, True]).reset_index(drop=True)

			# Get min index and max index of df
			self._lowest_index, self._highest_index = df.index.min(), df.index.max()

			# Get His. Messages with order tag only
			self.order_messages = df[(df['A']=='') & (df['Element'].isin(self.rc_element)) & (df['Tag'].isin(self.order_tags))]
			self._b1_list, self._b2_list, self._b3_list = self.order_messages['B1'].unique(), self.order_messages['B2'].unique(), self.order_messages['B3'].unique()
			
			ifs_messages = df[(df['A']=='') & (df['B1']=='IFS') & (df['B2']=='RTU_P1') & (df['Tag']=='')]
			# Get IFS name matching
			self._ifs_list = ifs_messages['B3'].unique()
			# Filter IFS messages only if related to RC Event
			self.ifs_messages = ifs_messages[ifs_messages['B3'].isin(self.b1_list)]

			# Filter His. Messages only if related to RC Event's B1, B2 and B3
			df = df[(df['A']=='') & (df['B1'].isin(self.b1_list)) & (df['B2'].isin(self.b2_list)) & (df['B3'].isin(self.b3_list))]

			# Reset comment column and search for order tag for RC element
			self.rc_messages = df[(df['Element'].isin(self.rc_element)) & (df['Status'].isin(['Open', 'Close', 'Dist.']))].copy()
			self.rc_messages['RC Order'] = np.where((self.rc_messages['Element'].isin(self.rc_element)) & (self.rc_messages['Tag'].isin(self.order_tags)), 'REMOTE', '')
			self.rc_messages['RC Feedback'] = ''
			self.rc_messages['Comment'] = ''

			# Split into DataFrames for each purposes, not reset index
			self.cd_messages = df[df['Element']=='CD'].copy()
			self.lr_messages = df[df['Element']=='LR'].copy()
			self.sync_messages = df[df['Element']=='CSO'].copy()
			self.prot_messages = df[df['Element'].isin(['CBTR', 'MTO'])].copy()
		else:
			self._is_valid = False
			df = None
			print('Dataframe tidak valid')
			raise ValueError
		
		return df

	def summary(self):
		"""
		Get aggregate data as per Station, Bay, and Operator, then return list of Excel worksheet name and Dataframe wrapped into dictionaries.
		* INPUT
		df : Dataframe of the analyzed RC result
		* OUTPUT
		summary : Immutable dictionary
		* SET
		analyzed_station : Dataframe of grouped by Station (B1)
		analyzed_bay : Dataframe of grouped by Bay (B1, B2, B3)
		analyzed_operator : Dataframe of grouped by Dispatcher (Operator)
		"""

		df = self.analyzed_rc
		df_filtered = self.used_rc

		self.analyzed_station = self.group_station(df_filtered)
		self.analyzed_bay = self.group_bay(df_filtered)
		self.analyzed_operator = self.group(df_filtered, ['Operator'])

		# Calculate overall success rate
		rc_all = df.shape[0]
		rc_unused = df[df['Marked Unused']=='*'].shape[0]
		rc_valid = df_filtered.shape[0]
		rc_repetition = df[df['Rep. Flag']=='*'].shape[0]
		rc_close = df_filtered[df_filtered['Status']=='Close'].shape[0]
		rc_open = df_filtered[df_filtered['Status']=='Open'].shape[0]
		rc_marked = df[(df['Marked Unused']=='*') | (df['Marked Success']=='*') | (df['Marked Failed']=='*')].shape[0]
		rc_marked_failed = df[df['Marked Failed']=='*'].shape[0]
		rc_marked_success = df[df['Marked Success']=='*'].shape[0]
		rc_failed = self.analyzed_bay['RC Failed'].sum()
		rc_failed_close = self.analyzed_bay['Close Failed'].sum()
		rc_failed_open = self.analyzed_bay['Open Failed'].sum()
		rc_success = self.analyzed_bay['RC Success'].sum()
		rc_success_close = self.analyzed_bay['Close Success'].sum()
		rc_success_open = self.analyzed_bay['Open Success'].sum()
		rc_percentage = round(rc_success/rc_valid*100, 2)
		rc_percentage_close = round(rc_success_close/rc_close*100, 2)
		rc_percentage_open = round(rc_success_open/rc_open*100, 2)

		return {
			'overall': {
				'total': rc_valid,
				'success': rc_success,
				'failed': rc_failed,
				'percentage': f'{rc_percentage}%'
			},
			'statistic': {
				'total_event': rc_all,
				'total_repetition': rc_repetition,
				'total_marked': rc_marked,
				'total_valid': rc_valid,
				'marked': {'unused': rc_unused, 'success': rc_marked_success, 'failed': rc_marked_failed},
				'operation': {'close': rc_close, 'close_failed': rc_failed_close, 'close_success': rc_success_close, 'close_success_percentage': f'{rc_percentage_close}%', 'open': rc_open, 'open_failed': rc_failed_open, 'open_success': rc_success_open, 'open_success_percentage': f'{rc_percentage_open}%'},
				'cd_event': self.cd_messages.shape[0],
				'lr_event': self.lr_messages.shape[0],
				'prot_event': self.prot_messages.shape[0],
				'sync_event': self.sync_messages.shape[0],
				'updown_event': self.ifs_messages.shape[0]
			}
		}

	def worksheet_writer(self, workbook:xlsxwriter.Workbook, sheet_name:str, sheet_data:pd.DataFrame, *args):
		"""
		This method parameterized each exported worksheet.
		* INPUT
		workbook :
		df_sheet :
		* OUTPUT
		"""

		ws = workbook.add_worksheet(sheet_name)

		# Worksheet formatting
		format_header = {'border': 1, 'bold': True, 'align': 'center', 'bg_color': '#ededed'}
		format_base = {'valign': 'vcenter'}
		format_row_summary = {'bold': True, 'bg_color': '#dcdcdc'}

		nrow, ncol = sheet_data.shape
		tbl_header = sheet_data.columns.to_list()
		
		# Write worksheet header
		if sheet_name=='RC_ONLY':
			ws.write_row(0, 0, tbl_header, workbook.add_format({**format_header, 'valign': 'top', 'text_wrap': True}))
		else:
			ws.write_row(0, 0, tbl_header, workbook.add_format(format_header))

		for x, col in enumerate(tbl_header):
			# Write table body
			ws.write_column(1, x, sheet_data[col].fillna(''), workbook.add_format({**format_base, ** self._sheet_parameter['format'].get(col, {})}))
			ws.set_column(x, x, self._sheet_parameter['width'].get(col))

			if sheet_name=='RC_ONLY':
				xlcol = xl_col_to_name(x)

				# Write additional summary rows
				if col=='Operator':
					ws.write_column(nrow+1, x, ['TOTAL RC (RAW)', 'SUCCESS (RAW)', 'FAILED (RAW)', 'SUCCESS RATE'], workbook.add_format(format_row_summary))
				elif col=='Pre Result':
					formula = [
						f'=COUNTA(${xlcol}$2:${xlcol}${nrow+1})',
						f'=COUNTIF(${xlcol}2:{xlcol}${nrow+1}, "SUCCESS")',
						f'=COUNTIF(${xlcol}2:{xlcol}${nrow+1}, "FAILED")',
						f'=ROUND(${xlcol}${nrow+3}/${xlcol}${nrow+2}*100, 2)'
					]
					ws.write_column(nrow+1, x, formula, workbook.add_format({**format_row_summary, 'align': 'center'}))
				else:
					ws.write_column(nrow+1, x, ['']*4, workbook.add_format({**format_row_summary, 'align': 'center'}))
			elif sheet_name!='HIS_MESSAGES':
				# Write additional summary row
				cell_sum = ['RC Occurences', 'RC Success', 'RC Failed', 'Open Success', 'Open Failed', 'Close Success', 'Close Failed']

				if col=='Success Rate':
					frmt = {'num_format': '0.00%', 'align': 'center'}
					ws.write(nrow+1, x, f'=ROUND({xl_col_to_name(x-2)}{nrow+2}/{xl_col_to_name(x-3)}{nrow+2}, 4)', workbook.add_format({**format_row_summary, **frmt}))
				elif col in cell_sum:
					frmt = {'num_format': '0', 'align': 'center'}
					ws.write(nrow+1, x, f'=SUM({xl_col_to_name(x)}2:{xl_col_to_name(x)}{nrow+1})', workbook.add_format({**format_row_summary, **frmt}))
				else:
					ws.write(nrow+1, x, '', workbook.add_format(format_row_summary))

		# Set worksheet general parameter
		ws.set_paper(9)	# 9 = A4
		ws.set_landscape()
		ws.set_margins(0.25)
		ws.center_horizontally()
		ws.print_area(0, 0, nrow, ncol-1)
		ws.autofilter(0, 0, 0, ncol-1)
		ws.autofit()
		ws.freeze_panes(1, 0)
		ws.ignore_errors({'number_stored_as_text': f'A:{xl_col_to_name(ncol-1)}'})

		for y, col1 in enumerate(tbl_header):
			if col1 in self._sheet_parameter['width']: ws.set_column(y, y, self._sheet_parameter['width'].get(col1))


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
	def date_start(self):
		return self._date_start

	@property
	def date_stop(self):
		return self._date_stop
	
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
	def rc_indexes(self):
		return self._rc_indexes

	@property
	def result(self):
		if self.analyzed:
			return immutable_dict(self._summary)
		else:
			print('Data belum tersedia! Jalankan fungsi "calculate()" terlebih dahulu.')
			return None

	@property
	def used_rc(self):
		if isinstance(self.analyzed_rc, pd.DataFrame):
			# Filter only rows with not unused-marked
			df = self.analyzed_rc.loc[self.analyzed_rc['Marked Unused']=='']

			# Filter only rows without repetition-marked
			if self.check_repetition:
				df = df.loc[df['Rep. Flag']=='']

			return df
		else:
			return None


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

def main():
	filepaths = input('Lokasi file : ')
	rc = RCAnalyzer(filepaths)
	rc.calculate()
	rc.export_result()


if __name__=='__main__':
	main()