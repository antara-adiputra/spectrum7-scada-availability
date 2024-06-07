import datetime, gc, re, time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from types import MappingProxyType
from typing import Any, Union

import numpy as np
import pandas as pd
from xlsxwriter.utility import xl_col_to_name
from filereader import AVRSFileReader, SpectrumFileReader
from global_parameters import RTU_BOOK_PARAM
from lib import CONSOLE_WIDTH, BaseExportMixin, calc_time, join_datetime, immutable_dict, load_cpoint, progress_bar, timedelta_split
from ofdb import SpectrumOfdbClient


class _Export(BaseExportMixin):
	_sheet_parameter: MappingProxyType[str, dict[str, Any]] = immutable_dict(RTU_BOOK_PARAM)
	output_prefix: str = 'AVRS'
	result: dict[str, dict[str, Any]]
	
	def get_sheet_info_data(self, **kwargs):
		"""Define extra information into sheet "Info"."""
		extra_info = [
			*super().get_sheet_info_data(**kwargs),
			('', ''),
			('SUMMARY', ''),
			('RTU with Downtime', self.result['overall']['total_rtu_down']),
			('RTU Availability', self.result['overall']['rtu_availability']),
			('Link Availability', self.result['overall']['link_availability'])
		]
		return extra_info


class _IFSAnalyzer:
	"""Base class for analyze data from SOE.

	Args:
		data : SOE data

	Accepted kwargs:
		**
	"""
	_executor = ThreadPoolExecutor(8)
	maintenance_mark = '**maintenance**'
	linkfailure_mark = '**link**'
	rtufailure_mark = '**rtu**'
	otherfailure_mark = '**other**'
	keep_duplicate = 'last'
	# List of downtime categorization (<category>, <value on hour>), ordered by the most significant category
	category = [
		('Critical', 72),
		('Major', 24),
		('Intermediate', 8),
		('Minor', 3)
	]

	def __init__(self, data: pd.DataFrame = None, **kwargs):
		self._analyzed = False

		if data is not None: self.soe_all = data

		self._soe_setup()
		super().__init__(**kwargs)

	def _get_category(self, downtime: datetime.timedelta):
		"""Annotate downtime duration category from defined rules.

		Args:
			downtime : downtime duration

		Result:
			Annotation text
		"""
		result = None

		for rule in self.category:
			if downtime>datetime.timedelta(hours=rule[1]):
				result = f'Downtime > {rule[1]} jam ({rule[0]})'
				break
		
		return result

	def _set_range(self, start: datetime.datetime, stop: datetime.datetime) -> tuple[datetime.datetime, datetime.datetime]:
		"""Set adjusted time for start and stop datetime of query data.

		Args:
			start : oldest date limit
			stop : newest date limit

		Result:
			Pair of datetime limit
		"""
		dt0 = start.replace(hour=0, minute=0, second=0, microsecond=0)
		dt1 = stop.replace(hour=23, minute=59, second=59, microsecond=999999)

		self._t0 = dt0
		self._t1 = dt1
		return dt0, dt1

	def _soe_setup(self):
		"""Apply sorting and filtering on "soe_all" to get cleaned data."""
		if isinstance(self.soe_all, pd.DataFrame):
			self._is_valid = True
			df = self.soe_all.copy()
			df = df.sort_values(['System time stamp', 'System milliseconds'], ascending=[True, True]).reset_index(drop=True)
			# Get min index and max index of df
			self._lowest_index, self._highest_index = df.index.min(), df.index.max()
			# Double check His. Messages only for IFS status changes
			self.soe_ifs = df[(df['A']=='') & (df['B1']=='IFS') & (df['B2']=='RTU_P1') & (df['Tag']=='')]
		else:
			self._is_valid = False
			raise ValueError('Dataframe tidak valid')

	def get_rtu_updown(self, df: pd.DataFrame, rtu: str) -> list[dict]:
		"""Analyze all downtime occured on a RTU.

		Args:
			df : dataframe source
			rtu : remote station name

		Result:
			List of dict-like downtime information
		"""
		i_sys_tstamp = df.columns.get_loc('System time stamp')
		i_sys_msec = df.columns.get_loc('System milliseconds')
		i_status = df.columns.get_loc('Status')
		i_user_comment = df.columns.get_loc('User comment')
		i_b3_text = df.columns.get_loc('B3 text')
		df_rtu = df[df['B3']==rtu]
		index0 = self.lowest_index
		t0 = self._t0
		rtu_updown = list()
		notes = list()

		for y in range(df_rtu.shape[0]):
			t1 = join_datetime(*df_rtu.iloc[y, [i_sys_tstamp, i_sys_msec]])
			status, comment, description = df_rtu.iloc[y, [i_status, i_user_comment, i_b3_text]]
			data = {
				'Down Time': 0,
				'Up Time': 0,
				'RTU': rtu,
				'Long Name': description,
				'Duration': 0,
				'Annotations': '',
				'Acknowledged Down Time': '',
				'Fix Duration': 0,
				'Marked Maintenance': '',
				'Marked Link Failure': '',
				'Marked RTU Failure': '',
				'Marked Other Failure': '',
				'Navigation': (0, 0)
			}

			# Copy User Comment if any
			if comment:
				if self.maintenance_mark in comment:
					data['Marked Maintenance'] = '*'
					notes.append('User menandai downtime akibat pemeliharaan**')
				elif self.linkfailure_mark in comment:
					data['Marked Link Failure'] = '*'
					notes.append('User menandai downtime akibat gangguan telekomunikasi**')
				elif self.rtufailure_mark in comment:
					data['Marked RTU Failure'] = '*'
					notes.append('User menandai downtime akibat gangguan RTU**')
				elif self.otherfailure_mark in comment:
					data['Marked Other Failure'] = '*'
					notes.append('User menandai downtime akibat gangguan lainnya**')
				else:
					# Eleminate unnecessary character
					txt = re.sub('^\W*|\s*$', '', comment)
					notes += txt.split('\n')

			if status=='Up':
				# Calculate downtime duration in second and append to analyzed_rows
				downtime = t1 - t0
				category = self._get_category(downtime)

				if category: notes.append(category)

				data.update({
					'Down Time': t0,
					'Up Time': t1,
					'Duration': downtime,
					'Fix Duration': downtime,
					'Annotations': '\n'.join(notes),
					'Navigation': (index0, df_rtu.iloc[y].name)
				})
				rtu_updown.append(data)
				# (t0, t1, mnemo, des, downtime, '\n'.join(anno), f'=HYPERLINK("#HIS_MESSAGES!A{index0+2}:N{df_rtu.iloc[y].name+2}"," CARI >> ")')
				# Reset anno
				notes.clear()
			elif status=='Down':
				if y==df_rtu.shape[0]-1:
					# RTU down until max time range
					downtime = self._t1 - t1
					category = self._get_category(downtime)

					if category: notes.append(category)

					data.update({
						'Down Time': t1,
						'Up Time': self._t1,
						'Duration': downtime,
						'Fix Duration': downtime,
						'Annotations': '\n'.join(notes),
						'Navigation': (df_rtu.iloc[y].name, self.highest_index)
					})
					rtu_updown.append(data)
					# Reset anno
					notes.clear()
				else:
					index0 = df_rtu.iloc[y].name
					t0 = t1

		return rtu_updown

	def analyze_rtu_concurrently(self, df: pd.DataFrame, rtus: list[str]) -> tuple[list[dict], list[str]]:
		"""Run analyze process in Threads.

		Args:
			df : dataframe
			rtus : list of remote station name

		Result:
			Pair of updown list and rtu name list
		"""
		with ThreadPoolExecutor(len(rtus)) as tpe:
			updown_list = list()
			futures = [tpe.submit(self.get_rtu_updown, df, rtu) for rtu in rtus]

			for future in futures:
				updown_list += future.result()

			return updown_list, rtus

	def fast_analyze(self, start: datetime.datetime = None, stop: datetime.datetime = None, **kwargs) -> pd.DataFrame:
		"""Split analyze into some processes, datetime limits may included.

		Args:
			start : oldest date limit
			stop : newest date limit

		Result:
			Downtime data as dataframe
		"""
		gc.collect()
		# Can be filtered with date
		if isinstance(start, datetime.datetime) and isinstance(stop, datetime.datetime):
			t0, t1 = self._set_range(start=start, stop=stop)
		else:
			t0, t1 = self._set_range(start=self.soe_ifs['Time stamp'].min(), stop=self.soe_ifs['Time stamp'].max())

		rtu_list = self.get_rtus()
		df = self.soe_ifs[(self.soe_ifs['Time stamp']>=t0) & (self.soe_ifs['Time stamp']<=t1)]
		n = 8
		# chunksize = len(rtu_list)//n + 1
		chunksize = 1	# The fastest process duration proven from some tests
		data_list = list()

		print(f'\nMenganalisa downtime {len(rtu_list)} RTU...')
		with ProcessPoolExecutor(n) as ppe:
			futures = list()

			for i in range(0, len(rtu_list), chunksize):
				rtus = rtu_list[i:(i+chunksize)]
				future = ppe.submit(self.analyze_rtu_concurrently, df, rtus)
				futures.append(future)

			for x, future in enumerate(as_completed(futures)):
				progress_bar((x+1)/len(futures))
				result_list, rtus = future.result()
				data_list.extend(result_list)

		# Create new DataFrame from list of dict data
		df_downtime = pd.DataFrame(data=data_list).sort_values(['Down Time', 'Up Time'], ascending=[True, True]).reset_index(drop=True)
		self.rtus = rtus
		self.post_process = df
		self._analyzed = True
		return df_downtime

	def analyze(self, start: datetime.datetime = None, stop: datetime.datetime = None, **kwargs) -> pd.DataFrame:
		"""[DEPRECATED]
		Analyzed every Up/Down event with presume that all RTUs are Up in the date_start.

		Args:
			start : oldest date limit
			stop : newest date limit

		Result:
			Downtime data as dataframe
		"""
		gc.collect()
		updown_list = list()

		# Can be filtered with date
		if isinstance(start, datetime.datetime) and isinstance(stop, datetime.datetime):
			t0, t1 = self._set_range(start=start, stop=stop)
		else:
			t0, t1 = self._set_range(start=self.soe_ifs['Time stamp'].min(), stop=self.soe_ifs['Time stamp'].max())

		rtus = self.get_rtus()
		df = self.soe_ifs[(self.soe_ifs['Time stamp']>=self._t0) & (self.soe_ifs['Time stamp']<=self._t1)]
		i_sys_tstamp = df.columns.get_loc('System time stamp')
		i_sys_msec = df.columns.get_loc('System milliseconds')
		i_status = df.columns.get_loc('Status')
		i_user_comment = df.columns.get_loc('User comment')
		i_b3_text = df.columns.get_loc('B3 text')

		print(f'\nMenganalisa downtime dari {len(rtus)} Remote Station...')
		for x, rtu in enumerate(rtus):
			progress_bar((x+1)/len(rtus))

			notes = list()
			index0 = self.lowest_index
			t0 = self._t0
			df_rtu = df[df['B3']==rtu]

			for y in range(df_rtu.shape[0]):
				t1 = join_datetime(*df_rtu.iloc[y, [i_sys_tstamp, i_sys_msec]])
				status, comment, description = df_rtu.iloc[y, [i_status, i_user_comment, i_b3_text]]
				data = {
					'Down Time': 0,
					'Up Time': 0,
					'RTU': rtu,
					'Long Name': description,
					'Duration': 0,
					'Annotations': '',
					'Acknowledged Down Time': '',
					'Fix Duration': 0,
					'Marked Maintenance': '',
					'Marked Link Failure': '',
					'Marked RTU Failure': '',
					'Marked Other Failure': '',
					'Navigation': (0, 0)
				}

				# Copy User Comment if any
				if comment:
					if self.maintenance_mark in comment:
						data['Marked Maintenance'] = '*'
						notes.append('User menandai downtime akibat pemeliharaan**')
					elif self.linkfailure_mark in comment:
						data['Marked Link Failure'] = '*'
						notes.append('User menandai downtime akibat gangguan telekomunikasi**')
					elif self.rtufailure_mark in comment:
						data['Marked RTU Failure'] = '*'
						notes.append('User menandai downtime akibat gangguan RTU**')
					elif self.otherfailure_mark in comment:
						data['Marked Other Failure'] = '*'
						notes.append('User menandai downtime akibat gangguan lainnya**')
					else:
						# Eleminate unnecessary character
						txt = re.sub('^\W*|\s*$', '', comment)
						notes += txt.split('\n')

				if status=='Up':
					# Calculate downtime duration in second and append to analyzed_rows
					downtime = t1 - t0
					category = self._get_category(downtime)

					if category: notes.append(category)

					data.update({
						'Down Time': t0,
						'Up Time': t1,
						'Duration': downtime,
						'Fix Duration': downtime,
						'Annotations': '\n'.join(notes),
						'Navigation': (index0, df_rtu.iloc[y].name)
					})
					updown_list.append(data)
					# (t0, t1, mnemo, des, downtime, '\n'.join(anno), f'=HYPERLINK("#HIS_MESSAGES!A{index0+2}:N{df_rtu.iloc[y].name+2}"," CARI >> ")')
					# Reset anno
					notes.clear()
				elif status=='Down':
					if y==df_rtu.shape[0]-1:
						# RTU down until max time range
						downtime = self._t1 - t1
						category = self._get_category(downtime)

						if category: notes.append(category)

						data.update({
							'Down Time': t1,
							'Up Time': self._t1,
							'Duration': downtime,
							'Fix Duration': downtime,
							'Annotations': '\n'.join(notes),
							'Navigation': (df_rtu.iloc[y].name, self.highest_index)
						})
						updown_list.append(data)
						# Reset anno
						notes.clear()
					else:
						index0 = df_rtu.iloc[y].name
						t0 = t1

		# Create new DataFrame from list of dict data
		df_downtime = pd.DataFrame(data=updown_list).sort_values(['Down Time', 'Up Time'], ascending=[True, True]).reset_index(drop=True)
		self.rtus = rtus
		self.post_process = df
		self._analyzed = True
		return df_downtime

	def get_rtus(self) -> np.ndarray:
		"""Get list of unique remote station name with respect of datetime limits.

		Result:
			List of remote station name
		"""
		df = self.soe_ifs
		# Get His. Messages with order tag only
		rtus = df.loc[(df['Time stamp']>=self._t0) & (df['Time stamp']<=self._t1), 'B3'].unique()
		return rtus

	@property
	def analyzed(self):
		return self._analyzed

	@property
	def highest_index(self):
		return self._highest_index

	@property
	def is_valid(self):
		return self._is_valid

	@property
	def lowest_index(self):
		return self._lowest_index


class _AVRSBaseCalculation:
	"""Base class for Remote Station Availability calculation.

	Args:
		data : analyzed data input

	Accepted kwargs:
		**
	"""
	name = 'Availability Remote Station'
	keep_duplicate = 'last'
	cpoint_file = 'cpoint.xlsx'

	def __init__(self, data: pd.DataFrame = None, **kwargs):
		self._calculated = False
		self.availability = None

		if data is not None: self.rtudown_all = data

		cpoint = getattr(self, 'cpoint_description', None)
		if isinstance(cpoint, pd.DataFrame):
			self.cpoint_ifs = cpoint[(cpoint['B1']=='IFS') & (cpoint['B2']=='RTU_P1')]
		else:
			cpoint = load_cpoint(self.cpoint_file)
			# Remove duplicates to prevent duplication in merge process
			self.cpoint_ifs = cpoint[(cpoint['B1']=='IFS') & (cpoint['B2']=='RTU_P1')].drop_duplicates(subset=['B1 text', 'B2 text', 'B3 text'], keep='first')

		# if hasattr(self, 'rtudown_all'): self.calculate(start=kwargs.get('start'), stop=kwargs.get('stop'))

	def _avrs_setup(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
		"""Apply conditioning & filtering before calculation process.

		Args:
			df : raw data

		Result:
			Cleaned data
		"""
		prepared = df.copy()
		duracol_type = prepared['Duration'].dtype
		duracell_type = type(prepared.loc[prepared.index[0], 'Duration'])
		# Must be determined, can cause error on groupby process
		if duracol_type=='object' or duracell_type==datetime.time:
			prepared['Duration'] = prepared['Duration'].map(lambda time: pd.Timedelta(time.hour*3600 + time.minute*60 + time.second + time.microsecond/10**6, unit='s'))
		elif duracol_type=='timedelta64[ns]' or duracell_type==pd.Timedelta:
			pass
		else:
			print(f'Warning: kolom "Duration" (column_type={duracol_type}, cell_type={duracell_type})')
		# Filter only rows with not unused-marked
		# prepared = prepared.loc[(prepared['Marked Maintenance']=='') & (prepared['Marked Link Failure']=='') & (prepared['Marked Other Failure']=='')]
		return prepared

	@calc_time
	def _calculate(self, start: datetime.datetime, stop: datetime.datetime, force: bool = False, fast: bool = True, **kwargs) -> dict[str, dict[str, Any]]:
		"""Analyze and/or calculate RTU downtime.

		Args:
			start : oldest date limit
			stop : newest date limit
			force : force calculation
			fast : use fast calculation method or not

		Result:
			Downtime data as dataframe
		"""
		if not hasattr(self, 'rtudown_all') or force:
			# Must be analyzed first and pass to rtudown_all
			fn1 = getattr(self, 'fast_analyze', None)
			fn2 = getattr(self, 'analyze', None)
			if callable(fn1) and fast:
				self.rtudown_all = fn1(start=start, stop=stop, **kwargs)
			elif callable(fn2):
				self.rtudown_all = fn2(start=start, stop=stop, **kwargs)
			else:
				raise AttributeError('Atttribute error.', name='fast_analyze() / analyze()', obj=self.__class__.__name__)

		if isinstance(self.rtudown_all, pd.DataFrame):
			# Can be filtered with date
			if isinstance(start, datetime.datetime) and isinstance(stop, datetime.datetime):
				t0, t1 = self._set_range(start=start, stop=stop)
			else:
				t0, t1 = self._set_range(start=self.rtudown_all['Down Time'].min(), stop=self.rtudown_all['Down Time'].max())
		else:
			raise AttributeError('Data input tidak valid.', name='rtudown_all', obj=self)

		print(f'\nMenghitung downtime RTU tanggal {t0.strftime("%d-%m-%Y")} s/d {t1.strftime("%d-%m-%Y")}...')
		result = self.get_result(**kwargs)
		return result

	def _set_range(self, start:datetime.datetime, stop:datetime.datetime) -> tuple[datetime.datetime, datetime.datetime]:
		"""Set adjusted time for start and stop datetime of query data.

		Args:
			start : oldest date limit
			stop : newest date limit

		Result:
			Pair of datetime limit
		"""
		dt0 = start.replace(hour=0, minute=0, second=0, microsecond=0)
		dt1 = stop.replace(hour=23, minute=59, second=59, microsecond=999999)

		self._t0 = dt0
		self._t1 = dt1
		return dt0, dt1

	def get_result(self, **kwargs) -> dict[str, dict[str, Any]]:
		"""Get aggregate data of availability calculation.

		Result:
			Calculation result in dict
		"""
		df = self.rtudown_all.loc[(self.rtudown_all['Down Time']>=self.t0) & (self.rtudown_all['Down Time']<=self.t1)]
		df_pre = self._avrs_setup(df)
		df_av = self.group(df_pre)

		self.pre_process = df
		self.availability = df_av
		self._calculated = True

		# Statistics information
		down_all = df.shape[0]
		down_valid = df_pre.shape[0]
		down_maint = df[df['Marked Maintenance']=='*'].shape[0]
		down_telco = df[df['Marked Link Failure']=='*'].shape[0]
		down_rtugw = df[df['Marked RTU Failure']=='*'].shape[0]
		down_other = df[df['Marked Other Failure']=='*'].shape[0]
		down_marked = down_maint + down_telco + down_other
		down_avg = df_pre['Duration'].mean()
		down_avg_dd, down_avg_hh, down_avg_mm, down_avg_ss = timedelta_split(down_avg)
		down_max = df_pre['Duration'].max()
		down_max_dd, down_max_hh, down_max_mm, down_max_ss = timedelta_split(down_max)
		down_min = df_pre['Duration'].min()
		down_min_dd, down_min_hh, down_min_mm, down_min_ss = timedelta_split(down_min)

		avrtu = round(1 - (df_av['RTU Downtime'] / df_av['Time Range']) * 100, 2)
		avlink = round(1 - (df_av['Link Downtime'] / df_av['Time Range']) * 100, 2)
		rtu_count_max = df_av.loc[df_av['Downtime Occurences'].idxmax()]['RTU']
		rtu_count_min = df_av.loc[df_av[df_av['Downtime Occurences']>0]['Downtime Occurences'].idxmin()]['RTU']
		rtu_down_max = df_av.loc[df_av[df_av['Downtime Occurences']>0]['Total Downtime'].idxmax()]['RTU']
		rtu_down_min = df_av.loc[df_av[df_av['Downtime Occurences']>0]['Total Downtime'].idxmin()]['RTU']
		rtu_avg_max = df_av.loc[df_av[df_av['Downtime Occurences']>0]['Average Downtime'].idxmax()]['RTU']
		rtu_avg_min = df_av.loc[df_av[df_av['Downtime Occurences']>0]['Average Downtime'].idxmin()]['RTU']
		return {
			'overall': {
				'total_rtu': df_av.shape[0],
				'total_rtu_down': df_av[df_av['Downtime Occurences']>0].shape[0],
				'downtime_count': f'{down_valid}',
				'rtu_availability': f'{avrtu}%',
				'link_availability': f'{avlink}%'
			},
			'statistic': {
				'downtime_avg': f'{down_avg_dd} Hari {down_avg_hh:02}:{down_avg_mm:02}:{down_avg_ss:02}',
				'downtime_max': f'{down_max_dd} Hari {down_max_hh:02}:{down_max_mm:02}:{down_max_ss:02}',
				'downtime_min': f'{down_min_dd} Hari {down_min_hh:02}:{down_min_mm:02}:{down_min_ss:02}',
				'marked': {
					'maintenance': down_maint,
					'link': down_telco,
					'rtu': down_rtugw,
					'other': down_other,
					'total': down_marked
				},
				'remote_station': {
					'rtu_count_max': rtu_count_max,
					'rtu_count_min': rtu_count_min,
					'rtu_down_max': rtu_down_max,
					'rtu_down_min': rtu_down_min,
					'rtu_avg_max': rtu_avg_max,
					'rtu_avg_min': rtu_avg_min
				}
			}
		}

	def calculate(self, start: datetime.datetime = None, stop: datetime.datetime = None, force: bool = False, fast: bool = True, **kwargs):
		"""Calculate RTU downtime.

		Args:
			start : oldest date limit
			stop : newest date limit
			force : force calculation
			fast : use fast calculation method or not
		"""
		self.result, t = self._calculate(start=start, stop=stop, force=force, fast=fast, **kwargs)
		self._process_date = datetime.datetime.now()
		self._process_duration = round(t, 3)
		print(f'Perhitungan selesai. (durasi={t:.2f}s, error=0)')

	def generate_reference(self, soe: pd.DataFrame, down: pd.DataFrame) -> np.ndarray:
		"""Create excel hyperlink of each RTU up/down in sheet "DOWNTIME" to cell range in sheet "HIS_MESSAGES".

		Args:
			soe : dataframe of SOE
			down : dataframe of DOWNTIME

		Result:
			List of excel hyperlink
		"""
		navs = []
		errors = 0

		try:
			for idx_start, idx_stop in down['Navigation']:
				try:
					hyperlink = f'=HYPERLINK("#HIS_MESSAGES!A{soe.index.get_loc(idx_start)+2}:N{soe.index.get_loc(idx_stop)+2}","CARI >>")'
				except Exception:
					errors += 1
					hyperlink = f'=HYPERLINK("#ERROR!{idx_start}:{idx_stop}","ERROR!!")'

				navs.append(hyperlink)
		except Exception:
			errors += 1

		if errors>0: print(f'Terjadi {errors} error saat generate hyperlink.')
		return np.array(navs)

	def group(self, df: pd.DataFrame) -> pd.DataFrame:
		"""Perform aggregation and downtime grouping to get statistical result.

		Args:
			df : calculated downtime data

		Result:
			Aggregated availability data
		"""
		columns = ['RTU', 'Long Name']
		groupby_columns = columns + ['Duration']
		output_columns = [
			'RTU',
			'Long Name',
			'Time Range',
			'Downtime Occurences',
			'Total Downtime',
			'Average Downtime',
			'Longest Downtime',
			'Longest Downtime Date',
			'Uptime',
			'Unclassified Downtime',
			'RTU Downtime',
			'Link Downtime',
			'Other Downtime',
			'Quality',
			'RTU Availability',
			'Link Availability'
		]

		df_pre = df.copy()
		rtu_table = self.cpoint_ifs[['B3', 'B3 text']].rename(columns={'B3': 'RTU', 'B3 text': 'Long Name'})

		down_count = df_pre[groupby_columns].groupby(columns, as_index=False).count().rename(columns={'Duration': 'Downtime Occurences'})
		down_agg = df_pre[groupby_columns].groupby(columns).agg(['sum', 'mean', 'max']).reset_index()
		down_agg.columns = ['RTU', 'Long Name', 'Total Downtime', 'Average Downtime', 'Longest Downtime']
		filter_max_downtime = df_pre.groupby(columns, as_index=False)['Duration'].transform('max')==df_pre['Duration']
		down_max_t = df_pre.loc[filter_max_downtime, columns + ['Down Time']].rename(columns={'Down Time': 'Longest Downtime Date'})
		filter_unc_down =  (df['Marked Maintenance']=='*') & (df['Marked RTU Failure']=='*') & (df['Marked Link Failure']=='*') & (df['Marked Other Failure']=='*')
		filter_other_down = (df['Marked Maintenance']=='*') | (df['Marked Other Failure']=='*')
		filter_rtu_down = (df['Marked RTU Failure']=='*')
		filter_link_down = (df['Marked Link Failure']=='*')
		down_uncls = df.loc[filter_unc_down, groupby_columns].groupby(columns, as_index=False).sum().rename(columns={'Duration': 'Unclassified Downtime'})
		down_rtu = df.loc[filter_rtu_down, groupby_columns].groupby(columns, as_index=False).sum().rename(columns={'Duration': 'RTU Downtime'})
		down_link = df.loc[filter_link_down, groupby_columns].groupby(columns, as_index=False).sum().rename(columns={'Duration': 'Link Downtime'})
		down_other = df.loc[filter_other_down, groupby_columns].groupby(columns, as_index=False).sum().rename(columns={'Duration': 'Other Downtime'})

		# Merge table and fill NaN Downtime Occurences to 0
		df_groupby = rtu_table.merge(right=down_count, how='outer', on=columns).fillna(0)
		# Merge existing table with aggregated table and fill NaT with timedelta(0 second)
		for dfgroup in [down_agg, down_uncls, down_rtu, down_link, down_other]:
			df_groupby = df_groupby.merge(right=dfgroup, how='left', on=columns).fillna(datetime.timedelta(seconds=0))

		df_groupby = df_groupby.merge(right=down_max_t, how='left', on=columns).fillna(self.t1)
		df_groupby['Time Range'] = self.t1 - self.t0 + datetime.timedelta(microseconds=1)
		df_groupby['Uptime'] = df_groupby['Time Range'] - df_groupby['Total Downtime']
		df_groupby['Quality'] = 1
		df_groupby['RTU Availability'] = round(1 - (df_groupby['RTU Downtime'] / df_groupby['Time Range']), 4)
		df_groupby['Link Availability'] = round(1 - (df_groupby['Link Downtime'] / df_groupby['Time Range']), 4)
		return df_groupby[output_columns]

	def prepare_export(self, generate_formula: bool = False, **kwargs) -> dict[str, Union[pd.DataFrame, tuple[pd.DataFrame]]]:
		"""Applying excel formulas to output file.

		Args:
			generate_formula : either formula will be generated or not

		Accepted kwargs:
			soe : dataframe of SOE

		Result:
			Dict of sheet name & data
		"""
		if not self.calculated: raise SyntaxError('Jalankan calculate() terlebih dahulu!')

		df_dt = self.pre_process.copy()
		df_av = self.availability.copy()

		if generate_formula:
			dt_columns = df_dt.columns.to_list()
			av_columns = df_av.columns.to_list()
			dlen = df_dt.shape[0]
			alen = df_av.shape[0]

			def rule_lookup(xcol, key=None):
				if key:
					return f'DOWNTIME!${xd[xcol]}$2:${xd[xcol]}${dlen+1}, {key}'
				else:
					return f'DOWNTIME!${xd[xcol]}$2:${xd[xcol]}${dlen+1}'

			def ruleset(*rules):
				return ', '.join(rules)

			def countifs(*rules):
				return f'COUNTIFS({ruleset(*rules)})'

			def averageifs(range, *rules):
				return f'IFERROR(AVERAGEIFS({range}, {ruleset(*rules)}), 0)'

			def sumifs(range, *rules):
				return f'SUMIFS({range}, {ruleset(*rules)})'

			# Create dict of excel column label
			xd = {col: xl_col_to_name(dt_columns.index(col)) for col in dt_columns}
			xa = {col: xl_col_to_name(av_columns.index(col)) for col in av_columns}

			dt_update = {
				'Duration': [],
				'Fix Duration': []
			}
			av_update = {
				'Downtime Occurences': [],
				'Total Downtime': [],
				'Average Downtime': [],
				'Uptime': [],
				'Unclassified Downtime': [],
				'RTU Downtime': [],
				'Link Downtime': [],
				'Other Downtime': [],
				'RTU Availability': [],
				'Link Availability': []
			}
			
			# Define excel formula rule
			rule_maint = rule_lookup('Marked Maintenance', '""')
			rule_dtrtu = rule_lookup('Marked RTU Failure', '""')
			rule_telco = rule_lookup('Marked Link Failure', '""')
			rule_other = rule_lookup('Marked Other Failure', '""')

			# Apply excel formula as string
			# Sheet DOWNTIME
			if 'Navigation' in dt_columns:
				# Apply navigation hyperlink on sheet RC_ONLY
				df_dt['Navigation'] = self.generate_reference(soe=kwargs.get('soe'), down=df_dt)

			for rowd in range(dlen):
				h = rowd + 2
				dt_update['Duration'].append(f'=${xd["Up Time"]}{h}-${xd["Down Time"]}{h}')
				dt_update['Fix Duration'].append(f'=${xd["Duration"]}{h} - IF(ISNUMBER(${xd["Acknowledged Down Time"]}{h}), ${xd["Acknowledged Down Time"]}{h} - ${xd["Down Time"]}{h}, 0)')

			# Sheet AVAILABILITY
			for rowa in range(alen):
				i = rowa + 2
				rule_rtu = rule_lookup('RTU', f'${xa["RTU"]}{i}')
				rules = [rule_rtu, rule_maint, rule_dtrtu, rule_telco, rule_other]
				sum_uncls = sumifs(rule_lookup('Fix Duration'), *rules)
				sum_maint = sumifs(rule_lookup('Fix Duration'), rule_rtu, rule_lookup('Marked Maintenance', '"*"'))
				sum_dtrtu = sumifs(rule_lookup('Fix Duration'), rule_rtu, rule_lookup('Marked RTU Failure', '"*"'))
				sum_telco = sumifs(rule_lookup('Fix Duration'), rule_rtu, rule_lookup('Marked Link Failure', '"*"'))
				sum_other = sumifs(rule_lookup('Fix Duration'), rule_rtu, rule_lookup('Marked Other Failure', '"*"'))
				av_update['Downtime Occurences'].append('=' + countifs(rule_rtu))
				av_update['Total Downtime'].append('=' + sumifs(rule_lookup('Fix Duration'), rule_rtu))
				av_update['Average Downtime'].append('=' + averageifs(rule_lookup('Fix Duration'), rule_rtu))
				av_update['Uptime'].append(f'=${xa["Time Range"]}{i}-${xa["Total Downtime"]}{i}')
				av_update['Unclassified Downtime'].append(f'={sum_uncls}')
				av_update['RTU Downtime'].append(f'={sum_dtrtu}')
				av_update['Link Downtime'].append(f'={sum_telco}')
				av_update['Other Downtime'].append(f'={sum_maint}+{sum_other}')
				av_update['RTU Availability'].append(f'=ROUND((1-(${xa["RTU Downtime"]}{i}/${xa["Time Range"]}{i}))*${xa["Quality"]}{i}, 4)')
				av_update['Link Availability'].append(f'=ROUND((1-((${xa["Link Downtime"]}{i}+${xa["Unclassified Downtime"]}{i})/${xa["Time Range"]}{i}))*${xa["Quality"]}{i}, 4)')

			av_result = {
				'Downtime Occurences': [f'=SUM(${xa["Downtime Occurences"]}$2:${xa["Downtime Occurences"]}${alen+1})'],
				'Quality': [f'=SUM(${xa["Quality"]}$2:${xa["Quality"]}${alen+1})'],
				'RTU Availability': [f'=SUM(${xa["RTU Availability"]}$2:${xa["RTU Availability"]}${alen+1})/${xa["Quality"]}{alen+2}'],
				'Link Availability': [f'=SUM(${xa["Link Availability"]}$2:${xa["Link Availability"]}${alen+1})/${xa["Quality"]}{alen+2}']
			}
			df_av_result = pd.DataFrame(data=av_result)

			# Update new DataFrame
			for dtcol in dt_update: df_dt[dtcol] = np.array(dt_update[dtcol])
			for avcol in av_update: df_av[avcol] = np.array(av_update[avcol])

			# Update summary information
			count_maint = 'COUNTIF(' + rule_lookup('Marked Maintenance', '"*"') + ')'
			count_dtrtu = 'COUNTIF(' + rule_lookup('Marked Link Failure', '"*"') + ')'
			count_telco = 'COUNTIF(' + rule_lookup('Marked Link Failure', '"*"') + ')'
			count_other = 'COUNTIF(' + rule_lookup('Marked Other Failure', '"*"') + ')'
			self.result['overall']['total_rtu_down'] = f'=COUNTIF(AVAILABILITY!${xa["Downtime Occurences"]}$2:${xa["Downtime Occurences"]}${alen+1}, ">0")'
			self.result['overall']['rtu_availability'] = f'=ROUND(AVAILABILITY!${xa["RTU Availability"]}${alen+2}*100, 2) & "%"'
			self.result['overall']['link_availability'] = f'=ROUND(AVAILABILITY!${xa["Link Availability"]}${alen+2}*100, 2) & "%"'
			self.result['statistic']['marked']['maintenance'] = '=' + count_maint
			self.result['statistic']['marked']['rtu'] = '=' + count_dtrtu
			self.result['statistic']['marked']['link'] = '=' + count_telco
			self.result['statistic']['marked']['other'] = '=' + count_other
			self.result['statistic']['marked']['total'] = f'={count_maint}+{count_dtrtu}+{count_telco}+{count_other}'

		return {
			'DOWNTIME': df_dt,
			'AVAILABILITY': (df_av, df_av_result)
		}

	@property
	def calculated(self):
		return self._calculated

	@property
	def process_date(self):
		return self._process_date

	@property
	def process_duration(self):
		return self._process_duration

	@property
	def t0(self):
		return self._t0

	@property
	def t1(self):
		return self._t1


class AVRS(_Export, _AVRSBaseCalculation):

	def __init__(self, data: pd.DataFrame = None, **kwargs):
		super().__init__(data, **kwargs)


class SOEtoAVRS(_Export, _IFSAnalyzer, _AVRSBaseCalculation):

	def __init__(self, data: pd.DataFrame = None, **kwargs):
		super().__init__(data, **kwargs)

	def prepare_export(self, generate_formula: bool = False, **kwargs):
		"""Applying excel formulas to output file.

		Args:
			generate_formula : either formula will be generated or not

		Result:
			Dict of sheet name & data
		"""
		if not self.analyzed: raise SyntaxError('Jalankan calculate() terlebih dahulu!')

		# Define soe as reference on generating hyperlink in prepare_export()
		kwargs.update(soe=self.post_process, generate_formula=generate_formula)

		return {
			'HIS_MESSAGES': self.post_process,
			**super().prepare_export(**kwargs)
		}


class AVRSCollective(AVRSFileReader, AVRS):

	def __init__(self, filepaths: Union[str, list], **kwargs):
		super().__init__(filepaths, **kwargs)


class AVRSFromOFDB(SpectrumOfdbClient, SOEtoAVRS):

	def __init__(self, date_start: datetime.datetime, date_stop: datetime.datetime = None, **kwargs):
		super().__init__(date_start, date_stop, **kwargs)


class AVRSFromFile(SpectrumFileReader, SOEtoAVRS):

	def __init__(self, filepaths: Union[str, list], **kwargs):
		super().__init__(filepaths, **kwargs)


def test_analyze_file(**params):
	gc.collect()
	av = AVRSFromFile('sample/sample_rtu*.xlsx')
	print()
	print(' TEST ANALYZE AVRS CONCURRENTLY '.center(CONSOLE_WIDTH, '#'))
	# t0 = time.time()
	av.calculate(force=True, fast=True)
	# print(f'Durasi = {time.time()-t0:.2f}s')
	print(' TEST ANALYZE AVRS '.center(CONSOLE_WIDTH, '#'))
	# t1 = time.time()
	av.calculate(force=True, fast=False)
	# print(f'Durasi = {time.time()-t1:.2f}s')
	if 'y' in input('Export hasil test? [y/n]  '):
		av.to_excel(filename='test_analyze_rtu_spectrum')
	return av

def test_collective_file(**params):
	gc.collect()
	av = AVRSCollective('sample/sample_rtu*.xlsx')
	print()
	print(' TEST COLLECTIVE AVRS '.center(CONSOLE_WIDTH, '#'))
	av.calculate()
	if 'y' in input('Export hasil test? [y/n]  '):
		av.to_excel(filename='test_collective_rtu')
	return av


if __name__=='__main__':
	test_list = [
		('Test analisa file SOE Spectrum', test_analyze_file),
		('Test menggabungkan file', test_collective_file)
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