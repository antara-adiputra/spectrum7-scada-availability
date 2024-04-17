import re, time
from datetime import datetime, timedelta
from typing import Union

import numpy as np
import pandas as pd
from xlsxwriter.utility import xl_col_to_name
from filereader import AvFileReader, SpectrumFileReader
from global_parameters import RTU_BOOK_PARAM
from lib import BaseExportMixin, join_datetime, immutable_dict, load_cpoint, progress_bar, timedelta_split
from ofdb import SpectrumOfdbClient


class _Export(BaseExportMixin):
	_sheet_parameter = immutable_dict(RTU_BOOK_PARAM)
	output_prefix = 'AVRS'
	
	def get_sheet_info_data(self, **kwargs):
		"""
		"""

		info_data = super().get_sheet_info_data(**kwargs)
		extra_info = [
			*info_data,
			('', ''),
			('SUMMARY', ''),
			('RTU with Downtime', self.result['overall']['total_rtu_down']),
			('Availability', self.result['overall']['availability'])
		]

		return extra_info


class _IFSAnalyzer:
	maintenance_mark = '**maintenance**'
	commfailure_mark = '**communication**'
	otherfailure_mark = '**other**'
	keep_duplicate = 'last'
	# List of downtime categorization (<category>, <value on hour>), ordered by the most significant category
	category = [
		('Critical', 72),
		('Major', 24),
		('Intermediate', 8),
		('Minor', 3)
	]

	def __init__(self, data:pd.DataFrame=None, **kwargs):
		self._analyzed = False

		if data is not None: self.soe_all = data

		self._soe_setup()
		super().__init__(**kwargs)

	def _get_category(self, downtime:timedelta):
		"""
		"""

		result = None

		for rule in self.category:
			if downtime>timedelta(hours=rule[1]):
				result = f'Downtime > {rule[1]} jam ({rule[0]})'
				break
		
		return result

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
		"""

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

	def analyze(self, start:datetime=None, stop:datetime=None):
		"""
		Analyzed every Up/Down event with presume that all RTUs are Up in the date_start.
		"""

		updown_list = []
		rtus = self.get_rtus(start=start, stop=stop)
		df = self.soe_ifs[(self.soe_ifs['Time stamp']>=self._t0) & (self.soe_ifs['Time stamp']<=self._t1)]
		i_sys_tstamp = df.columns.get_loc('System time stamp')
		i_sys_msec = df.columns.get_loc('System milliseconds')
		i_status = df.columns.get_loc('Status')
		i_user_comment = df.columns.get_loc('User comment')
		i_b3_text = df.columns.get_loc('B3 text')

		print(f'\nMenganalisa downtime dari {len(rtus)} Remote Station...')
		for x, rtu in enumerate(rtus):
			progress_bar((x+1)/len(rtus))

			notes = []
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
					'Marked Maintenance': '',
					'Marked Comm. Failure': '',
					'Marked Other Failure': '',
					'Annotations': '',
					'Navigation': (0, 0)
				}

				# Copy User Comment if any
				if comment:
					if self.maintenance_mark in comment:
						data['Marked Maintenance'] = '*'
						notes.append('User menandai downtime akibat pemeliharaan**')
					elif self.commfailure_mark in comment:
						data['Marked Comm. Failure'] = '*'
						notes.append('User menandai downtime akibat gangguan telekomunikasi**')
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

	def get_rtus(self, start:datetime=None, stop:datetime=None):
		"""
		"""

		df = self.soe_ifs
		# Can be filtered with date
		if isinstance(start, datetime) and isinstance(stop, datetime):
			t0, t1 = self._set_range(start=start, stop=stop)
		else:
			t0, t1 = self._set_range(start=df['Time stamp'].min(), stop=df['Time stamp'].max())

		# Get His. Messages with order tag only
		rtus = df.loc[(df['Time stamp']>=t0) & (df['Time stamp']<=t1), 'B3'].unique()

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


class _AvBaseCalculation:
	name = 'Availability Remote Station'
	keep_duplicate = 'last'
	cpoint_file = 'cpoint.xlsx'

	def __init__(self, data:pd.DataFrame=None, **kwargs):
		self._calculated = False
		self.availability = None

		if data is not None: self.rtudown_all = data

		cpoint = getattr(self, 'cpoint_description')
		if isinstance(cpoint, pd.DataFrame):
			self.cpoint_ifs = cpoint[(cpoint['B1']=='IFS') & (cpoint['B2']=='RTU_P1')]
		else:
			cpoint = load_cpoint(self.cpoint_file)
			# Remove duplicates to prevent duplication in merge process
			self.cpoint_ifs = cpoint[(cpoint['B1']=='IFS') & (cpoint['B2']=='RTU_P1')].drop_duplicates(subset=['B1 text', 'B2 text', 'B3 text'], keep='first')

		if hasattr(self, 'rtudown_all'): self.calculate(start=kwargs.get('start'), stop=kwargs.get('stop'))

	def _avrs_setup(self, df:pd.DataFrame, **kwargs):
		"""
		"""

		prepared = df.copy()

		# Filter only rows with not unused-marked
		prepared = prepared.loc[(prepared['Marked Maintenance']=='') & (prepared['Marked Comm. Failure']=='') & (prepared['Marked Other Failure']=='')]

		return prepared

	def _calculate(self, start:datetime, stop:datetime):
		"""
		Get aggregate data of availability.
		"""

		if isinstance(self.rtudown_all, pd.DataFrame):
			# Can be filtered with date
			if isinstance(start, datetime) and isinstance(stop, datetime):
				t0, t1 = self._set_range(start=start, stop=stop)
			else:
				t0, t1 = self._set_range(start=self.rtudown_all['Down Time'].min(), stop=self.rtudown_all['Down Time'].max())
		else:
			raise AttributeError('Invalid data input.', name='rtudown_all', obj=self)

		df = self.rtudown_all.loc[(self.rtudown_all['Down Time']>=t0) & (self.rtudown_all['Down Time']<=t1)]
		df_pre = self._avrs_setup(df)
		df_av = self.group(df_pre)

		self.pre_process = df
		self.availability = df_av
		self._calculated = True

		# Statistics information
		down_all = df.shape[0]
		down_valid = df_pre.shape[0]
		down_maint = df[df['Marked Maintenance']=='*'].shape[0]
		down_telco = df[df['Marked Comm. Failure']=='*'].shape[0]
		down_other = df[df['Marked Other Failure']=='*'].shape[0]
		down_marked = down_maint + down_telco + down_other
		down_avg = df_pre['Duration'].mean()
		down_avg_dd, down_avg_hh, down_avg_mm, down_avg_ss = timedelta_split(down_avg)
		down_max = df_pre['Duration'].max()
		down_max_dd, down_max_hh, down_max_mm, down_max_ss = timedelta_split(down_max)
		down_min = df_pre['Duration'].min()
		down_min_dd, down_min_hh, down_min_mm, down_min_ss = timedelta_split(down_min)

		av = round((df_av['Calculated Availability'].sum() / df_av['Calculated Availability'].count()) * 100, 2)
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
				'availability': f'{av}%'
			},
			'statistic': {
				'downtime_avg': f'{down_avg_dd} Hari {down_avg_hh:02}:{down_avg_mm:02}:{down_avg_ss:02}',
				'downtime_max': f'{down_max_dd} Hari {down_max_hh:02}:{down_max_mm:02}:{down_max_ss:02}',
				'downtime_min': f'{down_min_dd} Hari {down_min_hh:02}:{down_min_mm:02}:{down_min_ss:02}',
				'marked': {
					'maintenance': down_maint,
					'communication': down_telco,
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

	def generate_reference(self, soe:pd.DataFrame, down:pd.DataFrame):
		"""
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

	def group(self, df:pd.DataFrame):
		"""
		Return DataFrameGroupBy Class of aggregation values which used in all grouped Dataframe with groupby_columns as columns parameter.
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
			'Non-RTU Downtime',
			'Calculated Availability',
			'Quality',
			'Availability'
		]

		df_pre = self._avrs_setup(df)
		rtu_table = self.cpoint_ifs[['B3', 'B3 text']].rename(columns={'B3': 'RTU', 'B3 text': 'Long Name'})

		down_count = df_pre[groupby_columns].groupby(columns, as_index=False).count().rename(columns={'Duration': 'Downtime Occurences'})
		down_agg = df_pre[groupby_columns].groupby(columns).agg(['sum', 'mean', 'max']).reset_index()
		down_agg.columns = ['RTU', 'Long Name', 'Total Downtime', 'Average Downtime', 'Longest Downtime']
		filter_max_downtime = df_pre.groupby(columns, as_index=False)['Duration'].transform('max')==df_pre['Duration']
		down_max_t = df_pre.loc[filter_max_downtime, columns + ['Down Time']].rename(columns={'Down Time': 'Longest Downtime Date'})
		filter_nonrtu_down = (df['Marked Maintenance']=='*') | (df['Marked Comm. Failure']=='*') | (df['Marked Other Failure']=='*')
		down_nonrtu = df.loc[filter_nonrtu_down, groupby_columns].groupby(columns, as_index=False).sum().rename(columns={'Duration': 'Non-RTU Downtime'})

		# Merge table and fill NaN Downtime Occurences to 0
		df_groupby = rtu_table.merge(right=down_count, how='outer', on=columns).fillna(0)
		# Merge existing table with aggregated table and fill NaT with timedelta(0 second)
		df_groupby = df_groupby.merge(right=down_agg, how='left', on=columns).fillna(timedelta(seconds=0))
		df_groupby = df_groupby.merge(right=down_nonrtu, how='left', on=columns).fillna(timedelta(seconds=0))
		df_groupby = df_groupby.merge(right=down_max_t, how='left', on=columns).fillna(self.t1)

		df_groupby['Time Range'] = self.t1 - self.t0 + timedelta(microseconds=1)
		df_groupby['Uptime'] = df_groupby['Time Range'] - df_groupby['Total Downtime']
		df_groupby['Calculated Availability'] = round((df_groupby['Uptime'] + df_groupby['Non-RTU Downtime']) / df_groupby['Time Range'], 4)
		df_groupby['Quality'] = 1
		df_groupby['Availability'] = df_groupby['Calculated Availability']

		return df_groupby[output_columns]

	def prepare_export(self, generate_formula:bool=False, **kwargs):
		"""
		Applying excel formulas to output file
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

			av_update = {
				'Downtime Occurences': [],
				'Total Downtime': [],
				'Average Downtime': [],
				'Non-RTU Downtime': [],
				'Calculated Availability': [],
				'Availability': []
			}
			
			# Define excel formula rule
			rule_maint = rule_lookup('Marked Maintenance', '""')
			rule_telco = rule_lookup('Marked Comm. Failure', '""')
			rule_other = rule_lookup('Marked Other Failure', '""')

			# Apply excel formula as string
			# Sheet DOWNTIME
			if 'Navigation' in dt_columns:
				# Apply navigation hyperlink on sheet RC_ONLY
				df_dt['Navigation'] = self.generate_reference(soe=kwargs.get('soe'), down=df_dt)
			# Sheet AVAILABILITY
			for rowa in range(alen):
				i = rowa + 2
				rule_rtu = rule_lookup('RTU', f'${xa["RTU"]}{i}')
				rules = [rule_rtu, rule_maint, rule_telco, rule_other]
				sum_maint = sumifs(rule_lookup('Duration'), rule_rtu, rule_lookup('Marked Maintenance', '"*"'))
				sum_telco = sumifs(rule_lookup('Duration'), rule_rtu, rule_lookup('Marked Comm. Failure', '"*"'))
				sum_other = sumifs(rule_lookup('Duration'), rule_rtu, rule_lookup('Marked Other Failure', '"*"'))
				av_update['Downtime Occurences'].append('=' + countifs(*rules))
				av_update['Total Downtime'].append('=' + sumifs(rule_lookup('Duration'), *rules))
				av_update['Average Downtime'].append('=' + averageifs(rule_lookup('Duration'), *rules))
				av_update['Non-RTU Downtime'].append(f'={sum_maint}+{sum_telco}+{sum_other}')
				av_update['Calculated Availability'].append(f'=ROUND((${xa["Uptime"]}{i}+${xa["Non-RTU Downtime"]}{i})/${xa["Time Range"]}{i}, 4)')
				av_update['Availability'].append(f'=ROUND(${xa["Calculated Availability"]}{i}*${xa["Quality"]}{i}, 4)')

			av_result = {
				'Downtime Occurences': [f'=SUM(${xa["Downtime Occurences"]}$2:${xa["Downtime Occurences"]}${alen+1})'],
				'Calculated Availability': [f'=IFERROR(AVERAGE(${xa["Calculated Availability"]}$2:${xa["Calculated Availability"]}${alen+1}), 0)'],
				'Quality': [f'=SUM(${xa["Quality"]}$2:${xa["Quality"]}${alen+1})'],
				'Availability': [f'=SUM(${xa["Availability"]}$2:${xa["Availability"]}${alen+1})/${xa["Quality"]}{alen+2}']
			}
			df_av_result = pd.DataFrame(data=av_result)

			# Update new DataFrame
			df_av.update(pd.DataFrame(av_update))

			# Update summary information
			count_maint = 'COUNTIF(' + rule_lookup('Marked Maintenance', '"*"') + ')'
			count_telco = 'COUNTIF(' + rule_lookup('Marked Comm. Failure', '"*"') + ')'
			count_other = 'COUNTIF(' + rule_lookup('Marked Other Failure', '"*"') + ')'
			self.result['overall']['availability'] = f'=ROUND(AVAILABILITY!${xa["Availability"]}${alen+2}*100, 2) & "%"'
			self.result['statistic']['marked']['maintenance'] = '=' + count_maint
			self.result['statistic']['marked']['communication'] = '=' + count_telco
			self.result['statistic']['marked']['other'] = '=' + count_other
			self.result['statistic']['marked']['total'] = f'={count_maint}+{count_telco}+{count_other}'

		return {
			'DOWNTIME': df_dt,
			'AVAILABILITY': (df_av, df_av_result)
		}


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

class AVRS(_Export, _AvBaseCalculation):

	def __init__(self, data:pd.DataFrame=None, **kwargs):
		super().__init__(data, **kwargs)


class SOEtoAVRS(_Export, _IFSAnalyzer, _AvBaseCalculation):

	def __init__(self, data:pd.DataFrame=None, **kwargs):
		super().__init__(data, **kwargs)

	def calculate(self, start:datetime=None, stop:datetime=None, force:bool=False):
		"""
		Override calculate function.
		"""

		process_date = datetime.now()
		process_begin = time.time()

		if not hasattr(self, 'rtudown_all') or force:
			# Must be analyzed first and pass to rtudown_all
			self.rtudown_all = self.analyze(start=start, stop=stop)

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

		# Define soe as reference on generating hyperlink in prepare_export()
		kwargs.update(soe=self.post_process, generate_formula=generate_formula)

		return {
			'HIS_MESSAGES': self.post_process,
			**super().prepare_export(**kwargs)
		}


class AVRSCollective(AvFileReader, AVRS):

	def __init__(self, filepaths:Union[str, list], **kwargs):
		super().__init__(filepaths, **kwargs)


class AVRSFromOFDB(SpectrumOfdbClient, SOEtoAVRS):

	def __init__(self, date_start:datetime, date_stop:datetime=None, **kwargs):
		super().__init__(date_start, date_stop, **kwargs)


class AVRSFromFile(SpectrumFileReader, SOEtoAVRS):

	def __init__(self, filepaths:Union[str, list], **kwargs):
		super().__init__(filepaths, **kwargs)