import os, platform, re, time
from datetime import date, datetime, timedelta
from glob import glob
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
import xlsxwriter
from xlsxwriter.utility import xl_col_to_name
from filereader import AvFileReader, SpectrumFileReader
from global_parameters import RTU_BOOK_PARAM
from lib import BaseExportMixin, read_xls, join_datetime, immutable_dict, load_cpoint, progress_bar, timedelta_split
from ofdb import SpectrumOfdbClient


class AvRemoteStation:
	_sheet_parameter = immutable_dict(RTU_BOOK_PARAM)
	name = 'Availability Remote Station'
	table_size = 1000
	table_columns = ['Down Time', 'Up Time', 'RTU', 'Long Name', 'Duration', 'Annotations']
	maintenance_mark = '**maintenance**'
	unused_mark = '**unused**'
	keep_duplicate = 'last'
	
	def __init__(self, input:Union[SpectrumFileReader, SpectrumOfdbClient], **kwargs):
		self._analyzed = False

		if input.date_range!=None:
			self._date_start = input.date_range[0].to_pydatetime() if isinstance(input.date_range[0], pd.Timestamp) else input.date_range[0]
			self._date_stop = input.date_range[1].to_pydatetime() if isinstance(input.date_range[1], pd.Timestamp) else input.date_range[1]
			self.ifs_messages = input.soe_rtu_updown.copy()
		else:
			raise AttributeError('Variable "date_range" belum diset.')

		self.base_dir = Path(__file__).parent.resolve()
		self.output_dir = self.base_dir / 'output'
		self.output_extension = 'xlsx'
		self.output_filename = f'AvRS_Output_{self.date_start.strftime("%Y%m%d")}-{self.date_stop.strftime("%Y%m%d")}'
		self.sources = input.sources

		# Set date_range if defined in kwargs
		if 'date_start' in kwargs: self._date_start = kwargs['date_start']
		if 'date_stop' in kwargs: self._date_stop = kwargs['date_stop']

		try:
			if hasattr(input, 'cpoint_description'):
				self.cpoint_ifs = input.cpoint_description[(input.cpoint_description['B1']=='IFS') & (input.cpoint_description['B2']=='RTU_P1')]
			else:
				print('Memuat data "Point Name Description"...', end='')
				df_cpoint = read_xls('point_name_description.xlsx', is_soe=False).fillna('')
				# Remove duplicates to prevent duplication in merge process
				self.cpoint_ifs = df_cpoint[(df_cpoint['B1']=='IFS') & (df_cpoint['B2']=='RTU_P1')].drop_duplicates(subset=['B1 text', 'B2 text', 'B3 text'], keep='first')
		except ValueError:
			self.cpoint_ifs = pd.DataFrame()

	def analyze(self, rtus:list):
		"""
		Analyzed every Up/Down event with presume that all RTUs are Up in the date_start.
		"""

		df = self.ifs_messages
		analyzed_rows = []
		maintenance_rows = []
		i_sys_tstamp = df.columns.get_loc('System time stamp')
		i_sys_msec = df.columns.get_loc('System milliseconds')
		i_status = df.columns.get_loc('Status')
		i_user_comment = df.columns.get_loc('User comment')
		i_b3_text = df.columns.get_loc('B3 text')

		print(f'\nMenganalisa downtime dari {len(rtus)} Remote Station...')
		for x, rtu in enumerate(rtus):
			progress_bar((x+1)/len(rtus))

			df_rtu = df[df['B3']==rtu]
			
			index0 = self.index_min
			t0 = self.date_start
			mnemo = rtu
			anno = []

			for y in range(df_rtu.shape[0]):
				t1 = join_datetime(*df_rtu.iloc[y, [i_sys_tstamp, i_sys_msec]])
				sts, cmt, des = df_rtu.iloc[y, [i_status, i_user_comment, i_b3_text]]

				# Copy User Comment if any
				if cmt:
					if self.maintenance_mark in cmt:
						anno.append('User menandai downtime pemeliharaan**')
						maintenance_rows.append((t0, t1, mnemo, des, downtime, '\n'.join(anno)))
					elif self.unused_mark in cmt:
						anno.append('User menandai downtime dianulir**')
					else:
						# Eleminate unnecessary character
						txt = re.sub('^\W*|\s*$', '', cmt)
						anno += txt.split('\n')

				if sts=='Up':
					# Calculate downtime duration in second and append to analyzed_rows
					downtime = t1 - t0

					if downtime>timedelta(days=3):
						# Give critical alarm
						anno.append('Downtime > 72 jam (Critical)')
					elif downtime>timedelta(days=1) and downtime<=timedelta(days=3):
						# Give major alarm
						anno.append('Downtime > 24 jam (Major)')
					elif downtime>timedelta(hours=3) and downtime<=timedelta(days=1):
						# Give minor alarm
						anno.append('Downtime > 3 jam (Minor)')

					analyzed_rows.append((t0, t1, mnemo, des, downtime, '\n'.join(anno), f'=HYPERLINK("#HIS_MESSAGES!A{index0+2}:N{df_rtu.iloc[y].name+2}"," CARI >> ")'))

					# Reset anno
					anno = []
				elif sts=='Down':
					if y==df_rtu.shape[0]-1:
						# RTU down until max time range
						downtime = self.date_stop - t1

						if downtime>timedelta(days=3):
							# Give critical alarm
							anno.append('Downtime > 72 jam (Critical)')
						elif downtime>timedelta(days=1) and downtime<=timedelta(days=3):
							# Give major alarm
							anno.append('Downtime > 24 jam (Major)')
						elif downtime>timedelta(hours=3) and downtime<=timedelta(days=1):
							# Give minor alarm
							anno.append('Downtime > 3 jam (Minor)')

						analyzed_rows.append((t1, self.date_stop, mnemo, des, downtime, '\n'.join(anno), f'=HYPERLINK("#HIS_MESSAGES!A{df_rtu.iloc[y].name+2}:N{self.index_max+2}"," CARI >> ")'))

						# Reset anno
						anno = []
					else:
						index0 = df_rtu.iloc[y].name
						t0 = t1

		# Sort DataFrame base on Down Time
		df_downtime = pd.DataFrame(data=analyzed_rows, columns=self.table_columns + ['Navigation']).sort_values(['Down Time', 'Up Time'], ascending=[True, True]).reset_index(drop=True)
		self.analyzed_downtime = df_downtime

		# Initialize Maintenance table
		df_maintenance = pd.DataFrame(data=maintenance_rows, columns=self.table_columns).sort_values(['Down Time', 'Up Time'], ascending=[True, True]).reset_index(drop=True)
		self.analyzed_maintenance = df_maintenance

		return df_downtime

	def calculate(self):
		"""
		Main process of availability calculation.
		"""

		rtu_list = self.setup()

		if self.is_valid:
			self._process_date = date.today()
			self._process_begin = time.time()

			# Check RTU Up/Down event
			df_downtime = self.analyze(rtu_list)
			self._summary = self.get_summary(df_downtime)

			self._process_stop = time.time()
			self._process_duration = round(self.process_stop - self.process_begin, 2)
			self._analyzed = True

	def export_result(self, filename:str=''):
		"""
		Export analyzed result into Excel file.
		"""

		# Check if Availability has been analyzed
		if self.analyzed==False:
			return print('Tidak dapat export data Availability Remote Station. Jalankan fungsi "calculate()" terlebih dahulu.')

		# Check required data
		try:
			overall = self.summary['overall']
			statistic = self.summary['statistic']
			worksheets = self.summary['export']
			df_historical = worksheets['HIS_MESSAGES']
			output_filename = self.output_filename
			self.prepare_export()
		except (AttributeError, KeyError, ValueError):
			return print('Tidak dapat export file Availability.')

		# Check target directory of output file
		if not os.path.isdir(self.output_dir): os.mkdir(self.output_dir)

		if filename:
			# Use defined filename
			if '.' in filename: filename = filename.split('.')[0]
		else:
			# Use default filename instead
			filename = output_filename
		
		file_list = glob(f'{self.output_dir}/{filename}*.{self.output_extension}')
		if len(file_list)>0: filename += f'_rev{len(file_list)}'

		output_file_properties = {
			'title': f'Hasil kalkulasi Availability Remote Station tanggal {self.date_start.strftime("%d-%m-%Y")} s/d {self.date_stop.strftime("%d-%m-%Y")}',
			'subject': 'Perhitungan Availability',
			'author': 'Python 3.11',
			'manager': 'Fasop SCADA',
			'company': 'PLN UP2B Sistem Makassar',
			'category': 'Excel Automation',
			'comments': 'File digenerate otomatis oleh program Kalkulasi Availability Remote Station'
		}

		# Create excel file
		with xlsxwriter.Workbook(self.output_dir / f'{filename}.{self.output_extension}') as wb:
			# Set excel workbook file properties
			wb.set_properties(output_file_properties)

			for name, sheet in worksheets.items():
				self.worksheet_writer(wb, name, sheet)

			# Write worksheet info
			ws_info = wb.add_worksheet('Info')
			rows = [
				('Source File', self.sources),
				('Output File', f'{filename}.{self.output_extension}'),
				('Date Range', f'{self.date_start.strftime("%d-%m-%Y")} s/d {self.date_stop.strftime("%d-%m-%Y")}'),
				('Processed Date', datetime.now().strftime('%d-%m-%Y %H:%M:%S')),
				('Execution Time', f'{self.process_duration}s'),
				('PC', platform.node()),
				('User', os.getlogin()),
				('', ''),
				('STATISTICS', ''),
				('Availability', overall['availability']),
				('Downtime Occurences', statistic['downtime_count']),
				('Overall Average Downtime', statistic['downtime_avg']),
				('Overall Longest Downtime', statistic['downtime_max']),
				('Overall Shortest Downtime', statistic['downtime_min']),
				('RTU All', overall['total_rtu']),
				('RTU with Downtime', overall['total_rtu_down']),
				('Most Down Occurences', overall['rtu_count_max']),
				('Least Down Occurences', overall['rtu_count_min']),
				('Longest Downtime', overall['rtu_down_max']),
				('Shortest Downtime', overall['rtu_down_min']),
				('Largest Average Downtime', overall['rtu_avg_max']),
				('Smallest Average Downtime', overall['rtu_avg_min'])
			]
			for i, row in enumerate(rows):
				ws_info.write_row(i, 0, row)
			ws_info.set_column(0, 0, 20, wb.add_format({'valign': 'vcenter', 'num_format': '@', 'bold': True}))
			ws_info.set_column(1, 1, 100, wb.add_format({'valign': 'vcenter', 'num_format': '@', 'text_wrap': True}))

		print(f'Data berhasil di-export pada "{self.output_dir / filename}.{self.output_extension}".')

	def get_summary(self, df:pd.DataFrame):
		"""
		Get aggregate data, then return list of Excel worksheet name and Dataframe wrapped into dictionaries.
		"""

		# Filter which only rows with not unused-marked
		df0 = df.loc[~(df['Annotations'].str.contains('dianulir\*\*') | df['Annotations'].str.contains('pemeliharaan\*\*'))]

		df1 = self.analyzed_maintenance
		df2 = self.groupby(df)
		self.analyzed_rtu = df2

		# Initialize Maintenance table


		# Calculate statistics
		down_all = df0.shape[0]
		down_avg = df0['Duration'].mean()
		down_avg_dd, down_avg_hh, down_avg_mm, down_avg_ss = timedelta_split(down_avg)
		down_max = df0['Duration'].max()
		down_max_dd, down_max_hh, down_max_mm, down_max_ss = timedelta_split(down_max)
		down_min = df0['Duration'].min()
		down_min_dd, down_min_hh, down_min_mm, down_min_ss = timedelta_split(down_min)

		av = round((df2['Calculated Availability'].sum() / df2['Calculated Availability'].count()) * 100, 2)
		rtu_count_max = df2.loc[df2['Downtime Occurences'].idxmax()]['RTU']
		rtu_count_min = df2.loc[df2['Downtime Occurences'].idxmin()]['RTU']
		rtu_down_max = df2.loc[df2[df2['Downtime Occurences']>0]['Total Downtime'].idxmax()]['RTU']
		rtu_down_min = df2.loc[df2[df2['Downtime Occurences']>0]['Total Downtime'].idxmin()]['RTU']
		rtu_avg_max = df2.loc[df2[df2['Downtime Occurences']>0]['Average Downtime'].idxmax()]['RTU']
		rtu_avg_min = df2.loc[df2[df2['Downtime Occurences']>0]['Average Downtime'].idxmin()]['RTU']

		return {
			'export': {
				'HIS_MESSAGES': self.ifs_messages,
				'DOWNTIME': df,
				'MAINTENANCE': df1,
				'RTU': df2
			},
			'overall': {
				'total_rtu': df2.shape[0],
				'total_rtu_down': df2[df2['Downtime Occurences']>0].shape[0],
				'availability': f'{av}%',
				'rtu_count_max': rtu_count_max,
				'rtu_count_min': rtu_count_min,
				'rtu_down_max': rtu_down_max,
				'rtu_down_min': rtu_down_min,
				'rtu_avg_max': rtu_avg_max,
				'rtu_avg_min': rtu_avg_min
			},
			'statistic': {
				'downtime_count': f'{down_all}',
				'downtime_avg': f'{down_avg_dd} Hari {down_avg_hh:02}:{down_avg_mm:02}:{down_avg_ss:02}',
				'downtime_max': f'{down_max_dd} Hari {down_max_hh:02}:{down_max_mm:02}:{down_max_ss:02}',
				'downtime_min': f'{down_min_dd} Hari {down_min_hh:02}:{down_min_mm:02}:{down_min_ss:02}'
			}
		}

	def groupby(self, df:pd.DataFrame):
		"""
		Return DataFrameGroupBy Class of aggregation values which used in all grouped Dataframe with groupby_columns as columns parameter.
		"""
		
		columns = ['RTU', 'Long Name']
		groupby_columns = columns + ['Duration']
		output_columns = ['RTU', 'Long Name', 'Time Range', 'Downtime Occurences', 'Total Downtime', 'Average Downtime', 'Longest Downtime', 'Longest Downtime Date', 'Uptime', 'Maintenance', 'Calculated Availability', 'Quality', 'Availability']

		rtu_table = self.cpoint_ifs[['B3', 'B3 text']].rename(columns={'B3': 'RTU', 'B3 text': 'Long Name'})
		down_count = df[groupby_columns].groupby(columns, as_index=False).count().rename(columns={'Duration': 'Downtime Occurences'})
		down_agg = df[groupby_columns].groupby(columns).agg(['sum', 'mean', 'max']).reset_index()
		down_agg.columns = ['RTU', 'Long Name', 'Total Downtime', 'Average Downtime', 'Longest Downtime']
		filter_max_downtime = df.groupby(columns, as_index=False)['Duration'].transform('max')==df['Duration']
		down_max_t = df[columns + ['Down Time']][filter_max_downtime].rename(columns={'Down Time': 'Longest Downtime Date'})

		# Merge table and fill NaN Downtime Occurences to 0
		df_groupby = rtu_table.merge(right=down_count, how='left', on=columns).fillna(0)
		# Merge existing table with aggregated table and fill NaT with timedelta(0 second)
		df_groupby = df_groupby.merge(right=down_agg, how='left', on=columns).fillna(timedelta(seconds=0))
		df_groupby = df_groupby.merge(right=down_max_t, how='left', on=columns).fillna(self.date_stop)

		df_groupby['Time Range'] = self.date_stop - self.date_start + timedelta(microseconds=1)
		df_groupby['Uptime'] = df_groupby['Time Range'] - df_groupby['Total Downtime']
		df_groupby['Maintenance'] = timedelta(seconds=0)
		df_groupby['Calculated Availability'] = round(df_groupby['Uptime']/df_groupby['Time Range'], 4)
		df_groupby['Quality'] = 1
		df_groupby['Availability'] = ''

		return df_groupby[output_columns]

	def prepare_export(self):
		"""
		Prepare DataFrame for Excel export.
		"""

		sumary_columns = self.analyzed_rtu.columns.to_list()

		# Lookup on table Maintenance with RTU column as key and Duration as lookup value
		xlcol_m_down = xl_col_to_name(self.table_columns.index('Down Time'))
		xlcol_m_up = xl_col_to_name(self.table_columns.index('Up Time'))
		xlcol_m_rtu = xl_col_to_name(self.table_columns.index('RTU'))
		xlcol_m_dura = xl_col_to_name(self.table_columns.index('Duration'))
		# Column index in RTU table
		xlcol_r_rtu = xl_col_to_name(sumary_columns.index('RTU'))

		print('Menyiapkan data...')
		# Prepare RTU table
		self.analyzed_rtu['Maintenance'] = np.array([f'=SUMPRODUCT((MAINTENANCE!${xlcol_m_rtu}$2:${xlcol_m_rtu}${self.table_size+1}=${xlcol_r_rtu}{row+2})*MAINTENANCE!${xlcol_m_dura}$2:${xlcol_m_dura}${self.table_size+1})' for row in range(self.analyzed_rtu.shape[0])])
		# Prepare Maintenance table
		self.analyzed_maintenance['Duration'] = np.array([f'=${xlcol_m_up}{row+2}-${xlcol_m_down}{row+2}' for row in range(self.table_size)])
		self.analyzed_maintenance.fillna('', inplace=True)

	def setup(self):
		"""
		Pre-calculation setup.
		"""

		# Normalize date_start and date_stop
		self._date_start.replace(hour=0, minute=0, second=0, microsecond=0)
		self._date_stop.replace(hour=23, minute=59, second=59, microsecond=999999)

		self.ifs_messages = self.ifs_messages.sort_values(['System time stamp', 'System milliseconds'], ascending=[True, True]).reset_index(drop=True)

		# Get min index and max index of df
		self._index_min, self._index_max = self.ifs_messages.index.min(), self.ifs_messages.index.max()

		if self.ifs_messages.shape[0]>0:
			self._is_valid = True
			rtu_index = self.ifs_messages['B3'].unique()
		else:
			self._is_valid = False
			rtu_index = []
			print('Dataframe tidak valid')

		return rtu_index

	def worksheet_writer(self, workbook:xlsxwriter.Workbook, sheet_name:str, sheet_data:pd.DataFrame, *args):
		"""
		Parameterized each exported worksheet.
		"""

		ws = workbook.add_worksheet(sheet_name)

		# Worksheet formatting
		format_header = {'border': 1, 'bold': True, 'align': 'center', 'bg_color': '#ededed'}
		format_base = {'valign': 'vcenter'}
		format_row_summary = {'bold': True, 'bg_color': '#dcdcdc'}

		nrow, ncol = sheet_data.shape
		tbl_header = sheet_data.columns.to_list()
		
		# Write worksheet header
		ws.write_row(0, 0, tbl_header, workbook.add_format(format_header))

		for x, col in enumerate(tbl_header):
			# Write table body
			ws.write_column(1, x, sheet_data[col].fillna(''), workbook.add_format({**format_base, **self._sheet_parameter['format'].get(col, {})}))
			ws.set_column(x, x, self._sheet_parameter['width'].get(col))
			xlcol = xl_col_to_name(x)

			if sheet_name=='DOWNTIME':
				start_col = sheet_data.columns.get_loc('Duration')
				result_col = xl_col_to_name(sheet_data.columns.get_loc('Duration'))
				formula_col = xl_col_to_name(start_col)

				# Write additional summary rows
				if x==len(tbl_header)-1:
					ws.write_row(nrow+1, 0, tuple(['']*(start_col-1) + ['TOTAL DOWN (RAW)', f'=COUNTA({result_col}2:{result_col}{nrow+1})'] + ['']*(len(tbl_header)-start_col-1)), workbook.add_format(format_row_summary))
			
			elif sheet_name=='RTU':
				# Write additional summary rows
				xlcol_trange = xl_col_to_name(2)
				xlcol_tdown = xl_col_to_name(4)
				xlcol_tmain = xl_col_to_name(9)
				xlcol_quality = xl_col_to_name(11)

				if col in ['Downtime Occurences', 'Quality']:
					# ['Time Range', 'Downtime Occurences', 'Total Downtime', 'Uptime', 'Maintenance', 'Quality']
					ws.write(nrow+1, x, f'=SUM({xlcol}2:{xlcol}{nrow+1})', workbook.add_format({**self._sheet_parameter['format'].get(col, {}), **format_row_summary}))
				elif col=='Calculated Availability':
					ws.write(nrow+1, x, f'=AVERAGE({xlcol}2:{xlcol}{nrow+1})', workbook.add_format({**self._sheet_parameter['format'].get(col, {}), **format_row_summary}))
				elif x==len(tbl_header)-1 or col=='Availability':
					ws.write_column(1, x, [f'=({xlcol_trange}{row+2}-{xlcol_tdown}{row+2}+{xlcol_tmain}{row+2})/{xlcol_trange}{row+2}*{xlcol_quality}{row+2}' for row in range(nrow)], workbook.add_format({**format_base, **self._sheet_parameter['format'].get(col, {})}))
					ws.write(nrow+1, x, f'=SUM({xlcol}2:{xlcol}{nrow+1})/{xlcol_quality}{nrow+2}', workbook.add_format({**self._sheet_parameter['format'].get(col, {}), **format_row_summary}))
				else:
					ws.write(nrow+1, x, '', workbook.add_format({**self._sheet_parameter['format'].get(col, {}), **format_row_summary}))

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

	@property
	def analyzed(self):
		return self._analyzed

	@property
	def date_start(self):
		return self._date_start

	@property
	def date_stop(self):
		return self._date_stop

	@property
	def index_max(self):
		return self._index_max

	@property
	def index_min(self):
		return self._index_min

	@property
	def is_valid(self):
		return self._is_valid

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
	def summary(self):
		if self.analyzed:
			return immutable_dict(self._summary)
		else:
			print('Data belum tersedia! Jalankan fungsi "calculate()" terlebih dahulu.')
			return None


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

		df = self.soe_all
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
	pass


class AVRSFromFile(SpectrumFileReader, SOEtoAVRS):

	def __init__(self, filepaths:Union[str, list], **kwargs):
		super().__init__(filepaths, **kwargs)