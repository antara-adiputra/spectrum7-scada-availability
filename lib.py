import os, platform, time
from datetime import datetime, timedelta
from difflib import SequenceMatcher, get_close_matches
from glob import glob
from pathlib import Path
from types import MappingProxyType

import pandas as pd
import xlsxwriter
from xlsxwriter.utility import xl_col_to_name
from lxml import etree as et
from global_parameters import SOE_COLUMNS


# decorator to calculate duration
# taken by any function.
def calc_time(func):
	# added arguments inside the inner1,
	# if function takes any arguments,
	# can be added like this.
	def inner1(*args, **kwargs):
		# storing time before function execution
		tock = time.time()
		returned_value = func(*args, **kwargs)
		# storing time after function execution
		tick = time.time()
		print(f' ({round(tick-tock, 2)}s)')

		return returned_value
 
	return inner1

def get_datetime(series:pd.Series):
	"""Return set of RTU Timestamp and System Timestamp"""

	return join_datetime(series['Time stamp'], series['Milliseconds']), join_datetime(series['System time stamp'], series['System milliseconds'])

def get_execution_duration(s0:pd.Series, s1:pd.Series):
	"""Calculate delta RTU time stamp"""

	delta_time = join_datetime(*s1.loc[['Time stamp', 'Milliseconds']].to_list()) - join_datetime(*s0.loc[['Time stamp', 'Milliseconds']].to_list())
	return round(delta_time.total_seconds(), 3)

def get_ifs_name(list1:list, list2:list):
	cross_connect_dict = {}
	if len(list1)>0 and len(list2)>0:
		avg1 = sum([len(s1) for s1 in list1])/len(list1)
		avg2 = sum([len(s2) for s2 in list2])/len(list2)
		if avg1>avg2:
			list1, list2 = list2, list1
		for b1 in list1:
			n = len(b1)
			match = get_close_matches(b1, list2, n, 0.75)
			if match:
				key = match[0] if avg1>avg2 else b1
				value = b1 if avg1>avg2 else match[0]
				cross_connect_dict.update({key: value})
	return cross_connect_dict

def get_table(ws_element:et._Element, namespace:dict):
	data = []
	rows = ws_element.findall('.//Row', namespaces=namespace)
	for irow, row in enumerate(rows):
		cells = row.findall('.//Data', namespaces=namespace)
		# HIS_MESSAGES column length = 18
		cell_data = [str(c.text).strip().replace('None', '') for i, c in enumerate(cells) if i<18]
		if irow==0:
			columns = cell_data
		else:
			if len(columns)==len(cell_data): data.append(tuple(cell_data))
	return columns, data

def get_termination_duration(s0:pd.Series, s1:pd.Series):
	"""Calculate delta Master Station time stamp"""

	delta_time = join_datetime(*s1.loc[['System time stamp', 'System milliseconds']].to_list()) - join_datetime(*s0.loc[['System time stamp', 'System milliseconds']].to_list())
	return round(delta_time.total_seconds(), 3)

def immutable_dict(input:dict):
	for key, item in input.items():
		if type(item)==dict:
			input[key] = immutable_dict(item)
	return MappingProxyType(input)

def join_datetime(dt:pd.Series, ms:pd.Series):
	"""Combine His. Messages style of separated datetime timestamp and milliseconds"""

	return pd.to_datetime(dt) + pd.to_timedelta(ms, unit='ms')

def load_cpoint(path:str):
	"""
	"""

	# Load point description
	print('\nMemuat data "Point Name Description"...', end='', flush=True)
	try:
		# Open first sheet
		df_cpoint = pd.read_excel(path, sheet_name=0).fillna('')
		# Remove duplicates to prevent duplication in merge process
		cpoint = validate_cpoint(df_cpoint)
		print('\tOK!')
	except FileNotFoundError:
		raise FileNotFoundError(f'\tNOK!\nFile "{path}" tidak ditemukan.')
	except Exception:
		raise ValueError(f'\tNOK!\nGagal membuka file "{path}".')
	
	return cpoint
	
def load_workbook(filepath:str):
	"""
	Load whole excel file as dict of worksheets.
	"""

	wb = {}

	try:
		wb = pd.read_excel(filepath, sheet_name=None)
	except FileNotFoundError:
		raise FileNotFoundError
	except Exception:
		raise ImportError
	
	return wb
	
def progress_bar(value:float, width:int=0, style:str='full-block'):
	symbol = {'full-block': '█', 'left-half-block': '▌', 'right-half-block': '▐'}
	if width==0: width = os.get_terminal_size()[0]-1
	percentage = int(value*100)
	char_block = symbol.get(style, style)
	if value<1:
		char_length = int(value*(width-5))
		print(f'\r {str(char_block*char_length).ljust(width-5, "-")} {percentage}%', end='', flush=True)
	else:
		print(f'\r {"Selesai... 100%".ljust(width, " ")}', flush=True)

def read_xls(filepath:str, sheet:str=None, is_soe:bool=True, **kwargs):
	"""
	"""

	wb = load_workbook(filepath)

	if sheet:
		if sheet in wb:
			df = wb[sheet]
		else:
			raise KeyError(f'Sheet "{sheet}" tidak ditemukan.')
	else:
		if is_soe:
			for ws_name, sheet in wb.items():
				if set(SOE_COLUMNS).issubset(sheet.columns):
					df = sheet[sheet['Time stamp'].notnull()].fillna('')
					break
		else:
			df = list(wb.values())[0]

	if df.shape[0]>0:
		return df
	else:
		raise ValueError

def read_xml(filepath:str, **kwargs):
	columns, rows = [], []
	
	try:
		xml = et.parse(filepath)
	except FileNotFoundError:
		raise FileNotFoundError
	except Exception:
		raise ImportError

	if type(xml)==et._ElementTree:
		xml_root = xml.getroot()
		# Get namespaces
		ns = xml_root.nsmap
		sheets = xml.findall('.//Worksheet', namespaces=ns)
		for sheet in sheets:
			if 'HIS_MESSAGES' in sheet.values():
				columns, rows = get_table(sheet, ns)
				break

	if len(columns)>0 and len(rows)>0:
		return pd.DataFrame(data=rows, columns=columns).fillna('')
	else:
		raise ValueError
	
def similarity_ratio(str1:str, str2:str):
	return SequenceMatcher(None, str1, str2).ratio()
	
def test_datetime_format(x):
	if type(x)!=str: return x

	# Replace string for ISO format
	if len(x)>19: x = x[0:19].replace('T', ' ')
	separator = '-' if '-' in x else '/'
	dtformats = [f'%d{separator}%m{separator}%Y', f'%d{separator}%m{separator}%y', f'%Y{separator}%m{separator}%d', f'%y{separator}%m{separator}%d', f'%m{separator}%d{separator}%Y', f'%m{separator}%d{separator}%y', f'%Y{separator}%d{separator}%m', f'%y{separator}%d{separator}%m']
	for dt in dtformats:
		format = dt + ' %H:%M:%S'
		try:
			result = datetime.strptime(x, format)
		except ValueError:
			result = False
		if result:
			x = result
			break
	return x

def timedelta_split(td:timedelta):
	"""
	"""

	dd = td.days
	sec = td.seconds
	hh = sec // 3600
	mm = (sec // 60) - (hh * 60)
	ss = sec % 60

	return dd, hh, mm, ss

def test():
	pass

def validate_cpoint(df:pd.DataFrame, verbose:bool=False):
	"""
	"""

	columns_base = ['B1', 'B2', 'B3']
	columns_text = ['B1 text', 'B2 text', 'B3 text']

	new_df = df.copy().drop_duplicates(subset=columns_base+columns_text).sort_values(['B1', 'B2', 'B3'])
	# similarity ratio to get better description and remove unwanted data
	for col in columns_base:
		new_df[f'{col} ratio'] = new_df.apply(lambda d: similarity_ratio(d[col], d[f'{col} text']), axis=1)

	new_df['Ratio'] = new_df['B1 ratio'] * new_df['B2 ratio'] * new_df['B3 ratio']
	# get highest similarity ratio
	new_df = new_df[(new_df['B1']!='') & (new_df['Ratio']>0)]

	filter_highest_ratio = new_df.groupby(columns_base, as_index=False)['Ratio'].transform('max')==new_df['Ratio']
	new_df = new_df[filter_highest_ratio]

	if verbose:
		return new_df
	else:
		return new_df[columns_base + columns_text]


class BaseExportMixin:
	_sheet_parameter = immutable_dict({})
	base_dir = Path(__file__).parent.resolve()
	output_dir = base_dir / 'output'
	output_extension = 'xlsx'
	output_prefix = ''

	def _worksheet_writer(self, workbook:xlsxwriter.Workbook, sheet_name:str, sheet_data:pd.DataFrame, *extra_data):
		"""
		"""

		ws = workbook.add_worksheet(sheet_name)
		# Worksheet formatting
		format_header = {'num_format': '@', 'border': 1, 'bold': True, 'align': 'center', 'valign': 'top', 'bg_color': '#ededed'}
		format_base = {'valign': 'vcenter'}
		format_footer = {'bold': True, 'bg_color': '#dcdcdc'}

		nrow, ncol = sheet_data.shape
		tbl_header = sheet_data.columns.to_list()

		for x, col in enumerate(tbl_header):
			# Write table header
			ws.write(0, x, col, workbook.add_format({**self._sheet_parameter['format'].get(col, {}), **format_header}))
			# Write table body
			ws.write_column(1, x, sheet_data[col].fillna(''), workbook.add_format({**self._sheet_parameter['format'].get(col, {}), **format_base}))
			# Append row if any
			if extra_data:
				try:
					# Extra data must be in DataFrame type
					ext_row = extra_data[0].shape[0]
					if col in extra_data[0].columns:
						ws.write_column(nrow+1, x, extra_data[0][col], workbook.add_format({**self._sheet_parameter['format'].get(col, {}), **format_footer}))
					else:
						ws.write_column(nrow+1, x, ['']*ext_row, workbook.add_format({**self._sheet_parameter['format'].get(col, {}), **format_footer}))
				except Exception:
					print(f'ERROR! Footer kolom "{col}"')

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

		# Set columns width
		for x1, col1 in enumerate(tbl_header):
			if col1 in self._sheet_parameter['width']: ws.set_column(x1, x1, self._sheet_parameter['width'].get(col1))

	def get_xls_properties(self):
		"""
		"""

		return {
			'title': f'Hasil kalkulasi {self.name} tanggal {self.t0.strftime("%d-%m-%Y")} s/d {self.t1.strftime("%d-%m-%Y")}',
			'subject': f'{self.name}',
			'author': f'Python {platform.python_version()}',
			'manager': 'Fasop SCADA',
			'company': 'PLN UP2B Sistem Makassar',
			'category': 'Excel Automation',
			'comments': f'File digenerate otomatis oleh program {self.name}'
		}

	def get_xls_filename(self):
		"""
		"""

		return f'{self.output_prefix}_Output_{self.t0.strftime("%Y%m%d")}-{self.t1.strftime("%Y%m%d")}'

	def get_sheet_info_data(self, **kwargs):
		"""
		"""

		return [
			('Source File', getattr(self, 'sources', '')),
			('Output File', f'{kwargs.get("filepath", "")}'),
			('RC Date Range', f'{self.t0.strftime("%d-%m-%Y")} s/d {self.t1.strftime("%d-%m-%Y")}'),
			('Processed Date', self.process_date.strftime('%d-%m-%Y %H:%M:%S')),
			('Execution Time', f'{self.process_duration}s'),
			('PC', platform.node()),
			('User', os.getlogin())
		]

	def prepare_export(self, **kwargs):
		return super().prepare_export(**kwargs)

	def to_excel(self, **kwargs):
		"""
		"""

		sheets_data = self.prepare_export(generate_formula=True, **kwargs)

		# Check target directory of output file
		if not os.path.isdir(self.output_dir): os.mkdir(self.output_dir)

		filename = self.get_xls_filename()
		file_list = glob(f'{self.output_dir}/{filename}*.{self.output_extension}')

		if len(file_list)>0: filename += f'_rev{len(file_list)}'

		output_file_properties = self.get_xls_properties()
		output_filepath = self.output_dir / f'{filename}.{self.output_extension}'

		# Create excel file
		with xlsxwriter.Workbook(output_filepath) as wb:
			# Set excel workbook file properties
			wb.set_properties(output_file_properties)

			for name, sheet in sheets_data.items():
				if isinstance(sheet, (tuple, list)):
					self._worksheet_writer(wb, name, sheet[0], *sheet[1:])
				else:
					self._worksheet_writer(wb, name, sheet)

			# Write worksheet info
			ws_info = wb.add_worksheet('Info')
			rows = self.get_sheet_info_data(filepath=output_filepath)

			for i, row in enumerate(rows):
				ws_info.write_row(i, 0, row)

			ws_info.autofit()
			ws_info.set_column(0, 0, None, wb.add_format({'valign': 'vcenter', 'num_format': '@', 'bold': True}))
			ws_info.set_column(1, 1, 100, wb.add_format({'valign': 'vcenter', 'num_format': '@', 'text_wrap': True}))

		print(f'Data berhasil di-export pada "{self.output_dir / filename}.{self.output_extension}".')

	
if __name__ == '__main__':
	test()
	# print(glob('RC_Output_CB_20230301-20230331.xlsx', root_dir='E:/project/excel/output'))