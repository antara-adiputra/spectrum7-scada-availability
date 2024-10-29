import asyncio, datetime, gc, os, platform, re, time
from glob import glob
from io import BytesIO
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List, Callable, Generator, Literal, Iterable, Optional, Tuple, TypeAlias, Union

import pandas as pd
import xlsxwriter
from xlsxwriter.utility import xl_col_to_name

from ..lib import immutable_dict


class Progress:
	_value: float
	_name: str

	def __init__(self, **kwargs) -> None:
		self.init()
		self._set_attribute(**kwargs)
		super().__init__(**kwargs)

	def _set_attribute(self, **kwargs) -> None:
		for key, val in kwargs.items():
			setattr(self, key, val)

	def init(self, name: Optional[str] = None, *args, **kwargs) -> None:
		"""Reset progress"""
		self._name = name
		self._value = 0.0
		self._set_attribute(**kwargs)

	def update(self, value: float, *args, **kwargs) -> None:
		"""Update progress"""
		self._value = value
		self._set_attribute(**kwargs)


	@property
	def name(self):
		return self._name

	@property
	def value(self):
		return self._value


class BaseAvailability:
	"""Basic parameter of availability analyze & calculation"""
	__params__: set = set()
	_errors: List[Any]
	_warnings: List[Any]
	keep_duplicate: Literal['first', 'last', 'none'] = 'last'

	def __init__(self, **kwargs) -> None:
		self._mro = self.__class__.__mro__
		self._errors = list()
		self._warnings = list()
		self.progress = Progress()
		# Add accepted attribute
		for key in kwargs:
			if key in self.__params__: setattr(self, key, kwargs[key])
		# super().__init__(**kwargs)

	def initialize(self) -> None:
		self._errors = list()
		self._warnings = list()

	def get_range(self) -> Tuple[datetime.datetime, datetime.datetime]:
		"""Get adjusted time for start and stop datetime of query data."""
		return self.t0, self.t1

	def set_range(self, start: datetime.datetime, stop: datetime.datetime) -> Tuple[datetime.datetime, datetime.datetime]:
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


	@property
	def errors(self):
		return self._errors

	@property
	def mro(self):
		return self._mro

	@property
	def t0(self) -> datetime.datetime:
		return getattr(self, '_t0', None)

	@property
	def t1(self) -> datetime.datetime:
		return getattr(self, '_t1', None)
	
	@property
	def warnings(self):
		return self._warnings


class XLSExportMixin:
	"""Base class mixin for exporting dataframe into excel file."""
	_sheet_parameter: MappingProxyType[str, Dict[str, Any]] = immutable_dict({})
	t0: datetime.datetime
	t1: datetime.datetime
	name: str
	process_date: datetime.datetime
	process_duration: float
	base_dir: Path = Path(__file__).parent.resolve()
	output_dir: Path = base_dir / 'output'
	output_extension: str = 'xlsx'
	output_prefix: str = ''

	def _worksheet_writer(self, workbook: xlsxwriter.Workbook, sheet_name: str, sheet_data: pd.DataFrame, *extra_data):
		"""Dataframe to excel sheet convertion.

		Args:
			workbook : current working workbook
			sheet_name : sheet name
			sheet_data : sheet content

		Accepted extra_data:
			Extra dataframe
		"""
		ws = workbook.add_worksheet(sheet_name)
		# Worksheet formatting
		format_header = {'num_format': '@', 'border': 1, 'bold': True, 'align': 'center', 'valign': 'top', 'font_color': 'black', 'bg_color': '#ededed'}
		format_base = {'valign': 'vcenter'}
		format_footer = {'bold': True, 'border': 0, 'font_color': 'black', 'bg_color': '#dcdcdc'}

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
		"""Define file properties."""
		return {
			'title': f'Hasil kalkulasi {self.name} tanggal {self.t0.strftime("%d-%m-%Y")} s/d {self.t1.strftime("%d-%m-%Y")}',
			'subject': f'{self.name}',
			'author': f'Python {platform.python_version()}',
			'manager': 'Fasop SCADA',
			'company': 'PLN UP2B Sistem Makassar',
			'category': 'Excel Automation',
			'comments': f'File digenerate otomatis oleh program {self.name}'
		}

	def get_xls_filename(self, **kwargs) -> str:
		"""Generate filename."""
		filename = kwargs.get('filename')
		if filename:
			return filename
		else:
			return f'{self.output_prefix}_Output_{self.t0.strftime("%Y%m%d")}-{self.t1.strftime("%Y%m%d")}'

	def get_sheet_info_data(self, **kwargs) -> list[tuple[str, str]]:
		"""Generate sheet "Info" content."""
		return [
			('Source File', getattr(self, 'sources', '')),
			('Output File', f'{kwargs.get("filepath", "")}'),
			('Date Range', f'{self.t0.strftime("%d-%m-%Y")} s/d {self.t1.strftime("%d-%m-%Y")}'),
			('Processed Date', self.process_date.strftime('%d-%m-%Y %H:%M:%S')),
			('Execution Time', f'{self.process_duration}s'),
			('PC', platform.node()),
			('User', os.getlogin())
		]

	def prepare_export(self, **kwargs) -> dict[str, Any]:
		"""Prepare result for excel export.
		Override this function to handle process before export.
		"""
		return super().prepare_export(**kwargs)
	
	def _writer(self, data: Dict[str, Any], output_filename: str, *args, **kwargs) -> Union[BytesIO, str]:
		"""Write data into excel file / buffer.
		
		Args:
			data : dictionary of sheets and contents
			filename : output filename

		Result:
			IOBuffer or filepath
		"""
		if kwargs.get('as_iobuffer'):
			target = BytesIO()
		else:
			target = output_filename

		with xlsxwriter.Workbook(target) as wb:
			# Set excel workbook file properties
			wb.set_properties(self.get_xls_properties())

			for name, sheet in data.items():
				if isinstance(sheet, (tuple, list)):
					self._worksheet_writer(wb, name, sheet[0], *sheet[1:])
				else:
					self._worksheet_writer(wb, name, sheet)

			# Write worksheet info
			ws_info = wb.add_worksheet('Info')
			rows = self.get_sheet_info_data(filepath=output_filename)

			for i, row in enumerate(rows):
				ws_info.write_row(i, 0, row)

			ws_info.autofit()
			ws_info.set_column(0, 0, None, wb.add_format({'valign': 'vcenter', 'num_format': '@', 'bold': True}))
			ws_info.set_column(1, 1, 100, wb.add_format({'valign': 'vcenter', 'num_format': '@', 'text_wrap': True}))

		return target

	def to_excel(self, as_iobuffer: bool = False, *args, **kwargs) -> Union[BytesIO, str]:
		"""Export data into excel file.
		
		Args:
			as_iobuffer : create as io buffer for file stream

		Result:
			IOBuffer or Workbook object
		"""
		sheets_data = self.prepare_export(generate_formula=True, **kwargs)
		filename = self.get_xls_filename(**kwargs)

		if as_iobuffer:
			output_filepath = f'{filename}.{self.output_extension}'
		else:	
			# Check target directory of output file
			if not os.path.isdir(self.output_dir): os.mkdir(self.output_dir)
			file_list = glob(f'{self.output_dir}/{filename}*.{self.output_extension}')
			if len(file_list)>0: filename += f'_rev{len(file_list)}'
			output_filepath = f'{self.output_dir}/{filename}.{self.output_extension}'
		# Create excel file
		output = self._writer(sheets_data, output_filepath, as_iobuffer=as_iobuffer, *args, **kwargs)

		if as_iobuffer:
			return output.getvalue()
		else:
			print(f'Data berhasil di-export pada "{output}".')
			return output
