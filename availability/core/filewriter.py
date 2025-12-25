import datetime, os, re
from dataclasses import dataclass, field, is_dataclass
from functools import cached_property, reduce
from glob import glob
from io import BytesIO

import pandas as pd
import xlsxwriter
from xlsxwriter.utility import cell_autofit_width, xl_col_to_name, xl_range_abs, xl_rowcol_to_cell

from .base import DataModel, FieldMetadata, model_fields
from .excel import *
from .soe import SOEModel
from ..lib import logprint
from ..settings import BASE_DIR
from ..types import *


TDM = TypeVar('TDM', bound=DataModel)


class InfoBase(TypedDict):
	source: str
	output: str
	date_range: Tuple[datetime.datetime, datetime.datetime]
	processed_date: datetime.datetime
	execution_time: float
	node: str
	user: str


class FileProperties(TypedDict):
	title: str
	subject: str
	author: str
	manager: str
	company: str
	category: str
	comments: str


class SheetWrapper(Generic[TDM]):
	header_format: ClassVar[XlsxFormat] = XlsxFormat(
		num_format='@',
		bold=True,
		border=1,
		align='center',
		valign='top',
		font_color='black',
		bg_color='#ededed'
	)
	footer_format: ClassVar[XlsxFormat] = XlsxFormat(
		bold=True,
		border=0,
		font_color='black',
		bg_color='#dcdcdc'
	)
	base_format: ClassVar[XlsxFormat] = XlsxFormat(valign='vcenter')

	def __init_subclass__(cls, model_class: Type[TDM]):
		cls.model_class = model_class

	def __init__(
		self,
		name: str,
		data: pd.DataFrame,
		row_offset: int = 1,
		autofilter: bool = True,
		**kwargs
	):
		super().__init__()
		self.sheet_name = name
		self.data = data.copy()
		self.formula: pd.DataFrame = None
		self.row_offset = row_offset
		self.enable_autofilter = autofilter

	def get_column_position(self, column: str) -> int:
		if isinstance(column, str):
			return self.fields_order.index(column)
		else:
			return self.fields_order.index(str(column))

	def get_xlcolumn(self, column: str) -> str:
		return self.xlcolumn[column]

	def xlcell_var(self, column: str, var_name: str = 'row', isexternal: bool = False, abs_col: bool = False, abs_row: bool = False) -> str:
		sheet = self.sheet_name + '!' if isexternal else ''
		mcol = '$' if abs_col else ''
		col = self.get_xlcolumn(column)
		mrow = '$' if abs_row else ''
		var_row = '{' + var_name + '}'
		return ''.join((sheet, mcol, col, mrow, var_row))

	def get_xlcolumn_range(self, column: str, isexternal: bool = False) -> str:
		"""Create excel notation for column range."""
		if isexternal:
			return f'{self.sheet_name}!{self.xlcolumn_range[column]}'
		else:
			return self.xlcolumn_range[column]

	def xlcell(self, row: int, column: str, isexternal: bool = False) -> str:
		"""Create excel notation for cell."""
		icol = self.get_column_position(column)
		if isexternal:
			return f'{self.sheet_name}!{xl_rowcol_to_cell(row, icol, True, True)}'
		else:
			return xl_rowcol_to_cell(row, icol, True, True)

	def generate_formula(self, sref: Optional[Self] = None, **kwargs):
		"""Generate excel cell formula as a Dataframe to update existing data."""
		pass

	def get_table_data(self, **kwargs) -> pd.DataFrame:
		# headers = model_mappings(self.model_class, as_dict=True)
		table_data = self.data.copy()
		if isinstance(self.formula, pd.DataFrame):
			for col in self.formula.columns:
				table_data[col] = self.formula[col]

		# table_data.columns = [headers[col] for col in table_data]
		return table_data.fillna('')

	def get_footer_data(self, **kwargs) -> Optional[pd.DataFrame]:
		"""Get data to write on table footer."""
		pass

	def add_sheet_to_workbook(self, workbook: xlsxwriter.Workbook, **kwargs) -> None:
		"""Add new worksheet into workbook and write data.

		Args:
			workbook : current working workbook

		Accepted extra_data:
			Extra dataframe
		"""
		# Copy and set formulas
		if self.sheet_name is None:
			sheet_name = f'Sheet{len(workbook.worksheets())+1}'
		else:
			sheet_name = self.sheet_name

		ws = workbook.add_worksheet(sheet_name)
		data = self.get_table_data(**kwargs)	# Data is now using header, not field name
		footer = self.get_footer_data()
		has_footer = isinstance(footer, pd.DataFrame)
		modfields = model_fields(self.model_class, as_dict=True)
		nrow, ncol = data.shape
		frzpane_y = 0

		for ix, field in enumerate(self.fields_order):
			assert is_dataclass(self.model_class)
			dtcol: FieldMetadata = modfields[field].metadata
			column_format = dtcol.get('column_format', XlsxFormat())
			if dtcol.get('freeze'):
				frzpane_y = ix

			# Write table header
			# header_fmt = self.header_format.dump()
			ws.write_column(
				0, ix,
				data=[dtcol['header']],
				cell_format=workbook.add_format(self.header_format.dump(exclude_unset=True))
			)

			# Write table body
			body_fmt = self.base_format.merge(column_format, to_dict=True)
			ws.write_column(
				1, ix,
				data=data[field],
				cell_format=workbook.add_format(body_fmt)
			)
			# Write table footer
			if has_footer:
				footer_fmt = self.footer_format.merge(column_format, to_dict=True)
				ws.write_column(
					self.data_rows + 1, ix,
					data=footer[field],
					cell_format=workbook.add_format(footer_fmt)
				)

			if dtcol.get('width'):
				width = dtcol['width']
			else:
				try:
					# reduce function can trigger error if data is empty
					autowidth = reduce(max, map(cell_autofit_width, self.data[field].astype('string').fillna('')))	# Value as pixel number
					headerlen = cell_autofit_width(dtcol['header'])
					width = (headerlen if headerlen>autowidth else autowidth)//6
				except Exception:
					width = None

			ws.set_column(ix, ix, width=width)

		# Set worksheet general parameter
		if self.enable_autofilter:
			ws.autofilter(0, 0, 0, ncol-1)

		ws.set_paper(9)	# 9 = A4
		ws.set_landscape()
		ws.set_margins(0.25)
		ws.center_horizontally()
		ws.print_area(0, 0, self.data_rows, ncol-1)
		# ws.autofit()
		ws.freeze_panes(1, frzpane_y)
		ws.ignore_errors({'number_stored_as_text': f'A:{xl_col_to_name(ncol-1)}'})

	@cached_property
	def data_rows(self) -> int:
		return self.data.shape[0]

	@cached_property
	def fields_order(self) -> List[str]:
		return self.data.columns.to_list()

	@cached_property
	def xlcolumn(self) -> Dict[str, str]:
		return {col: xl_col_to_name(self.get_column_position(col)) for col in self.fields_order}

	@cached_property
	def xlcolumn_range(self) -> Dict[str, str]:
		xlrange = dict()
		for f in self.fields_order:
			icol = self.get_column_position(f)
			xlrange[f] = xl_range_abs(1, icol, self.data_rows, icol)

		return xlrange


class SheetSOE(SheetWrapper, model_class=SOEModel):
	pass


@dataclass
class ModelInfo(DataModel):
	properties: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Properties',
			column_format=XLS_FORMAT_TEXT_WRAP,
		)
	)
	value: str = field(
		default=None,
		metadata=FieldMetadata(
			header='Value',
			column_format=XLS_FORMAT_TEXT_WRAP,
			width=100
		)
	)


class SheetInfo(SheetWrapper, model_class=ModelInfo):

	def __init__(
		self,
		name: str = 'Info',
		info_data: List[Tuple[str, Any]] = list(),
		kwargs: InfoBase = InfoBase()
	):
		"""General information sheet contains data summary.

		Parameters:
			source : Data source, can be filenames or offline database data
			output : Original output filename
			date_range : 
			processed_date : 
			execution_time : 
			node : 
			user : 
		"""
		date_range = kwargs.get('date_range', tuple())
		if all(isinstance(val, datetime.datetime) for val in date_range):
			date_range_str = f'{date_range[0].strftime("%d-%m-%Y")} s/d {date_range[1].strftime("%d-%m-%Y")}'
		else:
			date_range_str = '<unknown date_min> s/d <unknown date_max>'

		base_info = [
			('Source', kwargs.get('source')),
			('Output', kwargs.get('output')),
			('Date Range', date_range_str),
			('Processed Date', datetime.datetime.now().strftime('%d-%m-%Y')),
			('Execution Time', f'{kwargs.get("execution_time")}s'),
			('Server', kwargs.get('node')),
			('User', kwargs.get('user')),
		]
		data = pd.DataFrame(data=[*base_info, self.blank_row(), *info_data], columns=['properties', 'value'])
		super().__init__(name=name, data=data, row_offset=1, autofilter=False, **kwargs)

	@staticmethod
	def sub_title(title: str) -> Tuple[str, str]:
		return (f'## {title}', '')

	@staticmethod
	def blank_row() -> Tuple[str, str]:
		return ('', '')


def xl_hyperlink_to_range(ref: SheetWrapper, ix0: int, ix1: int, text: str = 'GO >>>') -> str:
	col1 = ref.data.shape[1] - 1
	try:
		row0 = ref.data.index.get_loc(ix0) + 1
		row1 = ref.data.index.get_loc(ix1) + 1
		range_ = xl_range_abs(row0, 0, row1, col1)
		return f'=HYPERLINK("#{ref.sheet_name}!{range_}", "{text}")'
	except Exception as exc:
		range_ = xl_range_abs(ix0, 0, ix1, col1)
		logprint('Error on generating hyperlink', level='warning')
		return f'=HYPERLINK("#ERROR!{range_}", "ERROR!!")'


class FileWriter:

	def __init__(
		self,
		dir: Optional[str] = None,
		filename_prefix: Optional[str] = '',
		sheets: Dict[str, SheetWrapper] = dict(),
		properties: InfoBase = {},
		**kwargs
	):
		self._sheets = sheets
		self.prefix = filename_prefix
		self.file_dir = dir or BASE_DIR / 'output'
		self.file_properties = properties

	def _writer(self, output_filename: str, *args, **kwargs) -> Union[BytesIO, str]:
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
			wb.set_properties(self.file_properties)

			for name, wrapper in self._sheets.items():
				wrapper.add_sheet_to_workbook(wb, **kwargs)

		return target

	def add_sheet(self, wrapper: SheetWrapper, name: str = None):
		if name is None:
			self._sheets[wrapper.sheet_name] = wrapper
		else:
			self._sheets[name] = wrapper

	def to_excel(self, filename: str, as_iobuffer: bool = False, **kwargs) -> Union[BytesIO, str]:
		"""Write data into excel file.
		
		Parameters:
			as_iobuffer : create as io buffer for file stream

		Result:
			IOBuffer or Workbook object
		"""
		filename = re.sub(r'\.(xlsx|xls)$', '', filename, flags=re.IGNORECASE)
		if as_iobuffer:
			filepath = f'{filename}.{self.file_extension}'
		else:	
			# Check target directory of output file
			if not os.path.isdir(self.file_dir):
				os.mkdir(self.file_dir)

			file_list = glob(f'{self.file_dir}/{filename}*.{self.file_extension}')
			if len(file_list)>0:
				filename += f'_rev{len(file_list)}'

			filepath = f'{self.file_dir}/{filename}.{self.file_extension}'

		# Create excel file
		output = self._writer(filepath, as_iobuffer=as_iobuffer, **kwargs)
		if as_iobuffer:
			return output.getvalue()
		else:
			logprint(f'Data berhasil di-export pada "{output}".', level='info')
			return output

	@property
	def sheets(self) -> Dict[str, SheetWrapper]:
		return self._sheets

	@cached_property
	def file_extension(self) -> str:
		return 'xlsx'

