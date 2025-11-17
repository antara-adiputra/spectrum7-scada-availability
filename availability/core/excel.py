from dataclasses import dataclass, field

from xlsxwriter.utility import xl_range_abs, xl_rowcol_to_cell, xl_col_to_name

from ..types import *

# |Index|Format String	|
# |-----+---------------+
# |	0	| General
# |	1	| 0	
# |	2	| 0.00
# |	3	| #,##0
# |	4	| #,##0.00
# |	5	| ($#,##0_);($#,##0)
# |	6	| ($#,##0_);[Red]($#,##0)
# |	7	| ($#,##0.00_);($#,##0.00)
# |	8	| ($#,##0.00_);[Red]($#,##0.00)
# |	9	| 0%
# |	10	| 0.00%
# |	11	| 0.00E+00
# |	12	| # ?/?
# |	13	| # ??/??
# |	14	| m/d/yy
# |	15	| d-mmm-yy
# |	16	| d-mmm
# |	17	| mmm-yy
# |	18	| h:mm AM/PM
# |	19	| h:mm:ss AM/PM
# |	20	| h:mm
# |	21	| h:mm:ss
# |	22	| m/d/yy h:mm
# |	…	| …
# |	37	| (#,##0_);(#,##0)
# |	38	| (#,##0_);[Red](#,##0)
# |	39	| (#,##0.00_);(#,##0.00)
# |	40	| (#,##0.00_);[Red](#,##0.00)
# |	41	| _(* #,##0_);_(* (#,##0);_(* "-"_);_(@_)
# |	42	| _($* #,##0_);_($* (#,##0);_($* "-"_);_(@_)
# |	43	| _(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)
# |	44	| _($* #,##0.00_);_($* (#,##0.00);_($* "-"??_);_(@_)
# |	45	| mm:ss
# |	46	| [h]:mm:ss
# |	47	| mm:ss.0
# |	48	| ##0.0E+0
# |	49	| @
# ------+--------------------------
# Numeric formats 23 to 36 are not documented by Microsoft and may differ in international versions. The listed date and currency formats may also vary depending on system settings.


FontColorName: TypeAlias = Literal['black', 'blue', 'brown', 'cyan', 'grey', 'green', 'lime', 'magenta', 'navy', 'orange', 'pink', 'purple', 'red', 'silver', 'white', 'yellow']
# Color code with #RRGGBB format, eg: #0000FF for blue
FontColorCode: TypeAlias = str
FontScript: TypeAlias = Literal[1, 2]
HAlign: TypeAlias = Literal['left', 'center', 'right', 'fill', 'justify', 'center_across', 'distributed']
VAlign: TypeAlias = Literal['top', 'vcenter', 'bottom', 'vjustify', 'vdistributed']


def cell_abs(row: Any, col: Any):
	"""Xls absolute cell row and column."""
	return xl_rowcol_to_cell(row=row, col=col, row_abs=True, col_abs=True)

def cell_cabs(row: Any, col: Any):
	"""Xls absolute cell column."""
	return xl_rowcol_to_cell(row=row, col=col, col_abs=True)

def cell_rabs(row: Any, col: Any):
	"""Xls absolute cell row."""
	return xl_rowcol_to_cell(row=row, col=col, row_abs=True)


@dataclass(kw_only=True)
class XlsxFormat:
	"""XlsxWriter formatting dataclass. Docs: https://xlsxwriter.readthedocs.io/format.html
	
	Params:
		- font_name ::Set the font used in the cell
		- font_size ::Set the size of the font used in the cell
		- font_color ::Set the color of the font used in the cell.
		
		"""
	__fields_set__: set[str] = field(init=False, default_factory=set)
	font_name: str = None
	font_size: int = None
	font_color: Union[FontColorName, FontColorCode] = None
	bold: bool = None
	italic: bool = None
	underline: bool = None
	font_strikeout: bool = None
	font_script: FontScript = None
	num_format: Union[int, str] = None
	locked: bool = None
	hidden: bool = None
	align: HAlign = None
	valign: VAlign = None
	# Rotation angle in the range -90 to 90 and 270
	rotation: int = None
	# Wrap text in a cell
	text_wrap: bool = None
	# 1: from left to right, 2: from right to left
	reading_order: Literal[1, 2] = None
	# Turn on the justify last text property
	text_justlast: bool = None
	# Center text across adjacent cells
	center_across: bool = None
	# Set the cell text indentation level
	indent: int = None
	# Turn on the text “shrink to fit” for a cell
	shrink: bool = None
	# Set the background pattern of a cell. index = 1-18
	pattern: int = None
	# Set the color of the background pattern in a cell
	bg_color: Union[FontColorName, FontColorCode] = None
	# Set the color of the foreground pattern in a cell
	fg_color: Union[FontColorName, FontColorCode] = None
	# Set the cell border style. Docs https://xlsxwriter.readthedocs.io/format.html#format-set-border
	border: int = None
	bottom: int = None
	top: int = None
	left: int = None
	right: int = None
	border_color: Union[FontColorName, FontColorCode] = None
	bottom_color: Union[FontColorName, FontColorCode] = None
	top_color: Union[FontColorName, FontColorCode] = None
	left_color: Union[FontColorName, FontColorCode] = None
	right_color: Union[FontColorName, FontColorCode] = None
	# Turn on quote prefix for the format
	quote_prefix: bool = None
	# Turn on the checkbox property for the format
	checkbox: bool = None

	def __setattr__(self, name: str, value: Any):
		super().__setattr__(name, value)
		if value is None:
			self.__fields_set__.discard(name)
		elif name not in ('__fields_set__'):
			self.__fields_set__.add(name)

	@property
	def __dict__(self) -> Dict[str, Union[bool, int, str]]:
		return {attr: getattr(self, attr) for attr in self.__dataclass_fields__.keys() if attr not in ('__fields_set__')}

	def dump(self, exclude_unset: bool = False) -> Dict[str, Union[bool, int, str]]:
		if exclude_unset:
			return {key: getattr(self, key) for key in self.__fields_set__}
		else:
			return self.__dict__

	def merge(self, source: Self, to_dict: bool = False) -> Union[Self, Dict[str, Union[bool, int, str]]]:
		data = {**self.dump(exclude_unset=True), **source.dump(exclude_unset=True)}
		if to_dict:
			return data
		else:
			return XlsxFormat(**data)


# Declare common excel format here
XLS_FORMAT_TEXT = XlsxFormat(num_format='@')
XLS_FORMAT_TEXT_CENTER = XlsxFormat(num_format='@', align='center', text_wrap=True)
XLS_FORMAT_TEXT_WRAP = XlsxFormat(num_format='@', text_wrap=True)
XLS_FORMAT_INTEGER = XlsxFormat(num_format='0', align='center', text_wrap=True)
XLS_FORMAT_DECIMAL3 = XlsxFormat(num_format='0.000', align='center', text_wrap=True)
XLS_FORMAT_DATETIME = XlsxFormat(num_format='dd-mm-yyyy hh:mm:ss.000', align='center', text_wrap=True)
XLS_FORMAT_TIMEDELTA = XlsxFormat(num_format='[hh]:mm:ss', align='center', text_wrap=True)
XLS_FORMAT_PERCENTAGE = XlsxFormat(num_format='0.00%', align='center', text_wrap=True)


class XlsxFormula:

	@staticmethod
	def _wrap(formula: str, /, *args, eq: bool = False) -> str:
		xls_formula = f'{formula}({",".join(tuple(map(str, args)))})'
		if eq:
			return '=' + xls_formula
		else:
			return xls_formula

	def if_(self, statement: str, true_val: Any, false_val: Any = None, /, eq: bool = False) -> str:
		if false_val is None:
			return self._wrap('IF', statement, true_val, eq=eq)
		else:
			return self._wrap('IF', statement, true_val, false_val, eq=eq)

	def iferror(self, value: str, error_value: Any, eq: bool = False) -> str:
		return self._wrap('IFERROR', value, error_value, eq=eq)

	def isnumber(self, value: str, eq: bool = False) -> str:
		return self._wrap('ISNUMBER', value, eq=eq)

	def range_criteria(self, xlrange: str, value: Any) -> str:
		"""Create excel syntax of criteria_range-value pair."""
		return f'{xlrange},{value}'

	def average(self, xlrange: str, /, eq: bool = False) -> str:
		"""Create excel SUM formula for single column."""
		return self._wrap('AVERAGE', xlrange, eq=eq)

	def averageifs(self, xlrange_value: str, /, *criterias, default: Any = 0, eq: bool = False) -> str:
		"""Create excel COUNTIFS formula."""
		averageifs = self._wrap('AVERAGEIFS', xlrange_value, *criterias)
		return self.iferror(averageifs, default, eq=eq)

	def count(self, xlrange: str, /, eq: bool = False) -> str:
		"""Create excel COUNT formula for single column."""
		return self._wrap('COUNT', xlrange, eq=eq)

	def counta(self, xlrange: str, /, eq: bool = False) -> str:
		"""Create excel COUNTA formula for single column."""
		return self._wrap('COUNTA', xlrange, eq=eq)

	def countif(self, xlrange: str, value: Any, /, eq: bool = False) -> str:
		"""Create excel COUNTIF formula for single column."""
		return self._wrap('COUNTIF', self.range_criteria(xlrange, value), eq=eq)

	def countifs(self, /, *criterias, eq: bool = False) -> str:
		"""Create excel COUNTIFS formula."""
		return self._wrap('COUNTIFS', *criterias, eq=eq)

	def countif_or(self, xlrange: str, /, *values: Any, eq: bool = False) -> str:
		multi_val = '{' + ','.join(map(lambda v: f'"{v}"', values)) + '}'
		return self.sum(self.countif(xlrange, multi_val), eq=eq)

	def sumifs(self, xlrange_value: str, /, *criteria, eq: bool = False) -> str:
		"""Create excel SUMIFS formula."""
		return self._wrap('SUMIFS', xlrange_value, *criteria, eq=eq)

	def div(self, cell_value: str, cell_divider: str, /, default: Any = 0, eq: bool = False) -> str:
		"""Create excel division formula with default error value.
		
		Parameters:
			* cell_value : sdsd"""
		div = f'{cell_value}/{cell_divider}'
		if default is None:
			return self._wrap('', div, eq=eq)
		else:
			return self.iferror(div, default, eq=eq)

	def sum(self, /, *values, eq: bool = False) -> str:
		"""Create excel basic SUM formula."""
		return self._wrap('SUM', *values, eq=eq)


def column_formula(strformat: str, rows_count: int, /, offset: int = 1, **fmtkwargs) -> List[str]:
	range_ = range(rows_count)
	return list(map(lambda i: strformat.format(**fmtkwargs), range_))