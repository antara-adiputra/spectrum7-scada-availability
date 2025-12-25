import asyncio, calendar, datetime, functools, os, re, time, warnings
import inspect
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from difflib import SequenceMatcher, get_close_matches
from functools import partial
from glob import glob
from io import BytesIO
from itertools import islice
from types import MappingProxyType

import pandas as pd
from lxml import etree as et

from .types import *
from . import settings


LogLevel: TypeAlias = Literal['debug', 'info', 'warning', 'error']

_WIDTH = os.get_terminal_size()[0]
MAX_WIDTH = 160
CONSOLE_WIDTH = _WIDTH - 2 if _WIDTH<MAX_WIDTH else MAX_WIDTH
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


class ProcessError(Exception):

	def __init__(self, name: str = '', msg: str = '', *param) -> None:
		self.name = name
		self.msg = msg
		self.param = param

	def __str__(self) -> str:
		return f'{self.name}: {self.msg} {" ".join(self.param)}'

def logprint(message: str, /, level: LogLevel = 'debug', timestamp: bool = True, cli: bool = True, ext_func: Optional[Callable] = None, **ext_kwargs):
	msgs = list()
	if timestamp:
		msgs.append(str(datetime.datetime.now())[:-3])

	msgs.append(level.upper())
	msgs.append(message)
	if cli:
		if level=='debug':
			if getattr(settings, 'DEBUG', False):
				print(*msgs, sep='  ')
		else:
			print(*msgs, sep='  ')

	if callable(ext_func):
		ext_func('  '.join(msgs), **ext_kwargs)

	return '  '.join(msgs)

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
		# print(f' ({round(tick-tock, 2)}s)')

		return returned_value, tick-tock
	return inner1

# See https://stackoverflow.com/questions/31174295/getattr-and-setattr-on-nested-objects
def rsetattr(obj, attr: str, value: Any, sep: str = '.'):
	pre, _, post = attr.rpartition(sep)
	return setattr(rgetattr(obj, pre) if pre else obj, post, value)

# using wonder's beautiful simplification: https://stackoverflow.com/questions/31174295/getattr-and-setattr-on-nested-objects/31174427?noredirect=1#comment86638618_31174427
def rgetattr(obj, attr: str, *args, sep: str = '.'):
	def _getattr(obj, attr):
		return getattr(obj, attr, *args)
	return functools.reduce(_getattr, [obj] + attr.split(sep))

def add_months(date: Union[datetime.date, datetime.datetime], n: int) -> Union[datetime.date, datetime.datetime]:
	def calc():
		mm = date.month - 1 + n
		year = date.year + mm // 12
		month = mm % 12 + 1
		day = min(date.day, calendar.monthrange(year, month)[1])
		return year, month, day

	if isinstance(date, datetime.datetime):
		return datetime.datetime(*calc(), date.hour, date.minute, date.second, date.microsecond)
	elif isinstance(date, datetime.date):
		return datetime.date(*calc())
	else:
		raise ValueError('Invalid input type of ' + type(date))

def instance_factory(cls, *initargs, **initkwargs) -> object:
	return cls(*initargs, **initkwargs)

def consume(iterator: Iterable, n: int | None = None) -> None:
	"Advance the iterator n-steps ahead. If n is None, consume entirely."
	# Use functions that consume iterators at C speed.
	# For more information, https://docs.python.org/3/library/itertools.html#itertools-recipes
	if n is None:
		deque(iterator, maxlen=0)
	else:
		next(islice(iterator, n, n), None)

def get_ifs_name(list1: list, list2: list) -> dict:
	"""Get IFS name pair based on name similarity in list1 & list2."""
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

def get_table(ws_element: et._Element, namespace: dict):
	"""Extract table data and columns name from xml file."""
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

def get_rtu_timestamp(s: pd.Series) -> datetime.datetime:
	"""Combine RTU timestamp and milliseconds from given series."""
	return pd.to_datetime(s['timestamp']) + pd.to_timedelta(s['ms'], unit='ms')

def get_system_timestamp(s: pd.Series) -> datetime.datetime:
	"""Combine system timestamp and milliseconds from given series."""
	return pd.to_datetime(s['system_timestamp']) + pd.to_timedelta(s['system_ms'], unit='ms')

def get_datetime(s: pd.Series) -> Tuple[datetime.datetime, datetime.datetime]:
	"""Return set of RTU timestamp and system timestamp."""
	return get_rtu_timestamp(s), get_system_timestamp(s)

def get_execution_duration(s0: pd.Series, s1: pd.Series) -> float:
	"""Calculate timedelta of RTU timestamp between 2 events."""
	delta_time = get_rtu_timestamp(s1) - get_rtu_timestamp(s0)
	return round(delta_time.total_seconds(), 3)

def get_termination_duration(s0: pd.Series, s1: pd.Series):
	"""Calculate timedelta of system timestamp between 2 events."""
	# delta_time = join_datetime(*s1.loc[['system_timestamp', 'system_ms']].to_list()) - join_datetime(*s0.loc[['system_timestamp', 'system_ms']].to_list())
	delta_time = get_system_timestamp(s1) - get_system_timestamp(s0)
	return round(delta_time.total_seconds(), 3)

def immutable_dict(input: dict) -> MappingProxyType:
	"""Create non-editable dict."""
	new = dict()
	for key, item in input.items():
		if type(item)==dict:
			new[key] = immutable_dict(item)
		else:
			new[key] = item
	return MappingProxyType(new)

def tryparse(type_: Type, value: Any, if_error: Any = None, **kwparse) -> Any:
	"""Try parsing an input as instance of type then return the result or default value if error occured."""

	try:
		output = type_(value, **kwparse)
	except Exception:
		output = if_error
	finally:
		return output
	
def try_strftime(value: Any, format: str, if_error: Any = None, **kwparse) -> Any:
	"""Try serialize datetime value or return default value if occured."""
	try:
		if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
			output = value.strftime(format)
		else:
			output = value
	except Exception:
		output = if_error
	finally:
		return output

def try_remove(list_: List[str], key: str, /):
	try:
		list_.remove(key)
	except ValueError:
		pass

def join_datetime(dt: pd.Series, ms: pd.Series) -> datetime.datetime:
	"""Combine two series of datetime timestamp and milliseconds."""
	return pd.to_datetime(dt) + pd.to_timedelta(ms, unit='ms')

def load_cpoint(path: str):
	"""
	"""
	filenames: List[str] = list()
	valid_df: List[pd.DataFrame] = list()
	for f in path.split(','):
		if '*' in f:
			g = glob(f.strip())
			if len(g)>0:
				filenames += g
			else:
				logprint(f'File like "{f}" is not found', level='warning')
		elif f.strip():
			filenames.append(f.strip())

	# Load point description
	for file in filenames:
		try:
			# Open first sheet
			result = pd.read_excel(file, sheet_name=0).fillna('')
			if isinstance(result, pd.DataFrame):
				valid_df.append(validate_cpoint(result))

		except FileNotFoundError:
			logprint(f'File "{file}" not found', level='warning')
		except Exception:
			logprint(f'Failed to open file "{file}"', level='warning')

	if len(valid_df)==0:
		logprint('No valid point description file was found')
	else:
		df = pd.concat(valid_df)
		df.columns = list(map(to_snake_case, df.columns))
		# Keep last will make any file with
		return df.drop_duplicates(subset=['b1', 'b2', 'b3'], keep='last')\
			.sort_values(['b1', 'b2', 'b3'])\
			.reset_index(drop=True)

def load_workbook(file: Union[str, BytesIO]) -> dict[str, pd.DataFrame]:
	"""Load whole excel file as dict of worksheets.

	Args:
		file : path of file

	Result:
		Dict of dataframe with sheetname as keys
	"""
	wb = {}

	try:
		wb = pd.read_excel(file, sheet_name=None, engine='openpyxl')
	except FileNotFoundError:
		raise FileNotFoundError
	except Exception:
		raise ImportError
	return wb

def nested_dict(obj: Union[dict, Any], keys: Union[list, tuple], default: Any = None, raise_error: bool = False):
	"""Get nested dictionary value."""
	if isinstance(obj, dict) and len(keys)>0:
		try:
			_keys = list(keys).copy()
			_key = _keys.pop(0)
			_obj = obj[_key]
		except Exception as e:
			if raise_error:
				raise e
			else:
				return default
		return nested_dict(_obj, _keys, default)
	else:
		return obj

def progress_bar(value: float, width: int = 0, style: str = 'full-block', *args, **kwargs):
	"""Print progress bar on console."""
	symbol = {'full-block': '█', 'left-half-block': '▌', 'right-half-block': '▐'}
	if width==0: width = CONSOLE_WIDTH
	percentage = int(value*100)
	char_block = symbol.get(style, style)
	if value<1:
		char_length = int(value*(width-5))
		print(f'\r {str(char_block*char_length).ljust(width-5, "-")} {percentage}%', end='', flush=True)
	else:
		print(f'\r {"Selesai... 100%".ljust(width, " ")}', flush=True)

def read_xml(filepath:str, *args, **kwargs):
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

def toggle_attr(name: str, *value):
	val0 = value[0] if len(value)>0 else True
	val1 = value[1] if len(value)>1 else None
	def wrapper(func):
		@functools.wraps(func)
		def wrapped(self, *args, **kwargs):
			rsetattr(self, name, val0)
			result = func(self, *args, **kwargs)
			rsetattr(self, name, val1)
			return result

		@functools.wraps(func)
		async def async_wrapped(self, *args, **kwargs):
			rsetattr(self, name, val0)
			result = await func(self, *args, **kwargs)
			rsetattr(self, name, val1)
			return result

		return async_wrapped if inspect.iscoroutinefunction(func) else wrapped

	return wrapper

async def run_background(proc: ProcessPoolExecutor, fn: Callable, *fnargs, **fnkwargs):
	loop = asyncio.get_running_loop() or asyncio.get_event_loop()
	return await loop.run_in_executor(proc, partial(fn, *fnargs, **fnkwargs))

def similarity_ratio(str1: str, str2: str):
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

def timedelta_split(td: datetime.timedelta):
	"""
	"""

	dd = td.days
	sec = td.seconds
	hh = sec // 3600
	mm = (sec // 60) - (hh * 60)
	ss = sec % 60

	return dd, hh, mm, ss

def to_snake_case(text: str) -> str:
	if ' ' in text:
		# Normal text
		return text.replace(' ', '_').lower()
	else:
		# Assumed as CamelCase
		return re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()

def truncate(text:str, max_length:int, on:str='left', debug:bool=False):
	"""
	"""

	length = len(text)

	if length>max_length:
		n = length - max_length + 3
		if debug: print(f'length={length}, max-length={max_length}, n={n}, on={on}')
		if on=='right':
			res = f'{text[:length-n]}...'
		elif on=='center':
			mid = max_length // 2
			mod = max_length % 2
			res = f'{text[:mid]}...{text[max_length+n-mid-mod:]}'
		else:
			res = f'...{text[n:]}'
	else:
		res = text

	return res

def validate_cpoint(df: pd.DataFrame, verbose: bool = False):
	"""Compare IFS's B3 name with SOE's B1 name and get the closest similarity into dict.
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


if __name__ == '__main__':
	for var in (75, 80, 100):
		print('\r\n' + '#'*var)
		print(truncate('/media/shared-ntfs/1-scada-makassar/AVAILABILITY/2023/RCD/RCD_Output_2023_01-07.xlsx', var, 'left', True))
		print(truncate('/media/shared-ntfs/1-scada-makassar/AVAILABILITY/2023/RCD/RCD_Output_2023_01-07.xlsx', var, 'right', True))
		print(truncate('/media/shared-ntfs/1-scada-makassar/AVAILABILITY/2023/RCD/RCD_Output_2023_01-07.xlsx', var, 'center', True))