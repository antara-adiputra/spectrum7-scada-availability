import datetime
from dataclasses import Field, InitVar, dataclass, field, fields, is_dataclass
from functools import partial
from typing_extensions import Required

import pandas as pd

from .excel import XlsxFormat
from ..lib import immutable_dict, logprint
from ..types import *


T = TypeVar('T')
ModelInput: TypeAlias = Union[pd.Series, pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]]


def create_default_factory(fn, **kwargs) -> Callable:
	return partial(fn, **kwargs)

def frozen_dataclass_set(dc, **attrs):
	"""Set frozen dataclass attributes values"""
	if is_dataclass(dc):
		for key, val in attrs.items():
			if hasattr(dc, key):
				object.__setattr__(dc, key, val)
			else:
				# Invalid attribute to set, for now we only warn in console
				print(datetime.datetime.now(), 'Warning', f'Set invalid attr "{key}" for {dc.__name__} object')
	else:
		# Warn user for invalid usage
		print(datetime.datetime.now(), 'Error', f'frozen_dataclass_set only works with dataclass')

def repr_dataclass(obj) -> str:
	def print_attr(key: str, value: pd.DataFrame):
		if isinstance(value, pd.DataFrame):
			return f'{key}=DataFrame[{value.shape[0]} rows]'
		else:
			return f'{key}={repr(value)}'

	assert is_dataclass(obj)
	return f'{obj.__class__.__name__}({", ".join([print_attr(attr, getattr(obj, attr)) for attr in obj.__dataclass_fields__.keys()])})'

class FieldMetadata(TypedDict):
	header: Required[str]
	dtype: str
	required: bool
	freeze: bool
	column_format: XlsxFormat
	width: int


class Base:
	"""Dummy class"""


@dataclass(frozen=True)
class SerializableException:
	e: InitVar[Exception]
	type_: str = field(init=False, default='')
	message: str = field(init=False, default='')
	extra: Dict[str, Any] = field(default_factory=dict)

	def __post_init__(self, e: Exception):
		frozen_dataclass_set(
			self,
			type_=e.__class__.__name__,
			message='. '.join(e.args)
		)


@dataclass(frozen=True)
class ExceptionMessage(Exception):
	type_: str = 'error'
	message: str = ''
	data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProgressData:
	value: float = 0.0
	message: str = ''
	percentage: Optional[str] = field(init=False, default=None)

	def init(self, value: float = 0.0, message: str = ''):
		self.value = value
		self.message = message

	def _update_percentage(self):
		self.percentage = f'{self.value*100:.1f}%'

	def dump(self) -> Dict[str, Union[float, str, None]]:
		return {attr: getattr(self, attr) for attr in ('value', 'message', 'percentage')}

	def update(self, value: float, message: Optional[str] = None, **kwargs):
		self.value = value
		self._update_percentage()
		if not message is None:
			self.message = message


class State:
	"""Abstract class of State"""

	def set(self, **kwargs):
		for key, val in kwargs.items():
			if hasattr(self, key):
				setattr(self, key, val)
			else:
				raise KeyError(f'Attr {key} is not in {self.__class__.__name__} object')

	def reset(self):
		for f in fields(self):
			# value = f.default_factory() if callable(f.default_factory) else f.default
			if callable(f.default_factory):
				# Here we prevent new creation of new object variable,
				# because new object will not be tracked on bind anymore
				continue

			value = f.default
			setattr(self, f.name, value)


@dataclass
class Config(State):
	"""Base class for avaiability calculation configuration."""
	master: SCDMasterType = 'spectrum'


class BaseClass:
	__binded_state__: Optional[T] = None

	def __init__(self, bind_to: Optional[T] = None, **kwargs):
		self.__binded_state__ = bind_to

	def __setattr_binded__(self, name: str, value):
		if hasattr(self.__binded_state__, name):
			# print(self.__class__.__name__, '==>', self.__binded_state__.__class__.__name__, f'({name}, {value})')
			setattr(self.__binded_state__, name, value)

	def __setattr__(self, name: str, value):
		super().__setattr__(name, value)
		if self.binded:
			self.__setattr_binded__(name, value)

	def bind_to(self, obj: Any):
		self.__binded_state__ = obj

	def set_binded_attr(self, attr: str, value: Any):
		if self.binded:
			self.__setattr_binded__(attr, value)

	@property
	def binded(self) -> bool:
		return not self.__binded_state__ is None


class BaseWithProgress(BaseClass):

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self.progress = ProgressData()
		self.set_progress(value=0.0)

	def set_progress(self, value: float, message: Optional[str] = None, show_percentage: bool = False):
		data = dict(value=value)
		if show_percentage:
			if message is None:
				data['message'] = f'{value*100:.1f}%'
			else:
				data['message'] = message + f' ({value*100:.1f}%)'
		else:
			if not message is None:
				data['message'] = message

		self.progress.update(**data)
		prg = getattr(self.__binded_state__, 'progress', False)
		if self.binded and isinstance(prg, ProgressData):
			prg.update(**self.progress.dump())


def model_fields(obj: Union['DataModel', Type['DataModel']], as_dict: bool = False) -> Union[List[Field], Dict[str, Field]]:
	if as_dict:
		return {f.name: f for f in obj.__model_fields__}
	else:
		return obj.__model_fields__

def model_fieldnames(obj: Union['DataModel', Type['DataModel']]) -> List[str]:
	return [f.name for f in obj.__model_fields__]

def model_mappings(obj: Union['DataModel', Type['DataModel']], swap: bool = False, as_dict: bool = False) -> Mapping[str, str]:
	mapping = [(t[1], t[0]) for t in obj.__model_mappings__] if swap else obj.__model_mappings__
	if as_dict:
		return dict(mapping)
	else:
		return mapping

def model_dtypes(obj: Union['DataModel', Type['DataModel']]) -> MappingProxyType[str, str]:
	return obj.__model_dtypes__


class DataModel(Base):

	def __init_subclass__(cls):
		fields: List[Field] = list()
		headers: List[str] = list()
		mappings: List[Tuple[str, str]] = list()
		requireds: List[Tuple[str, str]] = list()
		dtypes: Dict[str, str] = dict()
		# Store metadata information from fields
		for base in cls.mro()[::-1]:
			if issubclass(base, (Base, DataModel)):
				base_fields = [attr for attr, anno in base.__annotations__.items() if not (anno in (ClassVar, InitVar) or attr.startswith('__'))]
				# print(cls.__name__, base.__name__, base_fields)
				for attr in base_fields:
					# NOTE: When dataclass class not initiated (usually by subclassing dataclass base), __dataclass_fields__ will be empty
					# Then we get Field from class __dict__
					# field: Field = base.__dataclass_fields__.get(attr, base.__dict__.get(attr))
					field = getattr(base, '__dataclass_fields__', base.__dict__).get(attr)
					assert isinstance(field, Field), f'Invalid field type of {type(field)}'
					metadata: FieldMetadata = field.metadata
					header = metadata['header']
					required = metadata.get('required')
					fields.append(field)
					headers.append(header)
					mappings.append((attr, header))
					if required:
						requireds.append((attr, header))

					if 'dtype' in metadata:
						dtypes[attr] = metadata['dtype']

		cls.__model_fields__ = fields
		cls.__model_headers__ = headers
		cls.__model_mappings__ = mappings
		cls.__model_required_fields__ = requireds
		cls.__model_dtypes__ = immutable_dict(dtypes)
		# logprint(f'Init subclass of {cls.__name__}', level='debug')

	@classmethod
	def _from_obj(cls, obj) -> Self:
		if isinstance(obj, pd.Series):
			columns = set(obj.axes[0])
		elif isinstance(obj, dict):
			columns = set(obj.keys())
		else:
			logprint(f'Invalid object type "{type(obj)}", Series or Dict is expected', level='error')
			raise ValueError(f'Invalid object type "{type(obj)}", Series or Dict is expected')

		field_mapping = cls.__model_required_fields__ if cls.__model_required_fields__ else cls.__model_mappings__
		fields_, headers = list(zip(*field_mapping))
		if set(headers).issubset(columns):
			return cls(**{attr: obj[header] for attr, header in cls.__model_mappings__ if header in columns})
		elif set(fields_).issubset(columns):
			return cls(**{attr: obj[attr] for attr, header in cls.__model_mappings__ if attr in columns})
		else:
			logprint(f'{type(obj)} with keys {tuple(columns)} does not match with {cls.__name__} model', level='error')
			raise ValueError(f'{type(obj)} with keys {tuple(columns)} does not match with {cls.__name__} model')

	@classmethod
	def from_dict(cls, data: Dict[str, Any]) -> Self:
		return cls._from_obj(data)

	@classmethod
	def from_series(cls, s: pd.Series) -> Self:
		return cls._from_obj(s)

	@classmethod
	def from_dataframe(cls, df: pd.DataFrame) -> 'DataTable[Self]':
		return DataTable(list(map(lambda ix: cls.from_series(df.loc[ix]), df.index)))

	@classmethod
	def validate_dataframe(cls, df: pd.DataFrame, debug: bool = False, **kwargs) -> pd.DataFrame:
		if not cls.validate_schema(df):
			raise ValueError(f'Dataframe with columns {tuple(df.columns)} does not match with {cls.__name__} model')

		model_mapping = model_mappings(cls, swap=True, as_dict=True)
		df.columns = [model_mapping.get(col, col) for col in df.columns]
		to_model_dtypes = model_dtypes(cls)
		return df.astype(to_model_dtypes)

	@classmethod
	def validate_schema(cls, pd_obj: Union[pd.Series, pd.DataFrame]) -> bool:
		dcolumns = pd_obj.axes[0] if isinstance(pd_obj, pd.Series) else pd_obj.columns
		field_mapping = cls.__model_required_fields__ if cls.__model_required_fields__ else cls.__model_mappings__
		fields_, headers = list(zip(*field_mapping))
		if set(fields_).issubset(set(dcolumns)):
			return True
		else:
			if set(headers).issubset(set(dcolumns)):
				return True
			else:
				return False

	@classmethod
	def validate(cls, obj: Any) -> Union[Self, 'DataTable[Self]']:
		if isinstance(obj, pd.DataFrame):
			return cls.from_dataframe(obj)
		elif isinstance(obj, pd.Series):
			return cls.from_series(obj)
		elif isinstance(obj, dict):
			return cls.from_dict(obj)
		elif isinstance(obj, list):
			return DataTable(list(map(lambda d: cls.from_dict(d), obj)))

		return None

	def dump(self, as_title: bool = False, **kwargs) -> Dict[str, Any]:
		dumped = dict()
		excluded = kwargs.get('exclude', list())
		for field in fields(self):
			if field.name.startswith('__'):
				continue

			key = field.metadata['header'] if as_title else field.name
			if key in excluded or field.name in excluded:
				continue

			value = getattr(self, field.name)
			if field.name.startswith('marked_') and field.type is bool:
				value = '*' if value else ''

			dumped[key] = value

		return dumped


T1 = TypeVar('T1', bound=DataModel)

@dataclass
class DataTable(Generic[T1]):

	def __init__(self, values: Optional[List[T1]] = None):
		if isinstance(values, list):
			if not all(map(lambda x: isinstance(x, DataModel), values)):
				raise ValueError(f'Items must be subclass of DataModel')
		else:
			values = list()

		self._model = type(values[0]) if values else None
		self._values = values

	def __repr__(self):
		return f'{self.__class__.__name__}[{self._model}...] ({self.count} items)'

	def __iter__(self):
		for value in self._values:
			yield value

	def __getitem__(self, item) -> T1:
		return self._values[item]

	def add(self, obj: T1):
		if self._model is None:
			self._model = type(obj)

		if self._model.__model_required_fields__==obj.__model_required_fields__:
			self._values.append(obj)
		else:
			raise ValueError(f'Cannot add different item type ({type(obj).__name__}). {self._model.__name__} is required')

	def merge(self, source: Self, inplace: bool = False) -> Optional[Self]:
		if self._model==source._model:
			if inplace:
				self._values.extend(source._values)
			else:
				return type(self)(self._values + source._values)
		else:
			if self._model is None:
				# Values is empty or model class unknown, merge left
				self._values.extend(source._values)
				self._model = source._model
			elif source._model is None:
				# Empty source table, do nothing
				pass
			else:
				raise ValueError(f'Cannot merging DataTable of model {self._model} with {source._model} type')

	def dump(self, as_title: bool = False, **kwargs) -> List[Dict[str, Any]]:
		return [obj.dump(as_title=as_title, **kwargs) for obj in self._values]

	def to_dataframe(self, /, include_fields: Optional[List[str]] = None, exclude_fields: Optional[List[str]] = None, use_header: bool = False, **kwargs) -> pd.DataFrame:
		fields, _ = list(zip(*self._model.__model_mappings__))
		if isinstance(include_fields, (list, set, tuple)):
			incl = set(include_fields)
			if incl.issubset(set(fields)):
				exclude = list(fields - incl)
			else:
				raise ValueError(f'{str(include_fields)} is not subset of {self._model.__name__} fields')
		elif isinstance(exclude_fields, list):
			exclude = exclude_fields
		else:
			exclude = list()

		return pd.DataFrame(data=[data.dump(as_title=use_header, exclude=exclude) for data in self._values])

	@property
	def count(self) -> int:
		return len(self._values)


@dataclass
class CalculationState(State):
	progress: ProgressData = field(init=False, default_factory=ProgressData)
	loading_file: bool = False
	loaded: Union[bool, None] = None
	analyzing: bool = False
	analyzed: Union[bool, None] = None
	calculating: bool = False
	calculated: Union[bool, None] = None
	exporting: bool = False
	exported: Union[bool, None] = None
	last_exported_file: str = None

