import datetime, functools
from dataclasses import InitVar, asdict, dataclass, field, fields
from io import BytesIO

from nicegui.binding import bindable_dataclass

from .types import *
from ..core.base import BaseClass, ProgressData, State
from ..lib import rsetattr


@bindable_dataclass
class BaseState(State):
	pass


@bindable_dataclass
class InterlockState(BaseState):
	enable_reset: bool = False
	enable_calculate: bool = False
	enable_change_input: bool = True
	enable_change_master: bool = False
	enable_check_server: bool = False
	enable_download: bool = False
	enable_upload_file: bool = True
	enable_view_file_list: bool = False
	result_visible: bool = False
	setup_visible: bool = True

	def set_input_source(self, name: str):
		self.enable_change_master = bool(name=='SOE')
		self.enable_upload_file = bool(name in ('SOE', 'RCD', 'RTU'))
		self.enable_check_server = bool(name=='OFDB')
		self.set_uploaded(False)

	def set_uploaded(self, value: Optional[bool]):
		self.enable_upload_file = not value
		if not value:
			self.set_loaded(False)

	def set_loading(self, value: bool):
		self.enable_change_input = not value
		self.enable_change_master = not value
		self.enable_reset = not value

	def set_loaded(self, value: bool):
		self.enable_reset = value
		self.enable_calculate = value
		self.enable_view_file_list = value
		self.enable_change_input = not value
		if value:
			# Disable change master only if data loaded
			self.enable_change_master = False
		else:
			# Disable calculate if data not loaded
			self.set_calculated(False)

	def set_calculated(self, value: bool):
		self.enable_download = value


@dataclass
class _Wrapper:
	# __wrapped__: ClassVar[BaseClass] = None
	_obj: InitVar[BaseClass]

	def __post_init__(self, _obj: BaseClass, **kwargs):
		self.wrap(_obj)

	def wrap(self, obj: BaseClass):
		if obj is None:
			return

		self.__wrapped__ = obj
		obj.bind_to(self)


@dataclass
class AvStateWrapper(_Wrapper, State):
	progress: ProgressData = field(init=False, default_factory=ProgressData)
	date_range: Tuple[Optional[datetime.datetime], Optional[datetime.datetime]] = (None, None)
	loading_file: bool = False
	loaded: Optional[bool] = None
	analyzing: bool = False
	analyzed: Optional[bool] = None
	calculating: bool = False
	calculated: Optional[bool] = None
	exporting: bool = False

	def reset(self):
		super().reset()
		self.progress.init()


def toggle_attr(name: str, *value):
	val0 = value[0] if len(value)>0 else True
	val1 = value[1] if len(value)>1 else None
	def wrapper(func):
		@functools.wraps(func)
		async def wrapped(self, *args, **kwargs):
			rsetattr(self, name, val0)
			result = await func(self, *args, **kwargs)
			rsetattr(self, name, val1)
			return result
		return wrapped
	return wrapper




class MenuState(State):
	title: str
	subtitle: str
	description: str
	component: Optional[str]
	comp_kwargs: Dict[str, Any]

	def __init__(self) -> None:
		super().__init__()
		self.reset()

	def reset(self) -> None:
		self.title = ''
		self.subtitle = ''
		self.description = ''
		self.component = None
		self.comp_kwargs = dict()


class CalculationState(State):
	_steps: List[str] = ['analyze', 'calculate', 'export']
	is_analyzing: bool
	analyzed: bool
	is_calculating: bool
	calculated: bool
	is_exporting: bool
	result: Any

	def __init__(self) -> None:
		super().__init__()
		self.result = None
		self.reset()

	def _set_state_next(self, state: str) -> None:
		_ipos = self._steps.index(state)
		if _ipos<len(self._steps)-1:
			_fn = getattr(self, 'pre_' + self._steps[_ipos+1])
			_fn()

	def reset(self) -> None:
		self.is_analyzing = False
		self.analyzed = False
		self.is_calculating = False
		self.calculated = False
		self.is_exporting = False
		super().reset()

	def pre_analyze(self) -> None:
		self.is_analyzing = False
		self.analyzed = False
		self._set_state_next('analyze')

	def pre_calculate(self) -> None:
		self.is_analyzing = False
		self.analyzed = False
		self.is_calculating = False
		self.calculated = False
		self._set_state_next('calculate')

	def pre_export(self) -> None:
		self._set_state_next('export')


class FileInputState(CalculationState):
	_steps: List[str] = ['upload', 'validate', 'analyze', 'calculate', 'export']
	file_uploaded: bool
	file_isvalid: bool
	is_loading_file: bool
	loaded: bool
	files: Dict[str, BytesIO]
	filenames: List[str]
	filesizes: List[int]

	def __init__(self) -> None:
		super().__init__()
		self.reset()

	def reset(self) -> None:
		super().reset()
		self.file_uploaded = False
		self.file_isvalid = False
		self.is_loading_file = False
		self.loaded = False
		self.files = dict()
		self.filenames = list()
		self.filesizes = list()
		super().reset()

	def pre_upload(self) -> None:
		self.reset()

	def pre_validate(self) -> None:
		self.file_isvalid = False
		self.is_loading_file = False
		self.loaded = False
		self._set_state_next('validate')

	@property
	def filecount(self):
		return len(self.files)
	

class OfdbInputState(CalculationState):
	_steps: List[str] = ['communication', 'fetch', 'analyze', 'calculate', 'export']
	initialized: bool
	connecting_to_server: bool
	server_available: bool
	is_fetching_data: bool
	fetched: bool
	timer: int
	date_from: datetime.datetime
	date_to: datetime.datetime

	def __init__(self) -> None:
		super().__init__()
		self.reset()

	def reset(self) -> None:
		self.initialized = False
		self.connecting_to_server = False
		self.server_available = False
		self.is_fetching_data = False
		self.fetched = False
		self.timer = 0
		self.date_from = None
		self.date_to = None
		super().reset()

	def pre_communication(self) -> None:
		self.connecting_to_server = False
		self.server_available = False
		self._set_state_next('communication')

	def pre_fetch(self) -> None:
		self.is_fetching_data = False
		self.fetched = False
		self.timer = 0
		self._set_state_next('fetch')

	def tick(self) -> None:
		self.timer += 1