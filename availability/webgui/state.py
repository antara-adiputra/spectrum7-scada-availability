import asyncio, datetime, time
import threading
from dataclasses import InitVar, asdict, dataclass, field, fields, is_dataclass
from io import BytesIO

from nicegui.binding import bindable_dataclass

from .types import *
from ..core.base import CalculationState, ProgressData, State
from ..lib import rgetattr, rsetattr


StateVars: TypeAlias = List[str]
StateVarMapping: TypeAlias = Union[StateVars, Mapping[str, Optional[Callable]]]


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
		self.enable_change_master = bool(name=='soe')
		self.enable_upload_file = bool(name in ('soe', 'rcd', 'rtu'))
		self.enable_check_server = bool(name=='ofdb')
		self.set_uploaded(False)

	def set_uploaded(self, value: Optional[bool]):
		self.enable_reset = value
		self.enable_upload_file = not value
		self.enable_view_file_list = value
		if not value:
			self.set_loaded(False)

	def set_loading(self, value: bool):
		self.enable_change_input = not value
		self.enable_change_master = not value
		self.enable_reset = not value

	def set_loaded(self, value: bool):
		self.enable_calculate = value
		self.enable_change_input = not value
		if value:
			# Disable change master only if data loaded
			self.enable_change_master = False
		else:
			# Disable calculate if data not loaded
			self.set_calculated(False)

	def set_calculated(self, value: bool):
		self.enable_download = value


class ProxyState:
	__proxy_target__: object = None

	def __setattr_mirror__(self, name: str, value):
		if hasattr(self.__proxy_target__, name):
			setattr(self.__proxy_target__, name, value)

	def __setattr__(self, name: str, value):
		super().__setattr__(name, value)
		if self.mirrored:
			self.__setattr_mirror__(name, value)

	def mirror_to(self, obj: object):
		if obj is None:
			return

		self.__proxy_target__ = obj

	@property
	def mirrored(self) -> bool:
		return not self.__proxy_target__ is None


TB1 = TypeVar('TB1', bound=BaseState)

def create_bindable(datacls: Union[TB1, Type[TB1]]) -> Union[TB1, Type[TB1]]:
	if not is_dataclass(datacls):
		return

	cls = bindable_dataclass(type('ProxyOf' + datacls.__class__.__name__, (ProxyState, datacls.__class__, State), {}))
	if isinstance(datacls, type):
		return cls
	else:
		obj = cls()
		obj.set(**asdict(datacls))
		obj.mirror_to(datacls)
		return obj


class ObjectStateTracker:
	_tracking: bool
	_task: asyncio.Task = None

	def _track(self, object, mapping: StateVarMapping, interval: float = 0.01):
		logprint(f'Start tracking state of object {object.__class__.__name__} id={id(object)}.')
		if isinstance(mapping, list):
			if all(map(lambda x: isinstance(x, tuple), mapping)):
				attrs = dict(mapping)
			else:
				attrs = {attr: None for attr in mapping}
		else:
			attrs = mapping

		while self._tracking:
			for key, func in attrs.items():
				value = rgetattr(object, key)
				if callable(func):
					value = func(value)

				rsetattr(self, key, value)

			time.sleep(interval)

		logprint(f'Tracking state of object {object.__class__.__name__} id={id(object)} has been stopped.')

	def start_tracking(self, obj, mapping: StateVarMapping):
		self._tracking = True
		loop = asyncio.get_event_loop()
		self._task = loop.create_task(asyncio.to_thread(self._track, obj, mapping))

	def stop_tracking(self):
		self._tracking = False


@bindable_dataclass
class BindableProgress(ObjectStateTracker, ProgressData):
	pass


@bindable_dataclass
class BindableCoreState(ObjectStateTracker, CalculationState):
	progress: BindableProgress = field(init=False, default_factory=BindableProgress)

	def start_tracking(self, obj):
		mapping = [
			'progress.value',
			'progress.message',
			'progress.percentage',
			'loading_file',
			'loaded',
			'analyzing',
			'analyzed',
			'calculating',
			'calculated',
			'exporting',
			'exported',
			'last_exported_file',
		]
		return super().start_tracking(obj, mapping)

