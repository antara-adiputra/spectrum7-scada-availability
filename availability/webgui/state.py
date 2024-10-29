import datetime
from io import BytesIO
from typing import Any, Dict, List, Text, Callable, Optional, Tuple, Union


class State:
	"""Abstract class of State"""

	def __init__(self) -> None:
		pass

	def _attr_from_dict(self, **kwargs) -> None:
		for key, val in kwargs.items():
			if hasattr(self, key): setattr(self, key, val)

	def reset(self) -> None:
		pass

	def update(self, **kwargs) -> None:
		self._attr_from_dict(**kwargs)


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