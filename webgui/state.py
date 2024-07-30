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


class FileProcessorState(State):
	_steps: List[str] = ['upload', 'validate', 'analyze', 'calculate', 'export']
	file_uploaded: bool
	file_isvalid: bool
	is_loading_file: bool
	loaded: bool
	is_analyzing: bool
	analyzed: bool
	is_calculating: bool
	calculated: bool
	is_exporting: bool
	files: Dict[str, BytesIO]
	filenames: List[str]
	filesizes: List[int]
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
		self.file_uploaded = False
		self.file_isvalid = False
		self.is_loading_file = False
		self.loaded = False
		self.is_analyzing = False
		self.analyzed = False
		self.is_calculating = False
		self.calculated = False
		self.is_exporting = False
		self.files = dict()
		self.filenames = list()
		self.filesizes = list()

	def pre_upload(self) -> None:
		self.reset()

	def pre_validate(self) -> None:
		self.file_isvalid = False
		self.is_loading_file = False
		self.loaded = False
		self._set_state_next('validate')

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
		pass

	@property
	def filecount(self):
		return len(self.files)
	

class OfdbProcessorState(State):
	_steps: List[str] = ['communication', 'fetch', 'analyze', 'calculate', 'export']
	initialized: bool
	connecting_to_server: bool
	server_available: bool
	is_fetching_data: bool
	fetched: bool
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
		self.initialized = False
		self.connecting_to_server = False
		self.server_available = False
		self.is_fetching_data = False
		self.fetched = False
		self.is_analyzing = False
		self.analyzed = False
		self.is_calculating = False
		self.calculated = False
		self.is_exporting = False

	def pre_communication(self) -> None:
		self.connecting_to_server = False
		self.server_available = False
		self._set_state_next('communication')

	def pre_fetch(self) -> None:
		self.is_fetching_data = False
		self.fetched = False
		self._set_state_next('fetch')

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
		pass