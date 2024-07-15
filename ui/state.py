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
	input_type: Optional[str]
	calculation_handler: Any

	def __init__(self) -> None:
		super().__init__()
		self.reset()

	def reset(self) -> None:
		self.title = ''
		self.subtitle = ''
		self.description = ''
		self.input_type = None
		self.calculation_handler = None


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