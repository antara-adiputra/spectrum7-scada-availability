import asyncio, calendar, datetime, functools, inspect, os
from dataclasses import dataclass, field, fields, is_dataclass
from functools import partial
from io import BytesIO

import pandas as pd
from nicegui import events, ui
from nicegui.binding import bindable_dataclass
from nicegui.observables import ObservableDict, ObservableList
from starlette.formparsers import MultiPartParser

from .event import EventChainsWithArgs, EventChainsWithoutArgs, consume
from .state import BindableCoreState, BaseState, InterlockState, create_bindable
from .types import *
from .. import config
from ..core import rcd, rtu, soe, params
from ..globals import MONTH_OPTIONS
from ..lib import rgetattr, toggle_attr, try_strftime


FileCalcObjects: TypeAlias = Union[Any, Any]
OfdbCalcObjects: TypeAlias = Union[Any]
CalcObjects: TypeAlias = Union[FileCalcObjects, OfdbCalcObjects]

MultiPartParser.spool_max_size = 1024*1024*2


class MenuTitle(ui.label):
	"""
	"""

	def __init__(self, text: str = '') -> None:
		super().__init__(text)
		self.classes('text-2xl md:text-3xl font-extrabold w-full text-teal-700')


class MenuSubtitle(ui.label):
	"""
	"""

	def __init__(self, text: str = '') -> None:
		super().__init__(text)
		self.classes('text-base md:text-lg font-bold w-full text-teal-400')


class Button(ui.button):
	"""Extended nicegui ui.button.
	Add contexts into button element attributes.
	"""
	__used__: set

	def __init__(self,
		text: str = '',
		*,
		on_click: Union[Callable[..., Any], None] = None,
		color: Union[str, None] = 'primary',
		icon: Union[str, None] = None,
		**contexts
	) -> None:
		super().__init__(text, on_click=on_click, color=color, icon=icon)
		self.__used__ = set([attr for attr in dir(ui.button) if not (attr.startswith('_') and attr.endswith('_'))])
		# Add custom attribute
		for key in contexts:
			if not key.startswith('_') and key not in self.__used__: setattr(self, key, contexts[key])


class ObjectDebugger(ui.expansion):
	"""Component which used to display object attributes for debug purpose only."""
	__used__: set
	excluded: List[str]
	included: List[str]

	def __init__(
		self,
		text: str = '',
		object: object = None,
		target: Optional[str] = None,
		*,
		caption: Optional[str] = None,
		icon: Optional[str] = None,
		group: Optional[str] = None,
		value: bool = False,
		on_value_change: Optional[Callable[..., Any]] = None,
		**contexts
	):
		title = text if text else repr(object)
		super().__init__(text=title, caption=caption, icon=icon, group=group, value=value, on_value_change=on_value_change)
		self.__used__ = set([attr for attr in dir(ui.expansion) if not (attr.startswith('_') and attr.endswith('_'))])

		if not (object is None or target is None):
			self._callable_obj = functools.partial(rgetattr, object, target)
		else:
			self._callable_obj = None

		self.props(add='dense')
		self.classes('w-full')
		if 'included' not in contexts: self.included = list()
		if 'excluded' not in contexts: self.excluded = list()
		# Add custom attribute
		for key in contexts:
			if not key.startswith('_') and key not in self.__used__: setattr(self, key, contexts[key])

	def render(self) -> ui.expansion:
		def _render_table(dtclass):
			for f in fields(dtclass):
				ui.label(f.name).classes('border')
				ui.label('').classes('border').bind_text_from(dtclass, f.name, lambda x: repr(x) if callable(x) else str(x), strict=False)

		with self:
			if self._callable_obj is None:
				return self

			with ui.grid(columns='auto auto').classes('w-full gap-0'):
				obj = self._callable_obj()
				if is_dataclass(obj):
					# for f in fields(obj):
					# 	ui.label(f.name).classes('border')
					# 	ui.label('').classes('border').bind_text_from(obj, f.name, lambda x: repr(x) if callable(x) else str(x))
					_render_table(obj)
				# elif isinstance(obj, MirrorableBaseClass):
				# 	mirror = obj.create_mirror()
				# 	_render_table(mirror)


				# for attr in dir(self._callable_obj()):
				# 	if not attr.startswith('__') or (attr in self.included and attr not in self.excluded):
				# 		ui.label(attr).classes('border')
				# 		ui.label('').classes('border').bind_text_from(self._callable_obj(), attr, lambda x: repr(x) if callable(x) else str(x))
		return self

	def refresh(self) -> None:
		"""Refreshable content"""
		self.clear()
		self.render()


####################################################################################################
# NEW DEVELOPED CODE
####################################################################################################


ui_item = ui.item.default_style('padding: 2px 8px;')
ui_section = ui.item_section.default_classes('align-stretch')
ui_input = ui.input.default_props('dense outlined square stack-label')
ui_menu_label = ui.item_label.default_classes('text-md')
Select = type('Select', (ui.select,), {}).default_props('dense outlined square stack-label options-dense')
ButtonDialogControl = type('ButtonDialogControl', (ui.button,), {}).default_props('dense').default_classes('min-w-16')

def get_css_class(*style, **kwstyle) -> str:
	cls = list(style)
	if kwstyle:
		cls.extend([v for k, v in kwstyle.items() if k.startswith('css_')])
	return ' '.join(cls)

def get_component_props(*props, **kwprops) -> str:
	prp = list(props)
	if kwprops:
		prp.extend([v for k, v in kwprops.items() if k.startswith('prop_')])
	return ' '.join(prp)

def dialog_title_section(title: str, icon: str):
	with UIRow().classes('px-2 bg-primary'):
		ui.icon(icon, size='sm').classes('p-2')
		ui.label(title).classes('w-full text-lg')


class LoadingSpinner(ui.dialog):

	def __init__(
		self,
		text: str = '',
		*,
		value: bool = False,
		spinner: SpinnerType = 'default',
		size: str = '10em',
		color: str = 'primary',
		thickness: int = 5,
		**kwargs
	):
		super().__init__(value=value)
		self.text = text
		self.props('persistent backdrop-filters="opacity(90%)"')
		with self:
			with UIColumn(align_items='center'):
				if spinner is not None: ui.spinner(type=spinner, size=size, color=color, thickness=thickness)
				self.message = ui.label(text=text).classes('text-2xl text-white')\
					.bind_text_from(self)


class UIRow(ui.row):

	def __init__(
		self,
		*,
		wrap: bool = False,
		align_items: FlexAlignOpt = 'center',
		overflow: OverflowOpt = 'auto',
		gap: int = 1,
		**kwargs
	):
		super().__init__(wrap=wrap, align_items=align_items)
		base_class = {
			'css_gap': f'gap-x-{gap}',
			'css_overflow': f'overflow-{overflow}',
		}
		base_class.update(kwargs)
		self.classes(get_css_class(**base_class))


class UIColumn(ui.column):

	def __init__(
		self,
		*,
		wrap: bool = False,
		align_items: FlexAlignOpt = 'stretch',
		**kwargs
	):
		super().__init__(wrap=wrap, align_items=align_items)
		base_class = {
			'css_width': 'w-full',
			'css_padding': 'py-2 px-4',
			'css_gap': 'gap-2'
		}
		base_class.update(kwargs)
		self.classes(get_css_class(**base_class))


class NavButton(ui.button):

	def __init__(
		self,
		text: str = '',
		*,
		on_click: Optional[Callable[..., Any]] = None,
		color: ColorTemplate = 'primary',
		icon: Optional[str] = None,
		style: QButtonStyle = 'flat'
	):
		super().__init__(text, on_click=on_click, color=color, icon=icon)
		props = get_component_props('dense', 'no-caps', 'size=md', style)
		classes = get_css_class('px-2')

		self.props(props)
		self.classes(classes)


class NavDropdownButton(ui.dropdown_button):

	def __init__(
		self,
		text: str = '',
		*,
		value: bool = False,
		on_value_change: Optional[Callable[..., Any]] = None,
		on_click: Optional[Callable[..., Any]] = None,
		color: ColorTemplate = 'primary',
		icon: Optional[str] = None,
		auto_close: bool = True,
		split: bool = False,
		style: QButtonStyle = 'flat',
		**kwargs
	) -> None:
		super().__init__(text, value=value, on_value_change=on_value_change, on_click=on_click, color=color, icon=icon, auto_close=auto_close, split=split)
		props = get_component_props(
			style,
			'dense',
			'no-caps,',
			'no-icon-animation',
			'dropdown-icon=more_vert',
			'size=md',
			'menu-anchor="bottom start"',
			'menu-self="top left"',
		)
		base_class = {
			'css_padding': 'px-2',
		}
		base_class.update(kwargs)

		self.props(props)
		self.classes(get_css_class(**base_class))


@bindable_dataclass
class _DialogPromptState(BaseState):
	message: Union[str, Callable]
	title: str = ''
	choices: Union[List[str], Dict[str, str]] = field(default_factory=['yes', 'no'])


class DialogPrompt(ui.dialog):

	def __init__(self, message: str = '', choices: Union[List[str], Dict[str, str]] = {'yes': 'Ya', 'no': 'Tidak'}, *, value: bool = False):
		super().__init__(value=value)
		self.state = _DialogPromptState(message=message, choices=choices)

		with self, ui.card(align_items='center').classes('p-2'):
			with UIColumn(css_padding='p-1', css_gap='gap-1'):
				self.title()
				self.message()
				self.prompts()

	@ui.refreshable_method
	def title(self):
		if self.state.title:
			ui.label(self.state.title).classes('text-lg text-bold text-center')
			ui.separator()

	@ui.refreshable_method
	def message(self):
		if isinstance(self.state.message, str):
			ui.label(self.state.message).classes('mb-4')
		elif callable(self.state.message):
			self.state.message()

	@ui.refreshable_method
	def prompts(self):
		with UIRow(gap=2):
			ui.space()
			if isinstance(self.state.choices, list):
				for ch in self.state.choices:
					fn_click = partial(self.submit, ch)
					ButtonDialogControl(ch.title(), on_click=fn_click).props('no-caps')
			elif isinstance(self.state.choices, dict):
				for val, text in self.state.choices.items():
					fn_click = partial(self.submit, val)
					ButtonDialogControl(text, on_click=fn_click).props('no-caps')

	def set(self, **params):
		self.state.set(**params)
		self.title.refresh()
		self.message.refresh()
		self.prompts.refresh()


WBookFiles: TypeAlias = Dict[str, BytesIO]

@dataclass(frozen=True)
class UploadedFilesInfo:
	files: WBookFiles = field(default_factory=dict)
	filenames: List[str] = field(init=False, default_factory=list)
	filesizes: List[int] = field(init=False, default_factory=list)
	count: int = field(init=False, default=0)
	total_size: int = field(init=False, default=0)

	def __post_init__(self):
		attrs = dict(
			filenames=list(self.files.keys()),
			filesizes=list(map(lambda x: len(x.getvalue()), self.files.values())),
			count=len(self.files),
			total_size = sum(map(lambda x: len(x.getvalue()), self.files.values())),
		)
		for key, val in attrs.items():
			object.__setattr__(self, key, val)


@bindable_dataclass
class FilePickerState(BaseState):
	files: WBookFiles = field(default_factory=dict)
	filenames: List[str] = field(init=False, default_factory=list)
	filesizes: List[int] = field(init=False, default_factory=list)
	count: int = field(init=False, default=0)
	fileinfo: UploadedFilesInfo = field(init=False, default=None)
	queue: set[str] = field(default_factory=set)
	queue_count: int = 0

	def _recalculate(self):
		self.filenames = list(self.files.keys())
		self.filesizes = list(map(lambda x: len(x.getvalue()), self.files.values()))
		self.count = len(self.files)

	def _recalculate_queue(self):
		self.queue_count = len(self.queue)

	def update_files(self, files: WBookFiles):
		self.files.update(files)
		self.fileinfo = UploadedFilesInfo(files)
		self._recalculate()

	def add_queues(self, filenames: Union[str, Sequence[str]]):
		if isinstance(filenames, str):
			self.queue.add(filenames)
		else:
			self.queue.update(set(filenames))
		self._recalculate_queue()

	def del_queues(self, filenames: Union[str, Sequence[str]]):
		if isinstance(filenames, str):
			self.queue.discard(filenames)
		else:
			consume(map(lambda x: self.queue.discard(x), filenames))
		self._recalculate_queue()

	def clear_files(self):
		self.files.clear()
		self.fileinfo = None

	def clear_queue(self):
		self.queue.clear()

	def clear(self):
		self.clear_files()
		self.clear_queue()


class FilePickerv2(ui.dialog):
	"""
	"""
	_fileupload: ui.upload = None

	def __init__(
		self,
		*,
		value: bool = False,
		on_uploaded: Union[Callable[..., Any], None] = None,
		on_close: Union[Callable[..., Any], Awaitable, None] = None
	):
		"""

		Params:
			value : initial value
			on_uploaded : callable handler chained after file successfuly uploaded
			on_close : callable handler chained after dialog closed
		"""
		super().__init__(value=value)
		self._event_callbacks: Dict[str, Callable[..., Any]] = {
			'on_uploaded': on_uploaded,
			'on_close': on_close,
		}
		self.state = FilePickerState()

	async def _handle_showed(self, e: events.ValueChangeEventArguments) -> None:
		# self.reset()
		pass

	async def _handle_hidden(self, e: events.ValueChangeEventArguments) -> None:
		self.state.clear_queue()
		func = self._event_callbacks['on_close']

		if inspect.iscoroutinefunction(func):
			await func()
		elif inspect.isfunction(func):
			func()
		else:
			pass

	async def _handle_commit_upload(self, e: events.ClickEventArguments) -> None:
		self.notification = ui.notification(message='Mengunggah file...', spinner=True, timeout=None)
		await self._fileupload.run_method('upload')

	async def _handle_uploaded_multiple(self, e: events.MultiUploadEventArguments) -> None:
		async def readfiles(buffers: List[ui.upload.FileUpload]):
			results = dict()
			for buf in buffers:
				data = await buf.read()
				results[buf.name] = BytesIO(data)

			return results

		uploaded = await readfiles(e.files)
		self.state.update_files(uploaded)
		self.trigger_events('uploaded')
		ui.notify(f'{len(e.files)} file berhasil diunggah.', type='positive')
		self.notification.dismiss()
		# Wait for 2s to be automatically closed
		await asyncio.sleep(2)
		self.close()

	async def _handle_queue_added(self, e: events.GenericEventArguments) -> None:
		files = list(map(lambda file: file['__key'], e.args))
		self.state.add_queues(files)

	async def _handle_queue_removed(self, e: events.GenericEventArguments) -> None:
		files = list(map(lambda file: file['__key'], e.args))
		self.state.del_queues(files)

	async def _handle_queue_reset(self, e: events.GenericEventArguments) -> None:
		self._fileupload.reset()
		self.state.clear_queue()

	def render(self) -> Self:
		btn_props = 'dense round outline size=sm'
		with self, ui.card().classes('p-0 gap-y-0'):
			self._fileupload = ui.upload(
				label='Upload File Excel',
				multiple=True,
				max_files=config.MAX_FILES,
				max_file_size=config.MAX_FILE_SIZE,
				max_total_size=config.MAX_TOTAL_SIZE,
				on_multi_upload=self._handle_uploaded_multiple
			)\
				.props('bordered hide-upload-btn accept=".xlsx,.xls"')\
				.on('added', self._handle_queue_added)\
				.on('removed', self._handle_queue_removed)

			with ui.row().classes('w-full gap-x-2 p-2'):
				ui.space()
				btn_refresh = Button(icon='restart_alt', on_click=self._handle_queue_reset)\
					.props(btn_props)\
					.tooltip('Reset file')
				btn_upload = Button(icon='check_circle_outline', on_click=self._handle_commit_upload)\
					.bind_enabled_from(self.state, 'queue_count')\
					.props(btn_props)\
					.tooltip('Unggah file')

		self.on('show', self._handle_showed)\
			.on('hide', self._handle_hidden)

		return self

	def reset(self, *args, **kwargs) -> None:
		self.state.clear()
		self.trigger_events('uploaded')

	def trigger_events(self, event: str) -> None:
		e = self._event_callbacks.get('on_' + event)
		args = events.ValueChangeEventArguments(client=self.client, sender=self, value=self.state.fileinfo, previous_value=None)
		if callable(e):
			e(args)

	def on_uploaded(self, fn: Callable) -> Self:
		self._event_callbacks['on_uploaded'] = fn
		return self


@bindable_dataclass
class DateSetupState(BaseState):
	period: Literal['monthly', 'specific'] = 'specific'
	year: int = field(default=datetime.date.today().year)
	month: int = -1
	custom_start: bool = False
	custom_end: bool = False
	start: datetime.date = None
	end: datetime.date = None

	def set_period(self, value: str):
		self.period = value

	def set_year(self, value: int):
		self.year = value
		if self.month in range(1, 13):
			self.start = datetime.date(year=value, month=self.month, day=1)
			self.end = datetime.date(year=value, month=self.month, day=calendar.monthrange(value, self.month)[1])

	def set_month(self, value: int):
		self.month = value
		if value in range(1, 13):
			self.start = datetime.date(year=self.year, month=value, day=1)
			self.end = datetime.date(year=self.year, month=value, day=calendar.monthrange(self.year, value)[1])

	def set_start_date(self, value: datetime.date):
		self.start = value
		if isinstance(value, datetime.date) and isinstance(self.end, datetime.date):
			if value.year==self.end.year:
				self.year = value.year

			if value.month==self.end.month:
				self.month = value.month
			else:
				self.month = -1

	def set_end_date(self, value: datetime.date):
		self.end = value
		if isinstance(value, datetime.date) and isinstance(self.start, datetime.date):
			if value.year==self.start.year:
				self.year = value.year

			if value.month==self.start.month:
				self.month = value.month
			else:
				self.month = -1

	def try_strftime(self, value: Any, format: str = '%Y-%m-%d') -> Optional[str]:
		return try_strftime(value=value, format=format)


@bindable_dataclass
class SourceSetupState(BaseState):
	category: Literal['file', 'database'] = ''
	options: Dict[str, str] = field(default_factory=dict)
	source: Literal['soe', 'ofdb', 'rcd', 'rtu'] = ''
	file_uploaded: Optional[bool] = None
	file_info: UploadedFilesInfo = field(init=False, default=None)

	def set_source(self, value: str):
		self.source = value
		self.categorize(value)

	def categorize(self, input: str):
		if input in ('soe', 'rcd', 'rtu'):
			self.category = 'file'
		elif input=='ofdb':
			self.category = 'database'
		else:
			self.category = ''


@bindable_dataclass
class PanelAvState(BaseState):
	is_active: bool = False
	result_visible: bool = False
	setup_visible: bool = True
	progress_visible: bool = False
	force_regeneration: bool = False

	def toggle_visibility(self, name: str):
		attr = name + '_visible'
		if hasattr(self, attr):
			setattr(self, attr, not getattr(self, attr))


@dataclass(init=False)
class AvProxy:
	config: Config
	object: Union[RCD, RTU]
	state: BindableCoreState

	def __init__(self, config: Config, object: Union[RCD, RTU], interlock: InterlockState):
		self._preconfig = dict()
		self.config = create_bindable(config)
		self.object = object
		self.iloc = interlock
		state_dict = asdict(self.object.state)
		del state_dict['progress']
		self.state = BindableCoreState(**state_dict)
		self.state.start_tracking(self.object.state)

	@ui.refreshable_method
	def config_layout(self):
		with ui.list().classes('px-2'):
			with ui_item():
				with ui_section():
					ui_menu_label('Parameter')
				with ui_section():
					ui.label('Value')

	def dialog_config(self) -> ui.dialog:
		with ui.dialog().on_value_change(self.event_dialog_changed).props('persistent') as dialog, ui.card(align_items='stretch').classes('min-w-lg pt-0 pb-2 px-0 gap-y-1'):
			dialog_title_section(title='Pengaturan', icon='settings')
			self.config_layout()
			ui.separator()
			with UIRow(gap=2).classes('px-4'):
				ui.space()
				ButtonDialogControl('Batal', color='yellow', on_click=EventChainsWithoutArgs((dialog.close, self.event_cancel_changes))).props('text-color=black')
				ButtonDialogControl('Simpan', color='secondary', on_click=dialog.close)

		return dialog

	def event_dialog_changed(self, e: events.ValueChangeEventArguments):
		if e.value==True:
			self.config_layout.refresh()
			self._preconfig = asdict(self.config)

	def event_cancel_changes(self):
		self.config.set(**self._preconfig)


class GUIAvailability(ui.tab_panel):

	def __init__(
		self,
		name: str,
		logger: ui.log,
		options: Dict[str, str] = dict(),
	):
		super().__init__(name=name)
		self._refreshable = list()
		self.name = name
		self.state = PanelAvState()
		self.iloc = InterlockState()
		self.src = SourceSetupState()
		self.dater = DateSetupState()
		self.av = self.init_av(interlock=self.iloc, logger=logger)
		self.logger = logger

		with self:
			self.filepicker = FilePickerv2(
				on_uploaded=self.event_file_uploaded,
				on_close=self.event_filepicker_closed
			).render()
			self.dialog_prompt = DialogPrompt()
			self.dialog_dater = self.dialog_dater_setup()
			self.dialog_avcfg = self.dialog_avconfig()
			with UIColumn(css_gap='gap-1', css_padding='px-0'):
				self.group_label(text='Setup', name='setup')
				self.parameter_setup(options=options)

			self.section_result()
			with UIRow().classes('w-full'):
				self.button_calculate = Button('Hitung', on_click=self.do_operation)\
					.bind_enabled_from(self.iloc, 'enable_calculate')\
					.classes('w-full')
				Button(icon='settings', on_click=self.dialog_avcfg.open).props('outline').classes('px-2')

	def init_av(self, interlock: InterlockState, logger: Optional[ui.log] = None) -> AvProxy:
		raise ValueError('Must be overidden.')

	def set_active(self, value: bool):
		self.state.is_active = value

	def update_refreshable(self, *args):
		consume(map(lambda comp: comp.refresh(), self._refreshable))

	def group_label(self, text: str, name: str, can_toggle: bool = True) -> ui.element:
		with UIRow(overflow='hidden', gap=2).classes('py-1') as glabel:
			ui.label(text).classes('font-bold whitespace-nowrap')
			if can_toggle:
				Button(icon='visibility_off', color='grey', on_click=lambda: self.state.toggle_visibility(name))\
					.bind_icon_from(self.state, f'{name}_visible', lambda vis: 'visibility_off' if vis else 'visibility')\
					.props('dense flat rounded')\
					.tooltip(f'Tampilkan / sembunyikan {text.lower()}')
			ui.separator().classes('w-fill')
		return glabel

	def section_result(self):
		self._refreshable.append(self.calculation_result)
		with UIColumn(css_gap='gap-1', css_padding='px-0'):
			self.group_label(text='Hasil Perhitungan', name='result')
			with UIColumn(css_padding='p-0').bind_visibility_from(self.state, 'result_visible'):
				with UIRow():
					Button('', icon='edit_calendar', color='secondary', on_click=self.dialog_dater.open)\
						.props('outline dense')\
						.tooltip('Ubah periode waktu')
					Button('', icon='preview', color='secondary')\
						.bind_enabled_from(self.iloc, 'enable_download')\
						.props('outline dense')\
						.tooltip('Lihat data')
					Button('', icon='file_download', color='secondary', on_click=self.download_result)\
						.bind_enabled_from(self.iloc, 'enable_download')\
						.props('outline dense')\
						.tooltip('Download')
					ui.checkbox()\
						.bind_value(self.state, 'force_regeneration')\
						.props('dense color=secondary')\
						.tooltip('Force generate file')
				self.calculation_result()

	def parameter_setup(self, options: Dict[str, str] = dict()):
		chain_reset = EventChainsWithoutArgs(chains=[
			self.av.object.reset,
			self.dater.reset,
			self.reset_operation,
			self.update_refreshable,
		])
		with ui.list()\
			.bind_visibility_from(self.state, 'setup_visible')\
			.classes('w-full'):
			with ui_item():
				with ui_section():
					ui_menu_label('Data Input')
				with ui_section():
					self.select_input = Select(options=options, on_change=self.event_input_source_changed)\
						.bind_value_from(self.src, 'source')\
						.bind_enabled_from(self.iloc, 'enable_change_input')\
						.classes('w-full')
			with ui_item():
				with ui_section():
					ui_menu_label('Master / Server')
				with ui_section():
					ui_input().bind_value_from(self.av.config, 'master', backward=lambda t: str(t).title())\
						.props('disable filled')
			with ui_item():
				with ui_section():
					ui_menu_label('').bind_text_from(self.src, 'category', backward=lambda x: str(x).title())
				with ui_section():
					with UIRow(overflow='visible')\
						.bind_visibility_from(self.src, 'category', value='file')\
						.classes('w-full'):
						self.button_reset = Button('Reset', color='info', on_click=chain_reset)\
							.bind_visibility_from(self.src, 'file_uploaded', value=True)\
							.bind_enabled_from(self.iloc, 'enable_reset')\
							.props('dense')\
							.classes('w-24 px-2')
						self.button_fileupload = Button('Pilih File', on_click=self.filepicker.open)\
							.bind_visibility_from(self.src, 'file_uploaded', lambda b: not b)\
							.bind_enabled_from(self.iloc, 'enable_upload_file')\
							.props('dense')\
							.classes('w-24 px-2')
						self.button_fileinfo = Button('', on_click=self.show_fileinfo, icon='attach_file', color='teal')\
							.bind_enabled_from(self.iloc, 'enable_view_file_list')\
							.props('dense flat')\
							.classes('px-0')
						ui.icon(name='check_circle_outline', size='sm', color='positive')\
							.bind_visibility_from(self.av.state, 'loaded', value=True, strict=False)\
							.tooltip('Validasi OK')
						ui.icon(name='error_outline', size='sm', color='negative')\
							.bind_visibility_from(self.av.state, 'loaded', value=False, strict=False)\
							.tooltip('Validasi NOK')
					with UIRow(overflow='visible')\
						.bind_visibility_from(self.src, 'category', value='database')\
						.classes('w-full'):
						self.button_checkdb = Button('Cek Koneksi', on_click=lambda: ui.notify('Untuk saat ini, fitur ini belum dapat digunakan', type='ongoing'))\
							.bind_enabled_from(self.src, 'category')\
							.props('dense')\
							.classes('px-2')

	def fileinfo_content(self):
		props1 = ['dense', 'separator']
		if self.src.file_info.count:
			props1.append('bordered')

		with ui.element('div').classes('min-w-sm mb-4'):
			with ui.list()\
				.props(get_component_props(*props1))\
				.classes('w-full max-h-36 pr-0 overflow-y-auto'):
				for i in range(self.src.file_info.count):
					with ui.item().props('clickable'):
						with ui.item_section():
							ui.item_label(f'{i+1}. {self.src.file_info.filenames[i]}').props('lines=1')
						with ui.item_section().props('side'):
							ui.item_label(f'{self.src.file_info.filesizes[i]/10**6:.2f}MB')
			# Summary statement
			ui.html(f'Total : <strong>{self.src.file_info.count}</strong> file ({self.src.file_info.total_size/10**6:.1f}MB)', sanitize=False).classes('mt-2')

	def dialog_avconfig(self) -> ui.dialog:
		with ui.dialog().props('presistent') as dialog, ui.card(align_items='center').classes('min-w-lg p-2 gap-y-1'):
			with ui.list():
				with ui_item():
					with ui_section():
						ui_menu_label('Parameter')
					with ui_section():
						ui.label('Value')

			ui.separator()
			with UIRow(gap=2).classes('w-full'):
				ui.space()
				ButtonDialogControl('Batal', on_click=dialog.close).props('color=white text-color=black')
				ButtonDialogControl('Simpan', on_click=dialog.close).props('color=secondary')

		return dialog

	def dialog_dater_setup(self) -> ui.dialog:
		with ui.dialog().on_value_change(self.event_dialog_dater_changed).props('persistent') as dialog, ui.card(align_items='stretch').classes('min-w-lg pt-0 pb-2 px-0 gap-y-1'):
			dialog_title_section(title='Periode waktu', icon='edit_calendar')
			self.date_setup_layout()
			ui.separator()
			with UIRow(gap=2).classes('px-4'):
				ui.space()
				ButtonDialogControl('Tutup', on_click=dialog.close)

		return dialog

	@ui.refreshable_method
	def date_setup_layout(self):
		with UIColumn():
			with UIRow():
				ui.radio(options=dict(monthly='Bulanan', specific='Spesifik'))\
					.bind_value(self.dater, 'period')\
					.bind_enabled_from(self.src, 'source')\
					.props('inline')
			with UIRow().bind_visibility_from(self.dater, 'period', value='monthly'):
				ui.select(options={-1: '--------', **MONTH_OPTIONS}, on_change=self.event_date_month_changed)\
					.bind_value_from(self.dater, 'month')\
					.classes('w-40')
				ui.select(options=[self.dater.year - x for x in range(3)], on_change=self.event_date_year_changed)\
					.bind_value_from(self.dater, 'year')
			with UIRow().bind_visibility_from(self.dater, 'period', value='specific'):
				ui.checkbox('Dari')\
					.bind_value(self.dater, 'custom_start')\
					.bind_enabled_from(self.src, 'source')\
					.props('dense inline')
				ui_input(on_change=self.event_start_date_changed)\
					.bind_value_from(self.dater, 'start', backward=self.convert_date2string)\
					.bind_enabled_from(self.dater, 'custom_start')\
					.props('type="date"')\
					.classes('ml-1 w-1/2')
				ui.label('').classes('mx-4 text-nowrap')
				ui.checkbox('Sampai')\
					.bind_value(self.dater, 'custom_end')\
					.bind_enabled_from(self.src, 'source')\
					.props('dense inline')
				ui_input(on_change=self.event_end_date_changed)\
					.bind_value_from(self.dater, 'end', backward=self.convert_date2string)\
					.bind_enabled_from(self.dater, 'custom_end')\
					.props('type="date"')\
					.classes('ml-1 w-1/2')

	@staticmethod
	def render_result_table(
		params: List[Tuple[str, str]],
		date_start: Optional[datetime.datetime] = None,
		date_end: Optional[datetime.datetime] = None
	):
		def try_strftime(value: datetime.datetime, default: str = 'dd-mm-yyyy') -> str:
			try:
				return value.strftime('%d-%m-%Y')
			except Exception:
				return default

		datestr_start = try_strftime(date_start)
		datestr_end = try_strftime(date_end)
		with UIRow():
			ui.icon('date_range', size='sm', color='blue-grey').classes('mr-2')
			ui.label(datestr_start).classes('underline text-slate-400')
			ui.label('s/d').classes('mx-1')
			ui.label(datestr_end).classes('underline text-slate-400')
		with UIRow(align_items='start'):
			rows = len(params)//2 + len(params)%2
			for x in range(2):
				# Split into 2 section columns
				with ui.list()\
					.props('dense')\
					.classes('w-full'):
					for param in params[rows*x:rows*(x+1)]:
						with ui.item():
							with ui_section():
								ui_menu_label(param[0])
							with ui_section()\
								.props('side')\
								.classes('w-1/3 border border-solid')\
								.style('padding-left: 0;'):
								ui_menu_label(param[1]).classes('mx-auto px-2')

	def get_result_kwargs(self) -> Dict[str, Any]:
		return dict(params=[])

	@ui.refreshable_method
	def calculation_result(self):
		self.render_result_table(**self.get_result_kwargs())

	def event_input_source_changed(self, e: events.ValueChangeEventArguments):
		self.src.set_source(e.value)
		self.iloc.set_input_source(e.value)

	def event_master_changed(self, e: events.ValueChangeEventArguments):
		self.av.config.master = e.value

	def convert_date2string(self, value: datetime.date) -> Optional[str]:
		return self.dater.try_strftime(value)

	def event_date_year_changed(self, e: events.ValueChangeEventArguments):
		self.dater.set_year(e.value)

	def event_date_month_changed(self, e: events.ValueChangeEventArguments):
		self.dater.set_month(e.value)

	def event_start_date_changed(self, e: events.ValueChangeEventArguments):
		# NOTE : Value received here is string
		try:
			self.dater.set_start_date(datetime.date.fromisoformat(e.value))
		except (TypeError, ValueError):
			self.dater.set_start_date(None)

	def event_end_date_changed(self, e: events.ValueChangeEventArguments):
		# NOTE : Value received here is string
		try:
			self.dater.set_end_date(datetime.date.fromisoformat(e.value))
		except (TypeError, ValueError):
			self.dater.set_end_date(None)

	def event_file_uploaded(self, e: events.ValueChangeEventArguments):
		fileinfo: UploadedFilesInfo = e.value
		self.src.file_info = fileinfo
		self.src.file_uploaded = bool(fileinfo.count)
		self.iloc.set_uploaded(bool(fileinfo.count))
		if fileinfo.count<3:
			fname = ' dan '.join(fileinfo.filenames[:2])
		else:
			fname = ', '.join(fileinfo.filenames[:2]) + f' dan {fileinfo.count-2} file lainnya'

		self.logger.push(logprint(f'File {fname} telah berhasil diupload.', level='info', cli=False), **params.SUCCESSLOG_KWARGS)

	async def event_filepicker_closed(self):
		if self.src.file_uploaded:
			self.iloc.enable_calculate = True

	async def show_fileinfo(self):
		content = self.fileinfo_content
		self.dialog_prompt.set(
			title='File Input',
			message=content,
			choices={'ok': 'OK'}
		)
		result = await self.dialog_prompt

	def reset_operation(self, *args):
		self.button_calculate.props(remove='loading')
		self.src.file_uploaded = False
		self.iloc.set_input_source(self.src.source)
		self.logger.push(logprint(f'Reset GUI {self.av.object.__class__.__name__}.', level='info', cli=False))

	@toggle_attr('state.progress_visible', True, False)
	async def do_operation(self):
		self.button_calculate.props('loading')

		step1 = await self.do_read_file()
		if isinstance(step1, soe.SOE):
			step2 = await self.do_analyze(step1.data)
		else:
			step2 = step1

		if isinstance(step2, pd.DataFrame):
			step3 = await self.do_calculation()
			if isinstance(step3, AvailabilityResult) and self.dater.start is None:
				self.dater.set_start_date(step3.data.start_date.date())
				self.dater.set_end_date(step3.data.end_date.date())

		await asyncio.sleep(1)
		self.button_calculate.props(remove='loading')

	async def do_read_file(self):
		if self.src.source not in ('soe', 'rcd', 'rtu'):
			return

		result = None
		if not self.av.state.loaded:
			files: FileDict = self.src.file_info.files
			# Load file
			self.iloc.set_loading(True)
			if self.src.source=='soe':
				df_soe = await self.av.object.async_read_soe_file(files=files, log_callback=self.logger.push)
				msg = f'Data / file SOE {self.av.config.master.title()} {{status}}'
				if isinstance(df_soe, pd.DataFrame):
					# df_soe is valid
					result = soe.SOE(df_soe, config=self.av.config, sources=self.av.object.reader.sources)
					self.logger.push(logprint(msg.format(status='valid'), level='info', cli=False), **params.INFOLOG_KWARGS)
				else:
					self.logger.push(logprint(msg.format(status='tidak valid'), level='error', cli=False), **params.ERRORLOG_KWARGS)
			else:
				result = await self.av.object.async_read_file(files=files, log_callback=self.logger.push)

			self.iloc.set_loading(False)
			self.iloc.set_loaded(self.av.object.state.loaded)
		else:
			# Use cached data
			result = self.av.object.data

		return result

	async def do_analyze(self, data: soe.SOEData):
		# Analyze data event
		return await self.av.object.async_analyze_soe(soe=data, log_callback=self.logger.push)

	async def do_calculation(self):
		# Calculate availability
		result = self.av.object.calculate(start_date=self.dater.start, end_date=self.dater.end, log_callback=self.logger.push)
		self.calculation_result.refresh()
		self.iloc.set_calculated(self.av.object.state.calculated)
		return result

	@toggle_attr('state.progress_visible', True, False)
	async def generate_file(self):
		filename = f'temp/{datetime.datetime.timestamp(datetime.datetime.now()):.0f}_availability_{self.av.object.__class__.__name__.lower()}_{self.av.object.result.date_min.strftime("%Y%m%d")}-{self.av.object.result.date_max.strftime("%Y%m%d")}'
		file = self.av.object.write_file(filename=filename, log_callback=self.logger.push)
		ui.notify('Generate file SUKSES', type='positive')

	async def download_result(self):
		if not self.av.state.exported or self.state.force_regeneration:
			await self.generate_file()

		dateparm = f'{self.dater.start.strftime("%Y%m%d")}-{self.dater.end.strftime("%Y%m%d")}'
		download_file = f'Availability_{self.av.object.__class__.__name__}_{dateparm}.xlsx'
		ui.download.file(self.av.object.state.last_exported_file, download_file)
		self.logger.push(logprint(f'File {download_file} berhasil diunduh.', level='info', cli=False), **params.SUCCESSLOG_KWARGS)

	def event_change_source(self, value: str):
		self.src.set_source(value)
		self.iloc.set_input_source(value)

	def event_change_uploaded(self, value: bool):
		self.src.file_uploaded = value
		self.iloc.set_uploaded(value)

	def event_dialog_dater_changed(self, e: events.ValueChangeEventArguments):
		if e.value==True:
			self.date_setup_layout.refresh()

	def refresh_state(self, *args):
		self.iloc.set_input_source(self.src.source)
		self.iloc.set_uploaded(self.src.file_uploaded)


################################################################################################
# AVAILABILITY RCD
################################################################################################


@dataclass
class RCDProxy(AvProxy):
	config: RCDConfig
	object: RCD

	def __init__(self, interlock: InterlockState, logger: Optional[ui.log] = None):
		config = RCDConfig()
		super().__init__(config=config, object=RCD(config, log_callback=logger.push), interlock=interlock)

	@ui.refreshable_method
	def config_layout(self):
		with ui.list().classes('px-2'):
			with ui_item():
				with ui_section():
					ui_menu_label('Master')
				with ui_section():
					with UIRow(overflow='visible').classes('w-full'):
						ui.radio(options=dict(spectrum='Spectrum', survalent='Survalent'))\
							.bind_value(self.config, 'master', strict=True)\
							.bind_enabled_from(self.iloc, 'enable_change_master')\
							.props('dense inline')\
							.classes('text-sm')
			with ui_item():
				with ui_section():
					ui_menu_label('Hitung BI / DS Bus')
				with ui_section():
					ui.checkbox()\
						.bind_value(self.config, 'include_bi', strict=True)\
						.props('dense')
			with ui_item():
				with ui_section():
					ui_menu_label('Hitung RC Berulang')
				with ui_section():
					ui.checkbox()\
						.bind_value(self.config, 'include_repetition', strict=True)\
						.props('dense')
			with ui_item():
				with ui_section():
					ui_menu_label('Threshold Gagal RC (untuk tagging)')
				with ui_section():
					ui_input()\
						.bind_value(self.config, 'reduction_ratio_threshold', forward=int, strict=True)\
						.props('min=1 max=9 type="number"')
			with ui_item():
				with ui_section():
					ui_menu_label('Range Waktu Cari (detik)')
				with ui_section():
					ui_input()\
						.bind_value(self.config, 't_search', forward=int, strict=True)\
						.props('min=10 max=600 type="number"')
			with ui_item():
				with ui_section():
					ui_menu_label('Penanda sukses')
				with ui_section():
					ui_input().bind_value(self.config, 'success_mark', strict=True)
			with ui_item():
				with ui_section():
					ui_menu_label('Penanda gagal')
				with ui_section():
					ui_input().bind_value(self.config, 'failed_mark', strict=True)
			with ui_item():
				with ui_section():
					ui_menu_label('Penanda dianulir')
				with ui_section():
					ui_input().bind_value(self.config, 'unused_mark', strict=True)


class RCDTabPanel(GUIAvailability):
	av: RCDProxy

	def init_av(self, interlock: InterlockState, logger: Optional[ui.log] = None) -> RCDProxy:
		return RCDProxy(interlock=interlock, logger=logger)

	def get_result_kwargs(self):
		return dict(
			params=[
				('Total', rgetattr(self.av.object, 'result.total_count', 0)),
				('Valid', rgetattr(self.av.object, 'result.total_valid', 0)),
				('Repetisi', rgetattr(self.av.object, 'result.total_reps', 0)),
				('Total Sukses', rgetattr(self.av.object, 'result.total_success', 0)),
				('Total Gagal', rgetattr(self.av.object, 'result.total_failed', 0)),
				('Rasio Sukses', f'{rgetattr(self.av.object, "result.success_ratio", 0)*100:.2f}%'),
				('Sukses Close', rgetattr(self.av.object, 'result.total_success_close', 0)),
				('Gagal Close', rgetattr(self.av.object, 'result.total_failed_close', 0)),
				('Rasio Sukses Close', f'{rgetattr(self.av.object, "result.success_close_ratio", 0)*100:.2f}%'),
				('Sukses Open', rgetattr(self.av.object, 'result.total_success_open', 0)),
				('Gagal Open', rgetattr(self.av.object, 'result.total_failed_open', 0)),
				('Rasio Sukses Open', f'{rgetattr(self.av.object, "result.success_open_ratio", 0)*100:.2f}%'),
			],
			date_start=rgetattr(self.av.object, 'result.data.start_date', None),
			date_end=rgetattr(self.av.object, 'result.data.end_date', None),
		)

	def dialog_avconfig(self):
		return self.av.dialog_config()


################################################################################################
# AVAILABILITY RTU
################################################################################################


@dataclass
class RTUProxy(AvProxy):
	config: RTUConfig
	object: RTU

	def __init__(self, interlock: InterlockState, logger: Optional[ui.log] = None):
		config = RTUConfig()
		super().__init__(config=config, object=RTU(config, log_callback=logger.push), interlock=interlock)

	@ui.refreshable_method
	def config_layout(self):
		with ui.list().classes('px-2'):
			with ui_item():
				with ui_section():
					ui_menu_label('Master')
				with ui_section():
					with UIRow(overflow='visible').classes('w-full'):
						ui.radio(options=dict(spectrum='Spectrum', survalent='Survalent'))\
							.bind_value(self.config, 'master', strict=True)\
							.bind_enabled_from(self.iloc, 'enable_change_master')\
							.props('dense inline')\
							.classes('text-sm')
			with ui_item():
				with ui_section():
					ui_menu_label('Selektif RTU')
				with ui_section():
					ui.checkbox()\
						.bind_value(self.config, 'known_rtus_only', strict=True)\
						.props('dense')
			with ui_item().bind_visibility_from(self.config, 'known_rtus_only', strict=True):
				with ui_section():
					ui_menu_label('List RTU')
				with ui_section():
					Select(options=['', *config.RTU_NAMES_CONFIG.keys()])\
						.bind_value(self.config, 'rtu_file_name', strict=True)\
						.props('dense')
			with ui_item():
				with ui_section():
					ui_menu_label('Penanda down HAR')
				with ui_section():
					ui_input().bind_value(self.config, 'maintenance_mark', strict=True)
			with ui_item():
				with ui_section():
					ui_menu_label('Penanda down link')
				with ui_section():
					ui_input().bind_value(self.config, 'link_failure_mark', strict=True)
			with ui_item():
				with ui_section():
					ui_menu_label('Penanda down RTU')
				with ui_section():
					ui_input().bind_value(self.config, 'rtu_failure_mark', strict=True)
			with ui_item():
				with ui_section():
					ui_menu_label('Penanda down lainnya')
				with ui_section():
					ui_input().bind_value(self.config, 'other_failure_mark', strict=True)


class RTUTabPanel(GUIAvailability):
	av: RTUProxy

	def init_av(self, interlock: InterlockState, logger: Optional[ui.log] = None) -> RTUProxy:
		return RTUProxy(interlock=interlock, logger=logger)

	def get_result_kwargs(self):
		def calculate_percentage(n, trange, tdown):
			try:
				result = 1 - (tdown / (n * trange))
			except ZeroDivisionError:
				result = 0
			return result

		rtu_count = rgetattr(self.av.object, 'result.rtu_count', 0)
		total_periods = rgetattr(self.av.object, 'result.total_periods', datetime.timedelta(0))
		total_dt_all = rgetattr(self.av.object, 'result.uncategorized.total_downtime', datetime.timedelta(0))
		total_dt_rtu = rgetattr(self.av.object, 'result.rtu.total_downtime', datetime.timedelta(0))
		total_dt_link = rgetattr(self.av.object, 'result.link.total_downtime', datetime.timedelta(0))
		av_all = calculate_percentage(rtu_count, total_periods, total_dt_all)
		av_rtu = calculate_percentage(rtu_count, total_periods, total_dt_rtu)
		av_link = calculate_percentage(rtu_count, total_periods, total_dt_link)
		max_occur = rgetattr(self.av.object, 'result.max_occurences', None)
		max_total = rgetattr(self.av.object, 'result.max_total_dt', None)
		rtu1 = max_occur.rtu if isinstance(max_occur, rtu.RTUAvailabilityModel) else '-'
		rtu2 = max_total.rtu if isinstance(max_total, rtu.RTUAvailabilityModel) else '-'
		return dict(
			params=[
				('Jumlah RTU', rtu_count),
				('x Down', rgetattr(self.av.object, 'result.total_count', 0)),
				('x Down RTU', rgetattr(self.av.object, 'result.rtu.total_count', 0)),
				('x Down Link', rgetattr(self.av.object, 'result.link.total_count', 0)),
				('x Down >>', rtu1),
				('Periode', f'{total_periods.days} hari'),
				('Availability (Overall)', f'{av_all*100:.2f}%'),
				('Availability RTU', f'{av_rtu*100:.2f}%'),
				('Availability Link', f'{av_link*100:.2f}%'),
				('Total Downtime >>', rtu2),
			],
			date_start=rgetattr(self.av.object, 'result.data.start_date', None),
			date_end=rgetattr(self.av.object, 'result.data.end_date', None),
		)

	def dialog_avconfig(self):
		return self.av.dialog_config()
