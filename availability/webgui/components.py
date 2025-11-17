import asyncio, datetime, functools, inspect, os
from dataclasses import InitVar, dataclass, field, fields, is_dataclass
from functools import partial
from io import BytesIO

import cryptography.fernet
import pandas as pd
from nicegui import events, ui
from nicegui.binding import bindable_dataclass
from nicegui.elements.mixins.value_element import ValueElement

from .event import EventChainsWithArgs, EventChainsWithoutArgs
from .state import AvStateWrapper, BaseState, CalculationState, FileInputState, InterlockState, OfdbInputState, toggle_attr
from .types import *
from .. import config
from ..core.rtu import RTUConfig, AvRTUResult
from ..globals import MONTH_OPTIONS
from ..lib import consume, rgetattr, try_strftime
from ..utils.worker import run_cpu_bound


FileCalcObjects: TypeAlias = Union[Any, Any]
OfdbCalcObjects: TypeAlias = Union[Any]
CalcObjects: TypeAlias = Union[FileCalcObjects, OfdbCalcObjects]


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
				ui.label('').classes('border').bind_text_from(dtclass, f.name, lambda x: repr(x) if callable(x) else str(x))

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


class FilePicker(ui.dialog):
	"""
	"""
	_event_callbacks: Dict[str, Callable[..., Any]]
	_files: Dict[str, BytesIO]
	_queued_files: List[str]
	upload_props = 'bordered hide-upload-btn accept=".xlsx,.xls" color=teal'
	upload_classes = ''
	button_props = 'dense round outline size=sm'
	button_classes = ''

	def __init__(
		self,
		*,
		value: bool = False,
		on_file_change: Optional[Callable[..., Any]] = None,
		on_close: Optional[Callable[..., Any]] = None
	) -> None:
		super().__init__(value=value)
		self._event_callbacks = {
			'on_file_change': on_file_change,
			'on_close': on_close
		}
		self._files = dict()
		self._queued_files = list()

	def render(self) -> ui.dialog:
		with self, ui.card().classes('p-0 gap-y-0'):
			self._fileupload = ui.upload(label='Upload File Excel', multiple=True, max_files=config.MAX_FILES, max_file_size=config.MAX_FILE_SIZE, max_total_size=config.MAX_TOTAL_SIZE, on_multi_upload=self._handle_uploaded_multiple)\
				.props(self.upload_props)\
				.classes(self.upload_classes)\
				.on('added', self._handle_queue_added)\
				.on('removed', self._handle_queue_removed)
			with ui.row().classes('w-full gap-x-2 p-2'):
				ui.space()
				btn_refresh = Button(icon='restart_alt', on_click=self._handle_queue_reset)\
					.props(self.button_props)\
					.classes(self.button_classes)\
					.tooltip('Reset file')
				btn_upload = Button(icon='check_circle_outline', on_click=self._handle_commit_upload)\
					.props(self.button_props)\
					.classes(self.button_classes)\
					.tooltip('Unggah file')\
					.bind_enabled_from(self, 'queue_count')
		self.on('show', self._handle_showed).on('hide', self._handle_hidden)
		# self.trigger_events('file_change')
		return self

	def reset(self, *args, **kwargs) -> None:
		self._queued_files.clear()
		self.files.clear()
		self.trigger_events('file_change')

	def trigger_events(self, event: str) -> None:
		e = self._event_callbacks.get('on_' + event)
		args = events.ValueChangeEventArguments(client=self.client, sender=self, value=self.files)
		if callable(e): e(args)

	def on_file_change(self, fn: Callable) -> ui.dialog:
		self._event_callbacks['on_file_change'] = fn
		return self

	async def _handle_showed(self, e: events.ValueChangeEventArguments) -> None:
		self.reset()

	async def _handle_hidden(self, e: events.ValueChangeEventArguments) -> None:
		self._queued_files.clear()

	async def _handle_commit_upload(self, e: events.ClickEventArguments) -> None:
		self.notification = ui.notification(message='Mengunggah file...', spinner=True, timeout=None)
		self._fileupload.run_method('upload')

	async def _handle_uploaded_multiple(self, e: events.MultiUploadEventArguments) -> None:
		async def read_buffer(buffers: list):
			results = list()
			for buf in buffers:
				with buf:
					results.append(BytesIO(buf.read()))
			return results

		iobuffers = await read_buffer(e.contents)
		uploaded_files = dict(zip(e.names, iobuffers))
		self._files = uploaded_files
		self.trigger_events('file_change')
		ui.notify(f'{len(e.names)} file berhasil diunggah.', type='positive')
		self.notification.dismiss()
		await asyncio.sleep(2)
		self.close()

	async def _handle_queue_added(self, e: events.GenericEventArguments) -> None:
		files = list(map(lambda file: file['__key'], e.args))
		self._queued_files.extend(files)

	async def _handle_queue_removed(self, e: events.GenericEventArguments) -> None:
		files = set(map(lambda file: file['__key'], e.args))
		queued_files = set(self._queued_files)
		self._queued_files = list(queued_files - files)

	async def _handle_queue_reset(self, e: events.GenericEventArguments) -> None:
		self._fileupload.reset()
		self._queued_files.clear()

	@property
	def files(self):
		return self._files

	@property
	def filenames(self):
		return list(self.files.keys())

	@property
	def filesizes(self):
		return list(map(lambda x: len(x.getvalue()), self.files.values()))

	@property
	def queue_count(self):
		return len(self._queued_files)


class CalculationStepper(ui.stepper):
	"""
	"""
	__used__: set
	instance: CalcObjects
	# List of steps_input + steps_calculation
	steps_list: List[Dict[str, Any]]
	steps_input: List[Dict[str, Any]] = list()
	steps_calculation: List[Dict[str, Any]] = [
		{
			'name': 'calculate',
			'title': 'Hitung Kinerja',
			'icon': 'calculate',
			'description': 'Analisa dan lakukan proses perhitungan kinerja.',
			'navigation': [
				{'text': 'Kembali', 'on_click': 'previous'},
				{'text': 'Hitung', 'on_click': '_handle_calculate'},
				{'text': 'Lihat Hasil', 'on_click': '_handle_preview_result'},
				{'text': 'Lanjut', 'on_click': 'next'},
			]
		},
		{
			'name': 'download',
			'title': 'Unduh Hasil',
			'icon': 'file_download',
			'description': 'Export hasil perhitungan kedalam bentuk file Excel.',
			'navigation': [
				{'text': 'Kembali', 'on_click': 'previous'},
				{'text': 'Unduh', 'on_click': '_handle_download_result'},
				{'text': 'Selesai', 'on_click': 'next'},
			]
		},
		{
			'name': 'completed',
			'title': 'Selesai',
			'icon': 'done_all',
			'description': 'Selamat! Proses perhitungan kinerja telah selesai.',
			'navigation': [
				{'text': 'Kembali', 'on_click': 'previous'},
			]
		},
	]
	button_classes: str = 'px-2'
	button_props: str = 'dense no-caps size=md'
	auto_next: bool = True
	auto_next_delay: float = 1.5

	def __init__(
		self,
		*,
		instance: Optional[Any] = None,
		value: Union[str, ui.step, None] = None,
		on_value_change: Optional[Callable[..., Any]] = None,
		keep_alive: bool = True,
		**contexts
	) -> None:
		super().__init__(value=value, on_value_change=on_value_change, keep_alive=keep_alive)
		self.__used__ = set([attr for attr in dir(ui.stepper) if not (attr.startswith('_') and attr.endswith('_'))])
		self._step_objects = dict()
		self.instance = instance
		self.state = CalculationState()
		self.steps_list = self.steps_input + self.steps_calculation
		# Set graphical props, classes and style
		self.props(add='vertical flat done-color=positive active-color=accent active-icon=my_location error-color=negative')
		self.classes('w-full h-full p-0')
		# Add custom attribute
		for key in contexts:
			if not key.startswith('_') and key not in self.__used__: setattr(self, key, contexts[key])

	def default_renderer(
		self,
		name: str,
		title: str,
		icon: Optional[str] = None,
		navigation: List[Dict[str, str]] = [],
		description: str = ''
	) -> ui.step:
		_icon = 'none' if icon is None else icon
		with ui.step(name=name, title=title, icon=_icon) as qstep:
			ui.label(description)
			with ui.stepper_navigation().style('padding: 0;') as qstep_nav:
				for btn in navigation:
					handler = getattr(self, btn['on_click'], None)
					Button(text=btn['text'], on_click=handler)\
						.props(self.button_props)\
						.classes(self.button_classes)
		return qstep

	def get_renderer(self, key: str) -> Callable:
		_key = key.replace(' ', '_').replace('-', '_')
		renderer = getattr(self, 'render_' + _key, None)
		return renderer if callable(renderer) else self.default_renderer

	def render(self) -> ui.stepper:
		with self:
			for step in self.steps_list:
				renderer = self.get_renderer(step['name'])
				qstep = renderer(**step)
				self._step_objects.update({step['name']: qstep})
		return self

	def reset(self, *args, **kwargs) -> None:
		pass

	def render_calculate(
		self,
		name: str,
		title: str,
		icon: Optional[str] = None,
		navigation: List[Dict[str, str]] = [],
		description: str = ''
	) -> ui.step:
		_icon = 'none' if icon is None else icon
		with ui.step(name=name, title=title, icon=_icon) as qstep:
			ui.label(description)
			self.calculation_summary()
			with ui.stepper_navigation().style('padding: 0; align-items: center;') as qstep_nav:
				for btn in navigation:
					handler = getattr(self, btn['on_click'], None)
					qbtn = Button(text=btn['text'], on_click=handler)\
						.props(self.button_props)\
						.classes(self.button_classes)
					if btn['text']=='Hitung':
						qbtn.bind_enabled_from(self.state, 'is_calculating', lambda state: not state)
					elif btn['text']=='Lanjut' or btn['text']=='Lihat Hasil':
						qbtn.bind_visibility_from(self.state, 'calculated')
				self.calculation_progress()
				ui.icon(name='check_circle_outline', size='sm', color='positive')\
					.bind_visibility_from(self.state, 'calculated')\
					.tooltip('Perhitungan Kinerja OK')
		return qstep

	def render_download(
		self,
		name: str,
		title: str,
		icon: Optional[str] = None,
		navigation: List[Dict[str, str]] = [],
		description: str = ''
	) -> ui.step:
		_icon = 'none' if icon is None else icon
		with ui.step(name=name, title=title, icon=_icon) as qstep:
			ui.label(description)
			with ui.stepper_navigation().style('padding: 0; align-items: center;') as qstep_nav:
				for btn in navigation:
					handler = getattr(self, btn['on_click'], None)
					qbtn = Button(text=btn['text'], on_click=handler)\
						.props(self.button_props)\
						.classes(self.button_classes)
					if btn['text']=='Unduh':
						qbtn.bind_enabled_from(self.state, 'is_exporting', lambda state: not state)
				with ui.row(align_items='center').classes('p-0 m-0 gap-1').bind_visibility_from(self.state, 'is_exporting'):
					ui.spinner('hourglass').props('size=sm thickness=5 color=accent')
					ui.label('Menyiapkan file...').classes('italic text-accent')
		return qstep

	async def handle_auto_next(self) -> None:
		if self.auto_next:
			await asyncio.sleep(self.auto_next_delay)
			self.next()

	async def _handle_calculate(self, e: events.ClickEventArguments) -> None:
		self.state.pre_calculate()
		if not getattr(self.instance, 'analyzed', True): self.state.is_analyzing = True
		self.state.is_calculating = True
		result = await self.instance.async_calculate(force=True)
		self.state.is_analyzing = False
		self.state.is_calculating = False
		self.state.analyzed = getattr(self.instance, 'analyzed', True)
		self.state.calculated = self.instance.calculated
		self.state.result = result
		self.calculation_summary.refresh()
		ui.notify(f'Perhitungan kinerja selesai. ({self.instance.process_duration:.2f}s)', color='positive')
		await self.handle_auto_next()

	async def _handle_preview_result(self, e: events.ClickEventArguments) -> None:
		ui.notify('Sedang dikembangkan, mohon bersabar yaa :)')

	async def _handle_download_result(self, e: events.ClickEventArguments) -> None:
		self.state.is_exporting = True
		content = await run_cpu_bound(self.instance.to_excel, as_iobuffer=True)
		self.state.is_exporting = False
		filename = f'{self.instance.get_xls_filename()}.{self.instance.output_extension}'
		ui.download(src=content, filename=filename)
		await self.handle_auto_next()

	async def _handle_completed(self, e: events.ClickEventArguments) -> None:
		last_step = self.slots['default'].children[0]
		last_step.props('done')

	@ui.refreshable
	def calculation_summary(self) -> None:
		if self.state.calculated:
			with ui.element('div').bind_visibility_from(self.state, 'calculated'):
				ui.html('<strong>Hasil perhitungan :</strong>').classes('py-2')
				with ui.list().props(f'dense bordered separator').classes('w-80 pr-0'):
					for key, val in self.state.result['overall'].items():
						with ui.item():
							with ui.item_section():
								ui.item_label(' '.join(key.split('_')).title()).props('lines=1')
							with ui.item_section().props('side'):
								ui.item_label(val)

	@ui.refreshable
	def calculation_progress(self) -> None:
		with ui.row(align_items='center').classes('p-0 m-0 gap-1').bind_visibility_from(self.state, 'is_calculating'):
			ui.spinner('hourglass').props('size=sm thickness=5 color=accent')
			ui.label('Proses perhitungan...')\
				.bind_text_from(self.instance, 'progress', lambda prg: f'{getattr(prg, "name", "Proses perhitungan")}... ')\
				.classes('italic text-accent')
			ui.label('')\
				.bind_text_from(self.instance, 'progress', lambda prg: f'{getattr(prg, "value", 0.0)*100:.1f}%')\
				.classes('ml-2 italic text-accent')


class FileProcessor(CalculationStepper):
	"""Display component that handle calculation from files.

	Args:
		instance :
		value :
		on_value_change :
		keep_alive :

	Acceptable contexts:

	"""
	instance: FileCalcObjects
	steps_input: List[Dict[str, Any]] = [
		{
			'name': 'setup',
			'title': 'Setup',
			'icon': 'tune',
			'description': 'Persiapkan file yang digunakan dalam perhitungan kinerja.',
			'navigation': [
				{'text': 'Mulai', 'on_click': '_handle_setup'},
			]
		},
		{
			'name': 'upload',
			'title': 'Unggah File',
			'icon': 'upload_file',
			'description': 'Unggah file yang akan digunakan dalam perhitungan kinerja (ekstensi file Excel).',
			'navigation': [
				{'text': 'Pilih File', 'on_click': '_handle_filepicker'},
			]
		},
		{
			'name': 'validate',
			'title': 'Validasi',
			'icon': 'verified',
			'description': 'Validasi data pada file yang telah diunggah.',
			'navigation': [
				{'text': 'Kembali', 'on_click': 'previous'},
				{'text': 'Validasi', 'on_click': '_handle_file_validate'},
				{'text': 'Lanjut', 'on_click': 'next'},
			]
		},
	]

	def __init__(
		self,
		*,
		instance: Optional[Any] = None,
		value: Union[str, ui.step, None] = None,
		on_value_change: Optional[Callable[..., Any]] = None,
		keep_alive: bool = True,
		**contexts
	) -> None:
		super().__init__(instance=instance, value=value, on_value_change=on_value_change, keep_alive=keep_alive, **contexts)
		self.state = FileInputState()
		self.filepicker = self._init_filepicker()
		# self.result = list()

	def _init_filepicker(self) -> FilePicker:
		filepicker = FilePicker(on_file_change=self.update_from_filepicker)\
			.on_value_change(self._handle_filepicker_display_change)
		return filepicker

	def update_from_filepicker(self, *args, **kwargs) -> None:
		attrs = ['files', 'filenames', 'filesizes']
		for attr in attrs:
			setattr(self.state, attr, getattr(self.filepicker, attr))

	def reset(self, *args, **kwargs) -> None:
		self.set_value(self.steps_list[0]['name'])
		self.filepicker.reset()
		self.state.reset()
		self.uploaded_files.refresh()

	def render(self) -> ui.stepper:
		stepper = super().render()
		self.filepicker.render()
		return stepper

	def render_upload(
		self,
		name: str,
		title: str,
		icon: Optional[str] = None,
		navigation: List[Dict[str, str]] = [],
		description: str = ''
	) -> ui.step:
		_icon = 'none' if icon is None else icon
		_ext_button = [
			{'text': 'Reset', 'on_click': '_handle_filepicker_reset'},
			{'text': 'Lanjut', 'on_click': 'next'},
		]
		with ui.step(name=name, title=title, icon=_icon) as qstep:
			ui.label(description)
			self.uploaded_files()
			with ui.stepper_navigation().style('padding: 0; align-items: center;') as qstep_nav:
				for btn in navigation:
					handler = getattr(self, btn['on_click'], None)
					Button(text=btn['text'], on_click=handler)\
						.bind_visibility_from(self.state, 'filecount', lambda c: c==0)\
						.props(self.button_props)\
						.classes(self.button_classes)
				for nbtn in _ext_button:
					handler = getattr(self, nbtn['on_click'], None)
					Button(text=nbtn['text'], on_click=handler)\
						.bind_visibility_from(self.state, 'filecount', lambda c: c>0)\
						.props(self.button_props)\
						.classes(self.button_classes)
		return qstep

	def render_validate(
		self,
		name: str,
		title: str,
		icon: Optional[str] = None,
		navigation: List[Dict[str, str]] = [],
		description: str = ''
	) -> ui.step:
		_icon = 'none' if icon is None else icon
		with ui.step(name=name, title=title, icon=_icon) as qstep:
			ui.label(description)
			self.validation_info()
			with ui.stepper_navigation().style('padding: 0; align-items: center;') as qstep_nav:
				for btn in navigation:
					handler = getattr(self, btn['on_click'], None)
					qbtn = Button(text=btn['text'], on_click=handler)\
						.props(self.button_props)\
						.classes(self.button_classes)
					if btn['text']=='Validasi':
						qbtn.bind_enabled_from(self, 'state', lambda state: not (state.is_loading_file or state.file_isvalid))
					elif btn['text']=='Lanjut':
						qbtn.bind_visibility_from(self.state, 'loaded')\
							.bind_enabled_from(self, 'instance', lambda clc: len(getattr(clc, 'errors', []))==0)
				with ui.row(align_items='center').classes('p-0 m-0 gap-1').bind_visibility_from(self.state, 'is_loading_file'):
					ui.spinner('hourglass').props('size=sm thickness=5 color=accent')
					ui.label('Proses memvalidasi...').classes('italic text-accent')
				ui.icon(name='check_circle_outline', size='sm', color='positive')\
					.bind_visibility_from(self, 'state', lambda state: state.loaded and state.file_isvalid)\
					.tooltip('Validasi OK')
				ui.icon(name='error_outline', size='sm', color='negative')\
					.bind_visibility_from(self, 'instance', lambda clc: len(getattr(clc, 'errors', []))>0)\
					.tooltip('Validasi NOK')
		return qstep

	async def _handle_filepicker_display_change(self, e: events.ValueChangeEventArguments) -> None:
		if e.value:
			self.uploaded_files.refresh()
		else:
			self.uploaded_files.refresh()
			if self.state.filecount:
				# File(s) uploaded
				self.state.file_uploaded = True
				await self.handle_auto_next()
			else:
				# No file uploaded
				self.state.file_uploaded = False

	async def _handle_filepicker_reset(self, e: events.ClickEventArguments) -> None:
		self.filepicker.reset()
		self.instance.initialize()
		# Change state to pre_upload
		self.state.pre_upload()
		self.uploaded_files.refresh()
		self.validation_info.refresh()

	async def _handle_setup(self, e: events.ClickEventArguments) -> None:
		self.state.reset()
		self.calculation_progress.refresh()
		self.next()

	async def _handle_filepicker(self, e: events.ClickEventArguments) -> None:
		self.filepicker.open()

	async def _handle_file_validate(self, e: events.ClickEventArguments) -> None:
		self.instance.set_file(self.state.files)
		if self.state.loaded:
			# File(s) already loaded
			if self.state.file_isvalid:
				ui.notify('File telah tervalidasi. Silahkan lanjutkan proses.', color='positive')
				await self.handle_auto_next()
			else:
				# Some error(s) occured though file(s) already loaded
				pass
		else:
			# If file(s) are not loaded yet, then load
			self.state.pre_validate()
			self.state.is_loading_file = True
			df_load = await self.instance.async_load()
			self.state.is_loading_file = False
			self.state.loaded = self.instance.loaded
			if isinstance(df_load, pd.DataFrame):
				self.state.file_isvalid = len(self.instance.errors)==0
				self.validation_info.refresh()
				ui.notify(f'Validasi file berhasil. (error = {len(self.instance.errors)})', color='positive')
				await self.handle_auto_next()
			else:
				self.validation_info.refresh()
				ui.notify(f'Validasi file gagal. (error = {len(self.instance.errors)})', color='negative')

	@ui.refreshable
	def uploaded_files(self) -> None:
		count = self.state.filecount
		if count:
			with ui.element('div').bind_visibility_from(self.state, 'filecount').classes('w-full'):
				with ui.list().props(f'dense{" bordered" if count else ""} separator').classes('w-full max-h-36 pr-0 overflow-y-auto'):
					for i in range(count):
						with ui.item().props('clickable'):
							with ui.item_section():
								ui.item_label(f'{i+1}. {self.state.filenames[i]}').props('lines=1')
							with ui.item_section().props('side'):
								ui.item_label(f'{self.state.filesizes[i]/10**6:.2f}MB')
				ui.html(f'Total : <strong>{count}</strong> file ({sum(self.state.filesizes)/10**6:.1f}MB)').classes('mt-2')

	@ui.refreshable
	def validation_info(self) -> None:
		warnings = getattr(self.instance, 'warnings', [])
		errors = getattr(self.instance, 'errors', [])
		if self.state.file_uploaded:
			with ui.element('div').classes('w-full gap-1'):
				with ui.expansion(f'warning ({len(warnings)})')\
					.bind_visibility_from(self.instance, 'warnings', lambda wrn: len(wrn)>0)\
					.props('dense dense-toggle header-class="px-2 text-yellow-700" expand-icon=more_horiz expanded-icon=expand_less')\
					.classes('w-full') as warning_info:
					with ui.list().props(f'dense').classes('w-full pr-0 text-sm'):
						for wrn in warnings:
							with ui.item().props('dense clickable'):
								with ui.item_section():
									with ui.row(wrap=False, align_items='center').classes('p-0'):
										ui.icon('report_problem', size='xs', color='warning')
										ui.label(str(wrn))
				with ui.expansion(f'error ({len(errors)})')\
					.bind_visibility_from(self.instance, 'errors', lambda err: len(err)>0)\
					.props('dense dense-toggle header-class="px-2 text-red-700" expand-icon=more_horiz expanded-icon=expand_less')\
					.classes('w-full') as error_info:
					with ui.list().props(f'dense').classes('w-full pr-0 text-sm'):
						for err in errors:
							with ui.item():
								with ui.item_section():
									ui.label(str(err))


class OfdbProcessor(CalculationStepper):
	"""Display component that handle calculation from Offline Database data.

	Args:
		instance :
		value :
		on_value_change :
		keep_alive :

	Acceptable contexts:

	"""
	instance: OfdbCalcObjects
	steps_input: List[Dict[str, Any]] = [
		{
			'name': 'setup',
			'title': 'Setup',
			'icon': 'tune',
			'description': 'Persiapan koneksi ke OFDB untuk pengambilan data kinerja.',
			'navigation': [
				{'text': 'Mulai', 'on_click': '_handle_setup'},
			]
		},
		{
			'name': 'server_check',
			'title': 'Cek Server',
			'icon': 'cable',
			'description': 'Periksa konektivitas dengan server OFDB.',
			'navigation': [
				{'text': 'Cek', 'on_click': '_handle_server_connection'},
				{'text': 'Lanjut', 'on_click': 'next'},
			]
		},
		{
			'name': 'fetch_data',
			'title': 'Ambil Data',
			'icon': 'cloud_download',
			'description': 'Proses pengambilan data dari server OFDB berdasarkan rentang waktu.',
			'navigation': [
				{'text': 'Kembali', 'on_click': 'previous'},
				{'text': 'Ambil Data', 'on_click': '_handle_fetch_data'},
				{'text': 'Lanjut', 'on_click': 'next'},
			]
		},
	]

	def __init__(
		self,
		*,
		instance: Optional[Any] = None,
		value: Union[str, ui.step, None] = None,
		on_value_change: Optional[Callable[..., Any]] = None,
		keep_alive: bool = True,
		**contexts
	) -> None:
		super().__init__(instance=instance, value=value, on_value_change=on_value_change, keep_alive=keep_alive, **contexts)
		self.state = OfdbInputState()

	def reset(self, *args, **kwargs) -> None:
		self.set_value(self.steps_list[0]['name'])
		self.state.reset()
		self.instance.initialize()
		self.instance.reset_date()

	def render_server_check(
		self,
		name: str,
		title: str,
		icon: Optional[str] = None,
		navigation: List[Dict[str, str]] = [],
		description: str = ''
	) -> ui.step:
		_icon = 'none' if icon is None else icon
		with ui.step(name=name, title=title, icon=_icon) as qstep:
			ui.label(description)
			with ui.stepper_navigation().style('padding: 0; align-items: center;') as qstep_nav:
				for btn in navigation:
					handler = getattr(self, btn['on_click'], None)
					qbtn = Button(text=btn['text'], on_click=handler)\
						.props(self.button_props)\
						.classes(self.button_classes)
					if btn['text']=='Cek':
						qbtn.bind_enabled_from(self.state, 'connecting_to_server', lambda x: not x)
					elif btn['text']=='Lanjut':
						qbtn.bind_enabled_from(self.state, 'server_available')
				ui.icon(name='check_circle_outline', size='sm', color='positive')\
					.bind_visibility_from(self, 'state', lambda state: state.initialized and state.server_available)\
					.tooltip('Koneksi server OK')
				ui.icon(name='error_outline', size='sm', color='negative')\
					.bind_visibility_from(self, 'state', lambda state: state.initialized and not state.server_available)\
					.tooltip('Koneksi server NOK')
				ui.spinner('facebook')\
					.bind_visibility_from(self.state, 'connecting_to_server')\
					.props('size=sm thickness=5 color=accent')
		return qstep

	def render_fetch_data(
		self,
		name: str,
		title: str,
		icon: Optional[str] = None,
		navigation: List[Dict[str, str]] = [],
		description: str = ''
	) -> ui.step:
		_icon = 'none' if icon is None else icon
		curr_date = datetime.datetime.now()
		prev_month = curr_date.replace(month=curr_date.month-1) if curr_date.month>1 else curr_date.replace(year=curr_date.year-1, month=12)
		with ui.step(name=name, title=title, icon=_icon) as qstep:
			ui.label(description)
			with ui.row(align_items='center').classes('p-0 m-0 gap-x-8'):
				for name, label in [('from', 'Dari'), ('to', 'Sampai')]:
					with ui.input(f'{label} Tanggal', on_change=getattr(self, 'set_date_' + name))\
						.bind_enabled_from(self.state, 'is_fetching_data', lambda x: not x)\
						.props('dense stack-label hide-hint hint="YYYY-MM-DD" color=teal') as date:
						with ui.menu().props('no-parent-events') as date_menu:
							ui.date(mask='YYYY-MM-DD').props(f'minimal color=teal default-year-month={prev_month.strftime("%Y/%m")}').bind_value(date)
						with date.add_slot('append'):
							ui.icon('edit_calendar').on('click', date_menu.open).classes('cursor-pointer')
			ui.label('*Estimasi pengambilan data 2-5 menit').classes('text-sm italic text-neutral-400').bind_visibility_from(self.state, 'is_fetching_data')
			self.validation_info()
			with ui.stepper_navigation().style('padding: 0; align-items: center;') as qstep_nav:
				for btn in navigation:
					handler = getattr(self, btn['on_click'], None)
					qbtn = Button(text=btn['text'], on_click=handler)\
						.props(self.button_props)\
						.classes(self.button_classes)
					if btn['text']=='Ambil Data':
						qbtn.bind_enabled_from(self, 'instance', lambda clc: getattr(clc, 'date_isset', False) and not self.state.is_fetching_data)
					elif btn['text']=='Lanjut':
						qbtn.bind_enabled_from(self.state, 'fetched')
				with ui.row(align_items='center').classes('p-0 m-0 gap-1').bind_visibility_from(self.state, 'is_fetching_data'):
					ui.spinner('hourglass').props('size=sm thickness=5 color=accent')
					ui.label('').bind_text_from(self.state, 'timer', lambda t: datetime.datetime.fromtimestamp(t).strftime('%M:%S')).classes('pr-2 text-accent')
					ui.label('Proses mengambil data...').classes('italic text-accent')
				ui.icon(name='check_circle_outline', size='sm', color='positive')\
					.bind_visibility_from(self, 'state', lambda state: state.fetched)\
					.tooltip('Pengambilan data berhasil')
				ui.icon(name='error_outline', size='sm', color='negative')\
					.bind_visibility_from(self, 'instance', lambda clc: len(getattr(clc, 'errors', []))>0)\
					.tooltip('Pengambilan data gagal')
		return qstep

	def set_date(self, key: str, value: str) -> None:
		try:
			val = datetime.datetime.strptime(value, '%Y-%m-%d')
		except Exception:
			val = None

		setattr(self.state, 'date_' + key, val)
		# Set date range to instance
		if isinstance(self.state.date_from, datetime.datetime) and isinstance(self.state.date_to, datetime.datetime):
			self.instance.set_date_range(self.state.date_from, self.state.date_to)
		else:
			self.instance._date_isset = False

	def set_date_from(self, e: events.ValueChangeEventArguments) -> None:
		self.set_date('from', e.value)

	def set_date_to(self, e: events.ValueChangeEventArguments) -> None:
		self.set_date('to', e.value)

	async def _handle_setup(self, e: events.ClickEventArguments) -> None:
		self.next()

	async def _handle_server_connection(self, e: events.ClickEventArguments) -> None:
		self.state.pre_communication()
		self.state.connecting_to_server = True
		server_available = await run_cpu_bound(self.instance.check_server)
		self.state.connecting_to_server = False

		if server_available:
			self.state.initialized = True
			ui.notify(f'Berhasil terhubung ke server.', color='positive')
		else:
			ui.notify(f'Gagal menghubungkan ke server.', color='negative')

		self.state.server_available = server_available

	async def _handle_fetch_data(self, e: events.ClickEventArguments) -> None:
		self.state.pre_fetch()
		timer = ui.timer(1, self.state.tick)
		self.state.is_fetching_data = True
		df_fetch = await self.instance.async_fetch_all()
		self.state.is_fetching_data = False

		if isinstance(df_fetch, pd.DataFrame):
			self.state.fetched = True
			self.validation_info.refresh()
			ui.notify(f'Pengambilan data berhasil. (error = {len(self.instance.errors)})', color='positive')
			await self.handle_auto_next()
		else:
			ui.notify(f'Pengambilan data berhasil. (error = {len(self.instance.errors)})', color='negative')

		timer.cancel()
		timer.clear()

	@ui.refreshable
	def validation_info(self) -> None:
		warnings = getattr(self.instance, 'warnings', [])
		errors = getattr(self.instance, 'errors', [])
		if self.state.fetched:
			with ui.element('div').classes('w-full gap-1'):
				with ui.expansion(f'warning ({len(warnings)})')\
					.bind_visibility_from(self.instance, 'warnings', lambda wrn: len(wrn)>0)\
					.props('dense dense-toggle header-class="px-2 text-yellow-700" expand-icon=more_horiz expanded-icon=expand_less')\
					.classes('w-full') as warning_info:
					with ui.list().props(f'dense').classes('w-full pr-0 text-sm'):
						for wrn in warnings:
							with ui.item().props('dense clickable'):
								with ui.item_section():
									with ui.row(wrap=False, align_items='center').classes('p-0'):
										ui.icon('report_problem', size='xs', color='warning')
										ui.label(str(wrn))
				with ui.expansion(f'error ({len(errors)})')\
					.bind_visibility_from(self.instance, 'errors', lambda err: len(err)>0)\
					.props('dense dense-toggle header-class="px-2 text-red-700" expand-icon=more_horiz expanded-icon=expand_less')\
					.classes('w-full') as error_info:
					with ui.list().props(f'dense').classes('w-full pr-0 text-sm'):
						for err in errors:
							with ui.item():
								with ui.item_section():
									ui.label(str(err))


class BaseSettingMenu(ui.list):
	"""Base setting menu"""
	parameters: List[Dict[str, Any]] = list()

	def __init__(self) -> None:
		super().__init__()
		# self.props(add='bordered')
		self.classes('py-2')
		self._params: List[str] = list()
		self._cfg = self._regenerate_params()
		self._changed: bool = False
		self.config_instance: Dict[str, Any] = self.get_configuration()

	def _regenerate_params(self) -> Dict[str, List[Dict[str, Any]]]:
		cfg: Dict[str, List[Dict[str, Any]]] = dict()
		for dcfg in self.parameters:
			group = dcfg.get('config_group', '')
			if group in cfg:
				cfg[group].append(dcfg)
			else:
				cfg[group] = [dcfg]
			self._params.append(dcfg['config_name'].upper())
		return cfg
	
	def get_configuration(self) -> Dict[str, Any]:
		return {param: getattr(config, param) for param in self._params}
	
	def parse_props_from_dict(self, **props: Dict[str, Any]) -> str:
		_props = list()
		for pkey, pval in props.items():
			if type(pval)==str:
				_props.append(f'{pkey}="{pval}"')
			elif type(pval) in (int, float):
				_props.append(f'{pkey}={pval}')
			else:
				_props.append(pkey)
		return ' '.join(_props)
	
	def parse_style_from_dict(self, **styles: Dict[str, Any]) -> str:
		_styles = list()
		for skey, sval in styles.items():
			_styles.append(f'{skey}:{sval}')
		return ';'.join(_styles)

	def get_item_renderer(self, key: str) -> Callable:
		_key = key.replace(' ', '_').replace('-', '_')
		renderer = getattr(self, 'render_' + _key, None)
		return renderer if callable(renderer) else self.default_item_renderer

	def default_item_renderer(self, **kwargs) -> ui.item:
		if 'element' in kwargs:
			element = kwargs['element']
		else:
			element = getattr(ui, kwargs['comp'], ui.input)

		with ui.item().props(f'dense {"tag=label" if kwargs["comp"] in ("input", "select") else ""}') as item:
			self.item_label(**kwargs)
			with ui.item_section().props('side'):
				element(**kwargs.get('comp_kwargs', {}))\
					.bind_value(self.config_instance, kwargs['config_name'].upper())\
					.on_value_change(self._handle_config_change)\
					.props(self.parse_props_from_dict(**kwargs.get('comp_props', {})))\
					.classes(' '.join(kwargs.get('comp_classes', [])))\
					.style(self.parse_style_from_dict(**kwargs.get('comp_style', {})))
		return item

	def item_label(self, **kwargs) -> ui.item_section:
		with ui.item_section() as item_section:
			helper = kwargs.get('description')
			with ui.row(align_items='center').classes('p-0 gap-0'):
				ui.item_label(kwargs['config_label'])
				if helper:
					ui.icon('help_outline', size='xs', color='teal')\
						.classes('pl-3 pb-px')\
						.tooltip(helper).props("anchor='bottom left' self='top left' max-width=8rem")
		return item_section

	def render(self) -> ui.list:
		group_idx = 0
		with self:
			for group, configs in self._cfg.items():
				if group_idx>0: ui.separator().classes('my-2')
				if group: ui.item_label(group.upper()).props('header').classes('font-extrabold')
				for cfg in configs:
					renderer = self.get_item_renderer(cfg['config_name'])
					item = renderer(**cfg)
				group_idx += 1
			with ui.item().props('dense').classes('flex-row-reverse pt-4'):
				with ui.row(align_items='center').classes('p-0 gap-x-0.5'):
					button = Button.default_props('dense no-caps').default_classes('px-2')
					# button('Print', on_click=lambda: print(self.config_instance))
					button('Reset Default', on_click=self._handle_restore_default)
					button('Batal', on_click=self._handle_fetch_config).bind_enabled_from(self, 'changed')
					button('Simpan', on_click=self._handle_save_config).bind_enabled_from(self, 'changed')
		return self

	async def _handle_config_change(self, e: events.ValueChangeEventArguments) -> None:
		self._changed = True

	async def _handle_dark_mode_change(self, e: events.ValueChangeEventArguments) -> None:
		self.config_instance['DARK_MODE'] = e.value
		self._changed = True

	async def _handle_fetch_config(self, e: events.ClickEventArguments) -> None:
		self.config_instance.update(self.get_configuration())
		await asyncio.sleep(0.25)	# Make some delay for event bindings
		self._changed = False

	async def _handle_save_config(self, e: events.ClickEventArguments) -> None:
		try:
			config.save(**self.config_instance)
			ui.notify('Pengaturan berhasil disimpan.', type='positive')
			self._changed = False
		except Exception as err:
			ui.notify(f'Gagal menyimpan pengaturan! {". ".join(err.args)}', type='negative')

	async def _handle_restore_default(self, e: events.ClickEventArguments) -> None:
		try:
			# Load default configuration
			default = {dkey: dval for dkey, dval in config.__DEFAULT__.items() if dkey in self._params}
			# Restore only owned parameters
			self.config_instance.update(default)
			ui.notify('Parameter telah dipulihkan ke pengaturan default.\nSilahkan "Simpan" untuk menerapkan pengaturan.', type='info', multi_line=True, classes='multi-line-notification')
		except Exception as err:
			ui.notify(f'Gagal memulihkan ke pengaturan default! {". ".join(err.args)}', type='negative')

	@property
	def changed(self):
		return self._changed


class DowntimeRulesInput(ValueElement, ui.list):
	""""""

	def __init__(self, value: List[List[Any]], on_change: Optional[Callable[..., Any]] = None) -> None:
		super().__init__(value=value, on_value_change=on_change)
		self.classes('w-full')
		self.value1 = {label: hour for label, hour in value}
		self._render()

	def _render(self) -> ui.list:
		with self:
			for name in self.value1:
				with ui.item().props('dense tag=label') as item:
					# with ui.row(align_items='center').classes('w-full gap-1 justify-between'):
					with ui.item_section():
						ui.item_label(name)
					with ui.item_section().props('side'):
						with ui.input()\
							.bind_value(self.value1, name, forward=lambda x: int(x))\
							.on_value_change(self._handle_value1_change)\
							.props(f'dense square filled color=teal type=number')\
							.classes('md:w-80') as qinput:
							with qinput.add_slot('prepend'):
								ui.label('>').classes('text-sm')
							with qinput.add_slot('append'):
								ui.label('jam').classes('text-sm')
		return self
	
	def _handle_value_change(self, value: Any) -> None:
		super()._handle_value_change(value)
		# Refresh component when value changed
		self.value1 = {label: hour for label, hour in value}
		self.clear()
		self._render()
	
	def _handle_value1_change(self, e: events.ValueChangeEventArguments) -> None:
		self.set_value([[key, val] for key, val in self.value1.items()])


class GeneralSettingMenu(BaseSettingMenu):
	"""General setting menu component"""
	parameters = [
		{
			'config_name': 'dark_mode',
			'config_type': 'bool',
			'config_label': 'Mode Gelap',
			'config_group': '',
			'description': 'Aktifkan atau matikan mode gelap.',
			'comp': 'switch',
			'comp_kwargs': {},
			'comp_props': {
				'color': 'teal-5'
			},
		}
	]

	def render_dark_mode(self, **kwargs) -> ui.item:
		element = getattr(ui, kwargs['comp'], ui.input)
		with ui.item().props(f'dense {"tag=label" if kwargs["comp"] in ("input", "select") else ""}'):
			self.item_label(**kwargs)
			with ui.item_section().props('side'):
				element(**kwargs.get('comp_kwargs', {}))\
					.bind_value(ui.dark_mode(config.DARK_MODE))\
					.on_value_change(self._handle_dark_mode_change)\
					.props(self.parse_props_from_dict(**kwargs.get('comp_props', {})))\
					.classes(' '.join(kwargs.get('comp_classes', [])))\
					.style(self.parse_style_from_dict(**kwargs.get('comp_style', {})))


class OfdbSettingMenu(BaseSettingMenu):
	"""OFDB setting menu component."""
	parameters = config.PARAMETER_OFDB

	def render_ofdb_token(self, **kwargs) -> ui.item:
		def crypto_key():
			secret = os.getenv('PRIVATE_KEY')
			return cryptography.fernet.Fernet(secret)

		def encrypt(pswd: str):
			key = crypto_key()
			return key.encrypt(pswd.encode()).decode()

		def decrypt(token: str):
			key = crypto_key()
			try:
				pwd = key.decrypt(token.encode()).decode()
			except cryptography.fernet.InvalidToken:
				pwd = ''
			return pwd

		element = getattr(ui, kwargs['comp'], ui.input)
		with ui.item().props(f'dense {"tag=label" if kwargs["comp"] in ("input", "select") else ""}') as item:
			self.item_label(**kwargs)
			with ui.item_section().props('side'):
				element(**kwargs.get('comp_kwargs', {}))\
					.bind_value(self.config_instance, kwargs['config_name'].upper(), forward=lambda pwd: encrypt(pwd), backward=lambda pwd: decrypt(pwd))\
					.on_value_change(self._handle_config_change)\
					.props(self.parse_props_from_dict(**kwargs.get('comp_props', {})))\
					.classes(' '.join(kwargs.get('comp_classes', [])))\
					.style(self.parse_style_from_dict(**kwargs.get('comp_style', {})))
		return item


class RCDSettingMenu(BaseSettingMenu):
	"""RCD setting menu component."""
	parameters = config.PARAMETER_RCD


class AVRSSettingMenu(BaseSettingMenu):
	"""AVRS setting menu component."""
	parameters = config.PARAMETER_AVRS

	def render_downtime_rules(self, **kwargs) -> ui.item:
		element = DowntimeRulesInput
		with ui.item().classes('p-0') as item:
			element(**kwargs.get('comp_kwargs', {}))\
				.bind_value(self.config_instance, kwargs['config_name'].upper())\
				.on_value_change(self._handle_config_change)\
				.props(self.parse_props_from_dict(**kwargs.get('comp_props', {})))\
				.classes(' '.join(kwargs.get('comp_classes', [])))\
				.style(self.parse_style_from_dict(**kwargs.get('comp_style', {})))
		return item


####################################################################################################
# NEW DEVELOPED CODE
####################################################################################################


ui_item = ui.item.default_style('padding: 2px 8px;')
ui_section = ui.item_section.default_classes('align-stretch')
ui_select = ui.select.default_props('dense outlined square stack-label options-dense')
ui_input = ui.input.default_props('dense outlined square stack-label')
ui_menu_label = ui.item_label.default_classes('text-md')

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
		with UIRow():
			ui.space()
			if isinstance(self.state.choices, list):
				for ch in self.state.choices:
					fn_click = partial(self.submit, ch)
					Button(ch.title(), on_click=fn_click)\
						.props('dense no-caps')\
						.classes('px-2')
			elif isinstance(self.state.choices, dict):
				for val, text in self.state.choices.items():
					fn_click = partial(self.submit, val)
					Button(text, on_click=fn_click)\
						.props('dense no-caps')\
						.classes('px-2')

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
	_event_callbacks: Dict[str, Callable[..., Any]]
	upload_props: ClassVar[str] = 'bordered hide-upload-btn accept=".xlsx,.xls"'
	upload_classes: ClassVar[str] = ''
	button_props: ClassVar[str] = 'dense round outline size=sm'
	button_classes: ClassVar[str] = ''

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
		self._event_callbacks = {
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
		async def read_buffer(buffers: List[BytesIO]):
			results = list()
			for buf in buffers:
				with buf:
					results.append(BytesIO(buf.read()))
			return results

		iobuffers = await read_buffer(e.contents)
		uploaded = dict(zip(e.names, iobuffers))

		self.state.update_files(uploaded)
		self.trigger_events('uploaded')
		ui.notify(f'{len(e.names)} file berhasil diunggah.', type='positive')
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
		with self, ui.card().classes('p-0 gap-y-0'):
			self._fileupload = ui.upload(
				label='Upload File Excel',
				multiple=True,
				max_files=config.MAX_FILES,
				max_file_size=config.MAX_FILE_SIZE,
				max_total_size=config.MAX_TOTAL_SIZE,
				on_multi_upload=self._handle_uploaded_multiple
			)\
				.props(self.upload_props)\
				.classes(self.upload_classes)\
				.on('added', self._handle_queue_added)\
				.on('removed', self._handle_queue_removed)

			with ui.row().classes('w-full gap-x-2 p-2'):
				ui.space()
				btn_refresh = Button(icon='restart_alt', on_click=self._handle_queue_reset)\
					.props(self.button_props)\
					.classes(self.button_classes)\
					.tooltip('Reset file')
				btn_upload = Button(icon='check_circle_outline', on_click=self._handle_commit_upload)\
					.bind_enabled_from(self.state, 'queue_count')\
					.props(self.button_props)\
					.classes(self.button_classes)\
					.tooltip('Unggah file')

		self.on('show', self._handle_showed)\
			.on('hide', self._handle_hidden)

		return self

	def reset(self, *args, **kwargs) -> None:
		self.state.clear()
		self.trigger_events('uploaded')

	def trigger_events(self, event: str) -> None:
		e = self._event_callbacks.get('on_' + event)
		args = events.ValueChangeEventArguments(client=self.client, sender=self, value=self.state.fileinfo)
		if callable(e):
			e(args)

	def on_uploaded(self, fn: Callable) -> Self:
		self._event_callbacks['on_uploaded'] = fn
		return self


@bindable_dataclass
class PanelState(BaseState):
	is_active: bool = False
	input_source: Literal['SOE', 'OFDB', 'RCD', 'RTU'] = ''
	input_category: Literal['File', 'Database'] = ''
	master: Literal['spectrum', 'survalent'] = 'spectrum'
	is_from_file: bool = False
	is_from_ofdb: bool = False
	date_period: Literal['all', 'monthly', 'specific'] = 'all'
	date_year: int = field(default=datetime.date.today().year)
	date_month: int = -1
	start_date: datetime.date = None
	end_date: datetime.date = None
	uploaded: Optional[bool] = None
	uploaded_fileinfo: UploadedFilesInfo = field(init=False, default=None)
	interlock: InterlockState = field(init=False, default_factory=InterlockState)
	result_visible: bool = False
	setup_visible: bool = True
	progress_visible: bool = False

	def set_input_source(self, value: str):
		print(datetime.datetime.now(), value)
		self.input_source = value
		self.categorize_input(value)
		self.interlock.set_input_source(value)

	def set_master(self, value: str):
		self.master = value

	def set_uploaded(self, value: bool):
		self.uploaded = value
		self.interlock.set_uploaded(value)

	def set_date_year(self, value: int):
		self.date_year = value
		if self.date_month in range(1, 13):
			self.start_date = datetime.date(year=value, month=self.date_month, day=1)
			self.end_date = datetime.date(year=value, month=self.date_month+1, day=1) - datetime.timedelta(days=1)

	def set_date_month(self, value: int):
		self.date_month = value
		if value in range(1, 13):
			self.start_date = datetime.date(year=self.date_year, month=value, day=1)
			self.end_date = datetime.date(year=self.date_year, month=value+1, day=1) - datetime.timedelta(days=1)

	def set_start_date(self, value: datetime.date):
		self.start_date = value
		if isinstance(self.end_date, datetime.date):
			if value.year==self.end_date.year:
				self.date_year = value.year

			if value.month==self.end_date.month:
				self.date_month = value.month
			else:
				self.date_month = -1

	def set_end_date(self, value: datetime.date):
		self.end_date = value
		if isinstance(self.start_date, datetime.date):
			if value.year==self.start_date.year:
				self.date_year = value.year

			if value.month==self.start_date.month:
				self.date_month = value.month
			else:
				self.date_month = -1

	def toggle_visibility(self, name: str):
		attr = name + '_visible'
		if hasattr(self, attr):
			setattr(self, attr, not getattr(self, attr))

	def toggle_master(self):
		self.master = 'survalent' if self.master=='spectrum' else 'spectrum'

	def categorize_input(self, input: str):
		if input in ('SOE', 'RCD', 'RTU'):
			self.input_category = 'File'
		elif input=='OFDB':
			self.input_category = 'Database'
		else:
			self.input_category = ''

		self.is_from_file = bool(self.input_category=='File')
		self.is_from_ofdb = bool(self.input_category=='Database')

	def refresh_state(self, *args):
		print(datetime.datetime.now(), 'state & interlock refreshed')
		self.interlock.set_input_source(self.input_source)
		self.interlock.set_uploaded(self.uploaded)

	def restart_av(self, *args):
		self.uploaded = False
		self.interlock.set_input_source(self.input_source)

	def try_strftime(self, value: Any, format: str = '%Y-%m-%d') -> Optional[str]:
		return try_strftime(value=value, format=format)


class AVTabPanel(ui.tab_panel):
	av: Union[AvRCObject, AvRSObject] = None
	_refreshable: List[ui.refreshable]
	select_input: ui.select = None
	select_master: ui.radio = None
	select_period: ui.radio = None
	button_reset: ui.button = None
	button_fileupload: ui.button = None
	button_fileinfo: ui.button = None
	button_checkdb: ui.button = None
	button_calculate: ui.button = None

	def __init__(
		self,
		name: str,
		options: Dict[str, str] = dict(),
	):
		super().__init__(name=name)
		self._refreshable = list()
		self.name = name
		self.state = PanelState()
		self.av = None
		self.av_state = AvStateWrapper(None)

		with self:
			self.filepicker = FilePickerv2(
				on_uploaded=self.event_file_uploaded,
				on_close=self.event_filepicker_closed
			).render()
			self.dialog_prompt = DialogPrompt()
			with UIColumn(css_gap='gap-1', css_padding='px-0'):
				self.group_label(text='Setup', name='setup')
				self.parameter_setup(options=options)

			self.section_result()
			self.button_calculate = Button('Hitung', on_click=self.do_calculation)\
				.bind_enabled_from(self.state.interlock, 'enable_calculate')\
				.classes('w-full')

	def _init_availability(self) -> Optional[Union[AvRCObject, AvRSObject]]:
		raise ValueError('Must be overidden.')

	def set_active(self, value: bool):
		self.state.is_active = value

	def update_refreshable(self, *args):
		consume(map(lambda comp: comp.refresh(), self._refreshable))

	def group_label(self, text: str, name: str, can_toggle: bool = True) -> ui.element:
		with UIRow(overflow='hidden', gap=2).classes('py-1') as glabel:
			ui.label(text).classes('font-bold whitespace-nowrap')
			if can_toggle:
				ui.button(icon='visibility_off', color='grey', on_click=lambda: self.state.toggle_visibility(name))\
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
					Button('Lihat', icon='read_more')\
						.bind_enabled_from(self.state.interlock, 'enable_download')\
						.props('outline dense size=sm')
					Button('Download', icon='file_download')\
						.bind_enabled_from(self.state.interlock, 'enable_download')\
						.props('outline dense size=sm')
				self.calculation_result()

	def parameter_setup(self, options: Dict[str, str] = dict()):
		event_chain1 = EventChainsWithArgs(chains=(
			self.event_input_source_changed,
			self.wrap_state,
		))
		event_chain2 = EventChainsWithArgs(chains=(
			self.event_master_changed,
			self.wrap_state,
		))
		event_chain3 = EventChainsWithoutArgs(chains=[
			self.event_restart_av,
			self.wrap_state,
			self.update_refreshable,
		])
		with ui.list()\
			.bind_visibility_from(self.state, 'setup_visible')\
			.classes('w-full'):
			with ui_item():
				with ui_section():
					ui_menu_label('Data Input')
				with ui_section():
					self.select_input = ui_select(options=options, on_change=event_chain1)\
						.bind_value_from(self.state, 'input_source')\
						.bind_enabled_from(self.state.interlock, 'enable_change_input')\
						.classes('w-full')
			with ui_item():
				with ui_section():
					ui_menu_label('Master')
				with ui_section():
					with UIRow(overflow='visible').classes('w-full'):
						self.select_master = ui.radio(options=dict(spectrum='Spectrum', survalent='Survalent'), on_change=event_chain2)\
							.bind_value_from(self.state, 'master')\
							.bind_enabled_from(self.state.interlock, 'enable_change_master')\
							.props('dense inline')\
							.classes('text-sm')
			with ui_item():
				with ui_section():
					ui_menu_label('Range Waktu')
				with ui_section().classes('gap-y-3'):
					with UIRow(overflow='visible').classes('w-full'):
						self.select_period = ui.radio(options=dict(all='Semua', monthly='Bulanan', specific='Spesifik'))\
							.bind_value(self.state, 'date_period')\
							.bind_enabled_from(self.state, 'input_source')\
							.props('dense inline')\
							.classes('text-sm')
			with ui_item().bind_visibility_from(self.state, 'date_period', backward=lambda val: val in ('monthly', 'specific')):
				with ui_section():
					ui_menu_label('')
				with ui_section().classes('gap-y-3'):
						with UIRow(overflow='visible')\
							.bind_visibility_from(self.state, 'date_period', value='monthly')\
							.classes('w-full'):
							ui.select(options=[self.state.date_year - x for x in range(5)], on_change=self.event_date_year_changed)\
								.bind_value_from(self.state, 'date_year')
							ui.select(options={-1: '--------', **MONTH_OPTIONS}, on_change=self.event_date_month_changed)\
								.bind_value_from(self.state, 'date_month')\
								.classes('w-full')
						ui_input('Dari', on_change=self.event_start_date_changed)\
							.bind_value_from(self.state, 'start_date', backward=self.state.try_strftime)\
							.bind_visibility_from(self.state, 'date_period', value='specific')\
							.props('type="date"')
						ui_input('Sampai', on_change=self.event_end_date_changed)\
							.bind_value_from(self.state, 'end_date', backward=self.state.try_strftime)\
							.bind_visibility_from(self.state, 'date_period', value='specific')\
							.props('type="date"')
			with ui_item():
				with ui_section():
					ui_menu_label('').bind_text_from(self.state, 'input_category')
				with ui_section():
					with UIRow(overflow='visible')\
						.bind_visibility_from(self.state, 'is_from_file')\
						.classes('w-full'):
						self.button_reset = Button('Reset', color='info', on_click=event_chain3)\
							.bind_visibility_from(self.state, 'uploaded', value=True)\
							.bind_enabled_from(self.state.interlock, 'enable_reset')\
							.props('dense')\
							.classes('w-24 px-2')
						self.button_fileupload = Button('Pilih File', on_click=self.filepicker.open)\
							.bind_visibility_from(self.state, 'uploaded', lambda b: not b)\
							.bind_enabled_from(self.state.interlock, 'enable_upload_file')\
							.props('dense')\
							.classes('w-24 px-2')
						self.button_fileinfo = Button('', on_click=self.show_fileinfo, icon='attach_file', color='teal')\
							.bind_enabled_from(self.state.interlock, 'enable_view_file_list')\
							.props('dense flat')\
							.classes('px-0')
						ui.icon(name='check_circle_outline', size='sm', color='positive')\
							.bind_visibility_from(self.av_state, 'loaded', value=True)\
							.tooltip('Validasi OK')
						ui.icon(name='error_outline', size='sm', color='negative')\
							.bind_visibility_from(self.av_state, 'loaded', value=False)\
							.tooltip('Validasi NOK')
						ui.spinner()\
							.bind_visibility_from(self.av_state, 'loading_file')
					with UIRow(overflow='visible')\
						.bind_visibility_from(self.state, 'is_from_ofdb')\
						.classes('w-full'):
						self.button_checkdb = Button('Cek Koneksi')\
							.bind_enabled_from(self.state, 'is_from_ofdb')\
							.props('dense')\
							.classes('px-2')

	def wrap_state(self, *args):
		print(datetime.datetime.now(), 'state', self.name, 'wrapped')
		av = self._init_availability()
		self.av = av
		self.av_state.wrap(av)

	def fileinfo_content(self):
		props1 = ['dense', 'separator']
		if self.state.uploaded_fileinfo.count:
			props1.append('bordered')

		with ui.element('div').classes('w-full mb-4'):
			with ui.list()\
				.props(get_component_props(*props1))\
				.classes('w-full max-h-36 pr-0 overflow-y-auto'):
				for i in range(self.state.uploaded_fileinfo.count):
					with ui.item().props('clickable'):
						with ui.item_section():
							ui.item_label(f'{i+1}. {self.state.uploaded_fileinfo.filenames[i]}').props('lines=1')
						with ui.item_section().props('side'):
							ui.item_label(f'{self.state.uploaded_fileinfo.filesizes[i]/10**6:.2f}MB')
			# Summary statement
			ui.html(f'Total : <strong>{self.state.uploaded_fileinfo.count}</strong> file ({self.state.uploaded_fileinfo.total_size/10**6:.1f}MB)').classes('mt-2')

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
			ui.icon('date_range', size='sm', color='teal').classes('mr-2')
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
								ui_menu_label(param[1]).classes('px-2')

	def get_result_kwargs(self) -> Dict[str, Any]:
		return dict(params=[])

	@ui.refreshable_method
	def calculation_result(self):
		self.render_result_table(**self.get_result_kwargs())

	def event_input_source_changed(self, e: events.ValueChangeEventArguments):
		self.state.set_input_source(e.value)

	def event_master_changed(self, e: events.ValueChangeEventArguments):
		self.state.set_master(e.value)

	def event_date_year_changed(self, e: events.ValueChangeEventArguments):
		self.state.set_date_year(e.value)

	def event_date_month_changed(self, e: events.ValueChangeEventArguments):
		self.state.set_date_month(e.value)

	def event_start_date_changed(self, e: events.ValueChangeEventArguments):
		# NOTE : Value received here is string
		try:
			self.state.set_start_date(datetime.date.fromisoformat(e.value))
		except (TypeError, ValueError):
			self.state.set_start_date(None)

	def event_end_date_changed(self, e: events.ValueChangeEventArguments):
		# NOTE : Value received here is string
		try:
			self.state.set_end_date(datetime.date.fromisoformat(e.value))
		except (TypeError, ValueError):
			self.state.set_end_date(None)

	def event_file_uploaded(self, e: events.ValueChangeEventArguments):
		fileinfo: UploadedFilesInfo = e.value
		self.state.uploaded_fileinfo = fileinfo
		self.state.set_uploaded(bool(fileinfo.count))
		if not self.av is None:
			self.av.filereader.set_file(fileinfo.files)

	async def event_filepicker_closed(self):
		if self.state.uploaded and self.av_state.loaded is None:
			await self.do_validate_file()

	async def show_fileinfo(self):
		content = self.fileinfo_content
		self.dialog_prompt.set(
			title='File Input',
			message=content,
			choices={'ok': 'OK'}
		)
		result = await self.dialog_prompt

	def event_restart_av(self, *args):
		self.state.restart_av(*args)
		self.av_state.reset()

	@toggle_attr('state.progress_visible', True, False)
	async def do_validate_file(self):
		self.state.interlock.set_loading(True)
		await self.av.async_load()
		self.state.interlock.set_loading(False)
		self.state.interlock.set_loaded(self.av_state.loaded)

	@toggle_attr('state.progress_visible', True, False)
	async def do_calculation(self):
		await self.av.async_calculate()
		self.calculation_result.refresh()
		self.state.interlock.set_calculated(self.av.calculated)
		await asyncio.sleep(1)
		self.state.result_visible = True


class RCDTabPanel(AVTabPanel):
	av: AvRCObject

	def _init_availability(self) -> Optional[AvRCObject]:
		classes = dict(
			SOE=None,
			OFDB=None,
			RCD=None,
		)
		cls = classes.get(self.state.input_source)
		instance = None if cls is None else cls(server=self.state.master)
		return instance

	def get_result_kwargs(self):
		return dict(
			params=[
				('RC Total', rgetattr(self, 'av.stats.total_count', 0)),
				('RC Total (valid)', rgetattr(self, 'av.stats.total_valid', 0)),
				('Repetisi RC', rgetattr(self, 'av.stats.total_reps', 0)),
				('RC Sukses', rgetattr(self, 'av.stats.total_success', 0)),
				('RC Gagal', rgetattr(self, 'av.stats.total_failed', 0)),
				('Rasio Sukses', f'{rgetattr(self, "av.stats.success_ratio", 0)*100:.2f}%'),
				('Sukses Close', rgetattr(self, 'av.stats.total_success_close', 0)),
				('Gagal Close', rgetattr(self, 'av.stats.total_failed_close', 0)),
				('Rasio Sukses Close', f'{rgetattr(self, "av.stats.success_close_ratio", 0)*100:.2f}%'),
				('Sukses Open', rgetattr(self, 'av.stats.total_success_open', 0)),
				('Gagal Open', rgetattr(self, 'av.stats.total_failed_open', 0)),
				('Rasio Sukses Open', f'{rgetattr(self, "av.stats.success_open_ratio", 0)*100:.2f}%'),
			],
			date_start=rgetattr(self, 'av.stats.date_min', None),
			date_end=rgetattr(self, 'av.stats.date_max', None),
		)
	

class RTUTabPanel(AVTabPanel):
	av: AvRSObject

	def _init_availability(self) -> Optional[AvRSObject]:
		classes = dict(
			# 
		)
		cls = classes.get(self.state.input_source)
		instance = None if cls is None else cls(server=self.state.master)
		return instance
