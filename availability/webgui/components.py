import asyncio, datetime, os
from io import BytesIO
from typing import Any, Dict, List, Callable, Literal, Optional, Tuple, TypeAlias, Union

import cryptography.fernet
import pandas as pd
from nicegui import events, ui
from nicegui.elements.mixins.value_element import ValueElement

from .state import CalculationState, FileInputState, OfdbInputState
from .. import config
from ..core.avrs import AVRSFromOFDB, AVRSFromFile, AVRSCollective
from ..core.rcd import RCDFromOFDB, RCDFromFile, RCDFromFile2, RCDCollective
from ..utils.worker import run_cpu_bound


FileCalcObjects: TypeAlias = Union[AVRSFromFile, AVRSCollective, RCDFromFile, RCDFromFile2, RCDCollective]
OfdbCalcObjects: TypeAlias = Union[AVRSFromOFDB, RCDFromOFDB]
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
		object: Optional[Any] = None,
		*,
		caption: Optional[str] = None,
		icon: Optional[str] = None,
		group: Optional[str] = None,
		value: bool = False,
		on_value_change: Optional[Callable[..., Any]] = None,
		**contexts
	) -> None:
		title = text if text else repr(object)
		super().__init__(text=title, caption=caption, icon=icon, group=group, value=value, on_value_change=on_value_change)
		self.__used__ = set([attr for attr in dir(ui.expansion) if not (attr.startswith('_') and attr.endswith('_'))])
		self._object = object
		self.props(add='dense')
		self.classes('w-full')
		if 'included' not in contexts: self.included = list()
		if 'excluded' not in contexts: self.excluded = list()
		# Add custom attribute
		for key in contexts:
			if not key.startswith('_') and key not in self.__used__: setattr(self, key, contexts[key])

	def render(self) -> ui.expansion:
		with self:
			with ui.grid(columns='auto auto').classes('w-full gap-0'):
				for attr in dir(self._object):
					if not attr.startswith('__') or (attr in self.included and attr not in self.excluded):
						ui.label(attr).classes('border')
						ui.label('').classes('border').bind_text_from(self._object, attr, lambda x: repr(x) if callable(x) else str(x))
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