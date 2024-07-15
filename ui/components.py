import asyncio
from io import BytesIO
from typing import Any, Dict, List, Callable, Optional, Tuple, TypeAlias, Union

import pandas as pd
from avrs import AVRSFromOFDB, AVRSFromFile, AVRSCollective
from rcd import RCDFromOFDB, RCDFromFile, RCDFromFile2, RCDCollective
from lib import rgetattr
from nicegui import ui, events
from worker import BackgroundWorker, run_cpu_bound
from .state import FileProcessorState


FileCalcObjects: TypeAlias = Union[AVRSFromFile, AVRSCollective, RCDFromFile, RCDFromFile2, RCDCollective]
OfdbCalcObjects: TypeAlias = Union[AVRSFromOFDB, RCDFromOFDB]
CalcObjects: TypeAlias = Union[FileCalcObjects, OfdbCalcObjects]


class MenuTitle(ui.label):
	"""
	"""

	def __init__(self, text: str = '') -> None:
		super().__init__(text)
		self._classes = ['text-h5', 'font-extrabold', 'w-full', 'text-teal-700']


class MenuSubtitle(ui.label):
	"""
	"""

	def __init__(self, text: str = '') -> None:
		super().__init__(text)
		self._classes = ['text-subtitle1', 'font-bold', 'w-full', 'text-teal-400']


class Button(ui.button):
	"""Extended nicegui ui.button.
	Add contexts into button element attributes.
	"""
	__used__: set = {'add_resource', 'add_slot', 'bind_enabled', 'bind_enabled_from', 'bind_enabled_to', 'bind_text', 'bind_text_from', 'bind_text_to', 'bind_visibility', 'bind_visibility_from', 'bind_visibility_to', 'classes', 'clear', 'clicked', 'client', 'component', 'default_classes', 'default_props', 'default_slot', 'default_style', 'delete', 'disable', 'enable', 'enabled', 'exposed_libraries', 'extra_libraries', 'id', 'ignores_events_when_disabled', 'ignores_events_when_hidden', 'is_deleted', 'is_ignoring_events', 'libraries', 'move', 'on', 'on_click', 'parent_slot', 'props', 'remove', 'run_method', 'set_enabled', 'set_text', 'set_visibility', 'slots', 'style', 'tag', 'tailwind', 'text', 'tooltip', 'update', 'visible'}

	def __init__(self,
		text: str = '',
		*,
		on_click: Union[Callable[..., Any], None] = None,
		color: Union[str, None] = 'primary',
		icon: Union[str, None] = None,
		**contexts
	) -> None:
		super().__init__(text, on_click=on_click, color=color, icon=icon)
		# Add custom attribute
		for key in contexts:
			if not key.startswith('_') and key not in self.__used__: setattr(self, key, contexts[key])


class _Stepper(ui.stepper):
	"""
	"""
	__used__: set = {'add_resource', 'add_slot', 'bind_value', 'bind_value_from', 'bind_value_to', 'bind_visibility', 'bind_visibility_from', 'bind_visibility_to', 'classes', 'clear', 'component', 'default_classes', 'default_props', 'default_style', 'delete', 'exposed_libraries', 'extra_libraries', 'is_deleted', 'is_ignoring_events', 'libraries', 'move', 'next', 'on', 'on_value_change', 'previous', 'props', 'remove', 'run_method', 'set_value', 'set_visibility', 'style', 'tooltip', 'update', 'value', 'visible'}
	step_contents: List[Dict[str, Any]] = list()
	button_classes: str = 'px-2'
	button_props: str = 'dense no-caps size=md'
	auto_next: bool = True
	auto_next_delay: float = 1.5

	def __init__(self,
		*,
		value: Union[str, ui.step, None] = None,
		on_value_change: Optional[Callable[..., Any]] = None,
		keep_alive: bool = True,
		**contexts
	) -> None:
		super().__init__(value=value, on_value_change=on_value_change, keep_alive=keep_alive)

		self.worker = BackgroundWorker()
		self._step_objects = dict()
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
			for step in self.step_contents:
				renderer = self.get_renderer(step['name'])
				qstep = renderer(**step)
				self._step_objects.update({step['name']: qstep})
		return self

	def reset(self, *args, **kwargs) -> None:
		pass


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
			self._fileupload = ui.upload(label='Upload File Excel', multiple=True, on_multi_upload=self._handle_uploaded_multiple)\
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


class FileProcessor(_Stepper):
	"""
	"""
	instance: FileCalcObjects
	step_contents: List[Dict[str, Any]] = [
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
		{
			'name': 'calculate',
			'title': 'Hitung Kinerja',
			'icon': 'calculate',
			'description': 'Analisa dan lakukan proses perhitungan kinerja.',
			'navigation': [
				{'text': 'Kembali', 'on_click': 'previous'},
				{'text': 'Hitung', 'on_click': '_handle_calculate'},
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
				{'text': 'Lihat Hasil', 'on_click': '_handle_preview_result'},
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

	def __init__(self,
		*,
		value: Union[str, ui.step, None] = None,
		on_value_change: Optional[Callable[..., Any]] = None,
		keep_alive: bool = True,
		**contexts
	) -> None:
		super().__init__(value=value, on_value_change=on_value_change, keep_alive=keep_alive, **contexts)
		self.state = FileProcessorState()
		self.filepicker = self._init_filepicker()
		self.instance = None
		self.result = list()

	def _init_filepicker(self) -> FilePicker:
		filepicker = FilePicker(on_file_change=self.update_from_filepicker)\
			.on_value_change(self._handle_filepicker_display_change)
		return filepicker

	def update_from_filepicker(self, *args, **kwargs) -> None:
		attrs = ['files', 'filenames', 'filesizes']
		for attr in attrs:
			setattr(self.state, attr, getattr(self.filepicker, attr))

	def get_result(self, result: Any):
		self.result.append(result)

	def reset(self, *args, **kwargs) -> None:
		self.set_value(self.step_contents[0]['name'])
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
					elif btn['text']=='Lanjut':
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
							with ui.item():
								with ui.item_section():
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


class OfdbProcessor(_Stepper):
	"""
	"""
	step_contents: List[Dict[str, Any]] = [
		{
			'name': 'setup',
			'title': 'Setup',
			'icon': 'tune',
			'description': 'Tahap persiapan.',
			'navigation': [
				{'text': 'Mulai', 'on_click': '_handle_setup'},
			]
		},
		{
			'name': 'connection_check',
			'title': 'Koneksi Server',
			'icon': 'cable',
			'description': 'Periksa konektivitas perangkat dengan server OFDB.',
			'navigation': [
				{'text': 'Cek', 'on_click': '_handle_server_connection'},
			]
		},
		{
			'name': 'query_data',
			'title': 'Query Data',
			'icon': 'cloud_download',
			'description': 'Tahap proses query data dari server OFDB.',
			'navigation': [
				{'text': 'Query', 'on_click': '_handle_query_data'},
				{'text': 'Lihat Data', 'on_click': '_handle_preview_query'},
			]
		},
		{
			'name': 'calculate',
			'title': 'Hitung Availability',
			'icon': 'calculate',
			'description': 'Tahap analisa dan perhitungan availability.',
			'navigation': [
				{'text': 'Hitung', 'on_click': '_handle_calculate'},
			]
		},
		{
			'name': 'download',
			'title': 'Unduh Hasil',
			'icon': 'file_download',
			'description': 'Export hasil kalkulasi kedalam bentuk file Excel?',
			'navigation': [
				{'text': 'Unduh', 'on_click': '_handle_download_result'},
				{'text': 'Lihat Hasil', 'on_click': '_handle_preview_result'},
			]
		},
		{
			'name': 'completed',
			'title': 'Selesai',
			'icon': 'done_all',
			'description': 'Selamat! Proses perhitungan availability telah selesai.',
			'navigation': [
				{'text': 'Kembali', 'on_click': 'previous'},
				{'text': 'Selesai', 'on_click': '_handle_completed'},
			]
		},
	]

	def reset(self, *args, **kwargs) -> None:
		self.set_value(self.step_contents[0]['name'])

	async def _handle_setup(self, e: events.ClickEventArguments) -> None:
		self.next()

	async def _handle_server_connection(self, e: events.ClickEventArguments) -> None:
		pass

	async def _handle_query_data(self, e: events.ClickEventArguments) -> None:
		pass

	async def _handle_preview_query(self, e: events.ClickEventArguments) -> None:
		pass

	async def _handle_calculate(self, e: events.ClickEventArguments) -> None:
		pass

	async def _handle_preview_result(self, e: events.ClickEventArguments) -> None:
		pass

	async def _handle_download_result(self, e: events.ClickEventArguments) -> None:
		pass

	async def _handle_completed(self, e: events.ClickEventArguments) -> None:
		pass