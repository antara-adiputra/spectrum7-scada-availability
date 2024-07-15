import asyncio, time
from io import BytesIO
from typing import Any, Dict, List, Callable, Generator, Literal, Tuple, TypeAlias, Union

from nicegui import app, events, ui
from lib import nested_dict
from avrs import AVRSFromOFDB, AVRSFromFile, AVRSCollective
from rcd import RCDFromOFDB, RCDFromFile, RCDFromFile2, RCDCollective
from lib import rgetattr
from worker import BackgroundWorker, run_cpu_bound
from .components import Button, MenuSubtitle, MenuTitle, FileProcessor, OfdbProcessor
from .state import MenuState, State


CalcOutputGen: TypeAlias = Generator[float, dict, dict]		# (percentage, state, result)


AVRCD_MENU: list[dict[str, Any]] = [
	{
		'id': 'ofdb',
		'label': 'Dari DB Offline',
		'description': 'Menganalisa dan melakukan perhitungan availability RCD menggunakan data dari database Offline.',
		'input_type': 'database',
		'calculation_handler': RCDFromOFDB
	},
	{
		'id': 'spectrum',
		'label': 'Dari File SOE (Spectrum)',
		'description': 'Menganalisa dan melakukan perhitungan availability RCD dari file SOE Spectrum.',
		'input_type': 'file',
		'calculation_handler': RCDFromFile
	},
	{
		'id': 'survalent',
		'label': 'Dari File SOE (Survalent)',
		'description': 'Menganalisa dan melakukan perhitungan availability RCD dari file SOE Survalent.',
		'input_type': 'file',
		'calculation_handler': RCDFromFile2
	},
	{
		'id': 'accumulative',
		'label': 'Rangkum File Availability',
		'description': 'Menghitung akumulasi availability RCD dari beberapa file availability RCD.',
		'input_type': 'file',
		'calculation_handler': RCDCollective
	}
]
AVRTU_MENU: list[dict[str, Any]] = [
	{
		'id': 'ofdb',
		'label': 'Dari DB Offline',
		'description': 'Menganalisa dan melakukan perhitungan availability Link & Remote Station menggunakan data dari database Offline.',
		'input_type': 'database',
		'calculation_handler': AVRSFromOFDB
	},
	{
		'id': 'spectrum',
		'label': 'Dari File SOE (Spectrum)',
		'description': 'Menganalisa dan melakukan perhitungan availability Link & Remote Station dari file SOE Spectrum.',
		'input_type': 'file',
		'calculation_handler': AVRSFromFile
	},
	{
		'id': 'accumulative',
		'label': 'Rangkum File Availability',
		'description': 'Menghitung akumulasi availability Link & Remote Station dari beberapa file availability Remote Station.',
		'input_type': 'file',
		'calculation_handler': AVRSCollective
	}
]
MAIN_MENU: list[dict[str, Any]] = [
	{
		'id': 'avrcd',
		'label': 'Remote Control',
		'description': 'Perhitungan Keberhasilan Remote Control (RCD) SCADA',
		'submenu': AVRCD_MENU
	},
	{
		'id': 'avrs',
		'label': 'Remote Station',
		'description': 'Perhitungan Availability Remote Station (RTU) SCADA',
		'submenu': AVRTU_MENU
	}
]


class WebGUI(ui.card):
	tabmenus: ui.tabs
	tabpanels: ui.tab_panels
	menus_dict: dict = MAIN_MENU
	calculation_handlers: dict = dict()
	base_classes: str = 'w-96 mx-auto'

	def __init__(self, cache: Any = None, accordion_menu: bool = True, default_menu: str = 'avrcd') -> None:
		super().__init__()

		self.loop = asyncio.get_running_loop()
		self.worker = BackgroundWorker()
		self.cache = app.storage.client if cache is None else cache
		self.accordion_menu = accordion_menu
		# Initialize display
		self._init_dialog()
		self._create_tab_menu()
		self.tabpanels.set_value(default_menu)

	def _create_control_btn(self, type: Literal['file', 'database'], id: str = ''):
		btn_props = 'dense no-caps'
		btn_class = 'px-2'
		if type=='file':
			row = ui.row().classes('flex-row-reverse')
			with row:
				Button('Hitung', on_click=self.commit_calculation).bind_enabled_from(self, 'cache', backward=lambda cache: cache.get('enable_calculation')==id)\
					.props(btn_props)\
					.classes(btn_class)
				Button('Pilih File', identifier=id, on_click=self.handle_open_filedialog)\
					.props(btn_props)\
					.classes(btn_class)
			return row
		elif type=='database':
			row = ui.row().classes('flex-row-reverse')
			with row:
				btn_get = Button('Ambil Data', identifier=id, on_click=increment_count)\
					.props(btn_props)\
					.classes(btn_class)
				Button('Cek Server', on_click=btn_get.enable)\
					.props(btn_props)\
					.classes(btn_class)
			btn_get.disable()
			return row
		else:
			return

	def _create_tab_menu(self, **kwargs):
		with self.classes(self.base_classes):
			self.tabmenus = ui.tabs()\
				.props('active-color=teal')\
				.classes('w-full')
			with self.tabmenus:
				for menu in self.menus_dict:
					ui.tab(name=menu['id'], label=menu['label'])
			self.tabpanels = ui.tab_panels(self.tabmenus, on_change=self.handle_tab_change)\
				.props('transition-duration=0')\
				.classes('w-full')
			with self.tabpanels:
				for menu in self.menus_dict:
					menu_id = menu['id']
					self.calculation_handlers[menu_id] = dict()
					with ui.tab_panel(menu_id).classes('p-0 overflow-hidden'):
						ui.label(menu['description']).classes('w-full font-medium text-center text-teal-900')
						with ui.list()\
							.props('separator')\
							.classes('w-full border rounded-borders overflow-y-auto'):
							for submenu in menu['submenu']:
								submenu_id = submenu['id']
								self.calculation_handlers[menu_id][submenu_id] = submenu['calculation_handler']
								group = menu_id if self.accordion_menu else submenu_id
								with ui.item().classes('p-0'):
									with ui.expansion(group=group, on_value_change=self.handle_expansion_change)\
										.props('duration=600 expand-icon-class="p-0"')\
										.classes('w-full font-bold text-teal-500') as expand_menu:
										with expand_menu.add_slot('header'):
											# Override header element
											with ui.row().classes('w-full gap-x-1 p-0 items-center font-bold text-teal-500'):
												ui.label(submenu['label'])
												ui.button(icon='help_outline', on_click=None)\
													.props('flat round size=xs padding="none"')\
													.tooltip('Info belum tersedia')
										with ui.column().classes('font-normal'):
											ui.label(submenu['description']).classes('text-teal-900')
											self._create_control_btn(type=submenu['input_type'], id=f'{menu_id}__{submenu_id}')

			ui.circular_progress(min=0, max=100).props('thicknes=0.4 color=green')\
				.bind_value_from(self.worker, 'progress', lambda x: x*100)\
				.bind_visibility_from(self.worker, 'is_running')

	def _init_dialog(self, **kwargs):
		self.filedialog = ui.dialog()
		with self.filedialog, ui.card().classes('p-0 gap-y-0'):
			self.fileupload = ui.upload(label='Upload File Excel', multiple=True, on_multi_upload=self.handle_fileupload_multiple)\
				.props('bordered hide-upload-btn accept=".xlsx,.xls" color=teal')
			with ui.row().classes('flex-row-reverse w-full gap-x-2 p-2'):
				Button(icon='check_circle_outline', on_click=self.commit_upload)\
					.props('dense round outline size=sm')\
					.tooltip('Upload file')
				Button(icon='restart_alt', on_click=self.fileupload.reset)\
					.props('dense round outline size=sm')\
					.tooltip('Reset file')

	async def commit_upload(self):
		self.cache.pop('enable_calculation', None)
		self.notif = ui.notification(message='Mengunggah file...', spinner=True, timeout=None)
		self.fileupload.run_method('upload')

	async def commit_calculation(self):
		self.notif = ui.notification(message='Membuka file...', spinner=True, timeout=None)
		instance = self.cache['object_instance']
		df_load, t0 = await run_cpu_bound(fn=instance.load)
		instance.post_load(df_load)
		self.notif.message = f'Melakukan perhitungan...'
		await self.worker.run(instance.fast_analyze_generator)
		self.notif.message = f'Selesai. ({t0:.2f}s)'
		self.notif.spinner = False
		await asyncio.sleep(3)
		self.notif.dismiss()

	def run_load_file(self, handler: Union[RCDFromFile, RCDFromFile2, RCDCollective, RCDFromOFDB, AVRSFromFile, AVRSCollective, AVRSFromOFDB], files: dict[str, BytesIO]):
		try:
			obj = handler
			instance = obj(files=files)
		except Exception as e:
			ui.notify('Error: Terjadi error pada proses perhitungan.\r\n'+e.args[0], type='negative', position='bottom-left')
			raise e
		return instance

	def handle_expansion_change(self, e: events.ValueChangeEventArguments):
		if e.value:
			self.cache['expanded_menu'] = e.sender

	def handle_tab_change(self, e: events.ValueChangeEventArguments):
		expanded_menu = self.cache.get('expanded_menu')
		if isinstance(expanded_menu, ui.expansion):
			# Restore menu state
			expanded_menu.set_value(False)
			del self.cache['expanded_menu']

	def handle_open_filedialog(self, e: events.ClickEventArguments):
		search_attr = 'identifier'
		selected_menu = getattr(e.sender, search_attr, '')
		if selected_menu:
			keys = selected_menu.split('__')
			self.cache.update({'selected_menu': keys, 'selected_handler': nested_dict(self.calculation_handlers, keys, None)})
			self.filedialog.open()
		else:
			self.cache.update({'selected_menu': None, 'selected_handler': None})
			ui.notify(f'"{search_attr.title()}" tidak valid.', type='warning')

	async def handle_fileupload(self, e: events.UploadEventArguments):
		ui.notify(f'{e.name} berhasil diupload.', type='positive', position='bottom-left')

	async def handle_fileupload_multiple(self, e: events.MultiUploadEventArguments):
		async def read_buffer(buffers: list):
			results = list()
			for buf in buffers:
				with buf:
					results.append(BytesIO(buf.read()))
			return results

		iobuffers = await read_buffer(e.contents)
		uploaded_files = dict(zip(e.names, iobuffers))
		cls = self.cache['selected_handler']
		instance = cls(uploaded_files)
		self.cache.update({'enable_calculation': '__'.join(self.cache['selected_menu']), 'object_instance': instance})

		ui.notify(f'{len(e.names)} file berhasil diunggah.', type='positive')
		self.notif.message = 'Selesai.'
		self.notif.spinner = False
		await asyncio.sleep(2)
		self.notif.dismiss()
		self.filedialog.close()

	@staticmethod
	def callback_process(value: float, name: str = '', **kwargs):
		# self.notif.message = f'{name} {value:.0f}%'
		ui.notify(message=f'{name} {value:.0f}%')
		# print(f'{name} {value*100:.0f}%')


class Menu:
	_menus: dict = MAIN_MENU

	def __init__(self) -> None:
		self._active: List = list()
		self._event_callbacks: Dict[str, Callable] = dict()
		self._ischanged: bool = False
		self.menus = self._init_menus()
		self.state = MenuState()

	def _init_menus(self):
		_dict = dict()
		for menu in self._menus:
			id1 = menu['id']
			_dict[id1] = menu.copy()
			_subdict = dict()
			for submenu in menu['submenu']:
				id2 = submenu['id']
				_subdict[id2] = submenu.copy()
			_dict[id1]['submenu'] = _subdict
		return _dict

	def update(self):
		with ui.list().classes('w-full overflow-y-scroll') as menu:
			for item in self._menus:
				item_id = item['id']
				with ui.item().classes('p-0'):
					with ui.expansion(text=item['label'], on_value_change=self._handle_menu_expansion)\
						.props('group=menu expand-icon-class="p-0" content-inset-level=0.5')\
						.classes('sidebar-menu w-full font-bold text-teal-500') as expand_item:
						expand_item.identifier = item_id
						with ui.element('div'):
							for subitem in item['submenu']:
								subitem_id = subitem['id']
								Button(subitem['label'], identifier=f'{item_id}__{subitem_id}', on_click=self._handle_submenu_click)\
									.props(f'flat no-wrap no-caps {"outline" if (item_id, subitem_id)==self.active else ""} align=left')\
									.classes('w-full')
		return menu

	def update_state(self):
		menu_id, submenu_id = self.active
		sel_menu = self.menus[menu_id]
		sel_submenu = sel_menu['submenu'][submenu_id]
		self.state.update(
			title=sel_menu['label'],
			subtitle=sel_submenu['label'],
			description=sel_submenu['description'],
			input_type=sel_submenu['input_type'],
			calculation_handler=sel_submenu['calculation_handler']
		)

	def on_change(self, fn: Callable) -> None:
		self._event_callbacks['on_change'] = fn

	def trigger_events(self, event: str):
		cb = self._event_callbacks.get('on_' + event)
		args = events.ValueChangeEventArguments(client=None, sender=self, value=self.active)
		if callable(cb):
			if asyncio.iscoroutinefunction(cb):
				loop = asyncio.get_running_loop()
				loop.create_task(cb(args))
			else:
				cb(args)

	def _handle_menu_expansion(self, e: events.ValueChangeEventArguments):
		pass

	def _handle_submenu_click(self, e: events.ClickEventArguments):
		id = getattr(e.sender, 'identifier', '')
		if id:
			if id.split('__')==self._active:
				self._ischanged = False
			else:
				self._active = id.split('__')
				self.update_state()
				self._ischanged = True
				self.trigger_events('change')

	@property
	def active(self):
		return tuple(self._active)

	@property
	def is_changed(self):
		return self._ischanged


class WebGUIv2(ui.card):
	components: Dict[str, Dict[str, ui.element]]
	base_classes: str = 'w-full max-w-[64rem] h-[32rem] mx-auto p-0'

	def __init__(self, cache: Any = None, accordion_menu: bool = True, default_menu: str = 'avrcd') -> None:
		super().__init__()

		self.menu = Menu()
		self.state = State()
		self.cache = app.storage.client if cache is None else cache
		self.accordion_menu = accordion_menu
		self.components = dict()
		# Initialize display
		self.main()

	def main(self, **kwargs):
		with self.classes(self.base_classes):
			with ui.row(wrap=False).classes('w-full h-full p-0'):
				with ui.column().classes('w-96 h-full p-3 border-r-2 bg-sky-50') as sidemenu:
					ui.label('APLIKASI PERHITUNGAN KINERJA FASOP').classes('w-full text-3xl font-extrabold text-teal-800')
					ui.separator().classes('m-0')
					# MENU
					self.menu.update()
				with ui.column().classes('w-full h-full p-3'):
					with ui.element('div').classes('w-full gap-0') as div_title:
						MenuTitle('<Title>').bind_text_from(self.menu.state, 'title')
						MenuSubtitle('<Subtitle>').bind_text_from(self.menu.state, 'subtitle')
						ui.separator().classes('my-2').bind_visibility_from(self.menu.state, 'title')
					with ui.element('div').classes('w-full') as div_description:
						ui.label('<Content description>').bind_text_from(self.menu.state, 'description')
					# CONTENT
					self.create_process_display()
		self.menu.on_change(self._handle_menu_change)
		with ui.dialog().props('persistent') as debug, ui.card().classes('w-1/2 md:w-full p-0 gap-y-0'):
			with ui.element('div').classes('w-full border overflow-y-scroll'):
				self.create_debug_table('stepper', self.components['stepper']['file'])
				self.create_debug_table('stepper.filepicker', self.components['stepper']['file'].filepicker)
				self.create_debug_table('stepper.state', self.components['stepper']['file'].state)
				self.debug_calculator()
			with ui.row(align_items='center').classes('w-full p-2 gap-1'):
				ui.space()
				Button(icon='refresh', on_click=self.debug_calculator.refresh).props('dense size=sm')
				Button(icon='close', on_click=debug.close).props('dense size=sm')
		Button(icon='open_in_full', on_click=debug.open).props('dense size=xs').classes('absolute top-1.5 right-1.5')

	def create_debug_table(self, title: str, object: object) -> ui.element:
		with ui.expansion(title, group='debug').props('dense').classes('w-full') as dbg:
			with ui.grid(columns='auto auto').classes('w-full gap-0'):
				for attr in dir(object):
					if not attr.startswith('__') and not callable(getattr(object, attr)):
						ui.label(attr).classes('border')
						ui.label('').classes('border').bind_text_from(object, attr, lambda x: str(x))
		return dbg

	@ui.refreshable
	def debug_calculator(self) -> None:
		self.create_debug_table('instance', getattr(self.components['stepper']['file'], 'instance', None))

	def create_process_display(self) -> ui.element:
		with ui.element('div').classes('overflow-y-scroll').style('width: 100%; height: 100%;') as div_content:
			step_file = FileProcessor(auto_next=False, auto_next_delay=0.0)\
				.render()\
				.bind_visibility_from(self.menu.state, 'input_type', lambda x: x=='file')
			step_ofdb = OfdbProcessor()\
				.render()\
				.bind_visibility_from(self.menu.state, 'input_type', lambda x: x=='database')
		self.components.update({'stepper': {'file': step_file, 'database': step_ofdb}})
		return div_content

	def reset_processes(self) -> None:
		process_type = self.menu.state.input_type
		stepper = self.components['stepper'][process_type]
		stepper.reset()

	def get_active_process(self) -> Union[FileProcessor, OfdbProcessor]:
		process_type = self.menu.state.input_type
		return nested_dict(self.components, ['stepper', process_type])

	async def _handle_menu_change(self, e: events.ValueChangeEventArguments) -> None:
		stepper = self.get_active_process()
		object = self.menu.state.calculation_handler
		stepper.instance = object()
		self.reset_processes()


def increment_count():
	app.storage.client['count'] = app.storage.client.get('count', 0) + 1

def not_implemented(e: events.ClickEventArguments):
	ui.notify('Belum diimplementasikan.', type='info')
	e.sender.disable()

def heavy_computation(n: int = 100, interval: float = 0.1, *args, **kwargs) -> Generator[float, str, None]:
	for i in range(n):
		time.sleep(interval)
		yield i/n, f'Process {i/n*100:.0f}%'
	yield 1, f'Process {100}%'


@ui.page(path='/', title='Aplikasi Perhitungan Availability')
async def index():
	webgui = WebGUIv2(cache=app.storage.client)
	# ui.button('Test', on_click=lambda: worker.run(heavy_computation, n=20, interval=1))
	# ui.circular_progress()\
	# 	.bind_value_from(worker, 'progress')\
	# 	.bind_visibility_from(worker, 'is_running')
	# ui.label('Dummy').bind_text_from(worker, 'text')