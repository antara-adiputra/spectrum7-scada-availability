import asyncio, datetime, os, time
from dataclasses import dataclass, field
from functools import partial

from nicegui import app, events, ui
from nicegui.binding import bindable_dataclass

from . import components
from .components import Button, DialogPrompt, FilePickerv2, MenuSubtitle, MenuTitle, NavButton, NavDropdownButton, ObjectDebugger, AVTabPanel, RCDTabPanel, RTUTabPanel, UIColumn, UIRow, ui_input, ui_item, ui_menu_label, ui_section, ui_select, get_component_props
from .event import EventChainsWithArgs, EventChainsWithoutArgs
from .state import AvStateWrapper, BaseState, InterlockState, MenuState, State, toggle_attr
from .types import *
from .. import config, settings, version
from ..core.rtu import *
from ..lib import consume, instance_factory, rgetattr


CalcOutputGen: TypeAlias = Generator[float, dict, dict]		# (percentage, state, result)

AVRCD_PARAMS: List[str] = ['calculate_bi', 'check_repetition', 'success_mark', 'failed_mark', 'unused_mark', 'reduction_ratio_threshold']
AVRTU_PARAMS: List[str] = ['maintenance_mark', 'link_failure_mark', 'rtu_failure_mark', 'other_failure_mark', 'downtime_rules']
AVRCD_MENU: list[dict[str, Any]] = [
	{
		'id': 'ofdb',
		'label': 'Dari DB Offline',
		'description': 'Menganalisa dan melakukan perhitungan availability RCD menggunakan data dari database Offline.',
		'component': 'OfdbProcessor',
		'component_kwargs': {
			# 'instance': partial(RCDFromOFDB),
			'auto_next': False,
		}
	},
	{
		'id': 'spectrum',
		'label': 'Dari File SOE (Spectrum)',
		'description': 'Menganalisa dan melakukan perhitungan availability RCD dari file SOE Spectrum.',
		'component': 'FileProcessor',
		'component_kwargs': {
			# 'instance': partial(instance_factory, RCDFromFile, **config.get_config(*AVRCD_PARAMS)),
			'auto_next': False,
		}
	},
	{
		'id': 'survalent',
		'label': 'Dari File SOE (Survalent)',
		'description': 'Menganalisa dan melakukan perhitungan availability RCD dari file SOE Survalent.',
		'component': 'FileProcessor',
		'component_kwargs': {
			# 'instance': partial(instance_factory, RCDFromFile2, **config.get_config(*AVRCD_PARAMS)),
			'auto_next': False,
		}
	},
	{
		'id': 'accumulative',
		'label': 'Rangkum File Availability',
		'description': 'Menghitung akumulasi availability RCD dari beberapa file availability RCD.',
		'component': 'FileProcessor',
		'component_kwargs': {
			# 'instance': partial(instance_factory, RCDCollective, **config.get_config(*AVRCD_PARAMS)),
			'auto_next': False,
		}
	}
]
AVRTU_MENU: list[dict[str, Any]] = [
	{
		'id': 'ofdb',
		'label': 'Dari DB Offline',
		'description': 'Menganalisa dan melakukan perhitungan availability Link & Remote Station menggunakan data dari database Offline.',
		'component': 'OfdbProcessor',
		'component_kwargs': {
			'instance': partial(instance_factory, Any),
			'auto_next': False,
		}
	},
	{
		'id': 'spectrum',
		'label': 'Dari File SOE (Spectrum)',
		'description': 'Menganalisa dan melakukan perhitungan availability Link & Remote Station dari file SOE Spectrum.',
		'component': 'FileProcessor',
		'component_kwargs': {
			'instance': partial(instance_factory, Any, **config.get_config(*AVRTU_PARAMS)),
			'auto_next': False,
		}
	},
	{
		'id': 'accumulative',
		'label': 'Rangkum File Availability',
		'description': 'Menghitung akumulasi availability Link & Remote Station dari beberapa file availability Remote Station.',
		'component': 'FileProcessor',
		'component_kwargs': {
			'instance': partial(instance_factory, Any, **config.get_config(*AVRTU_PARAMS)),
			'auto_next': False,
		}
	}
]
SETTINGS_MENU: list[dict[str, Any]] = [
	{
		'id': 'basic_conf',
		'label': 'General',
		'description': '',
		'component': 'GeneralSettingMenu',
		'component_kwargs': {}
	},
	{
		'id': 'ofdb_conf',
		'label': 'DB Offline',
		'description': '',
		'component': 'OfdbSettingMenu',
		'component_kwargs': {}
	},
	{
		'id': 'avrcd_conf',
		'label': 'Perhitungan RCD',
		'description': '',
		'component': 'RCDSettingMenu',
		'component_kwargs': {}
	},
	{
		'id': 'avrs_conf',
		'label': 'Perhitungan AVRS',
		'description': '',
		'component': 'AVRSSettingMenu',
		'component_kwargs': {}
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
	},
	{
		'id': 'setting',
		'label': 'Pengaturan',
		'description': '',
		'submenu': SETTINGS_MENU
	}
]
ABOUT = f"""**{settings.APP_TITLE}**

	Version		: {version.__version__}
	Company		: Fasop UP2B Sistem Makassar
	Contributor	: Putu Agus Antara A.

This project is _open source_ and free to use for testing serial link purpose in various applications.\n
Read our documentation [here](/docs) or check our source code [here](https://github.com/antara-adiputra/spectrum7-scada-availability).
"""


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
		with ui.list().classes('w-full overflow-y-auto') as menu:
			for item in self._menus:
				item_id = item['id']
				with ui.item().classes('p-0'):
					with ui.expansion(text=item['label'], on_value_change=self._handle_menu_expansion)\
						.props('group=menu expand-icon-class="p-0"')\
						.classes('sidebar-menu w-full font-bold text-teal-500') as expand_item:
						expand_item.identifier = item_id
						with ui.element('div'):
							for subitem in item['submenu']:
								subitem_id = subitem['id']
								Button(subitem['label'], identifier=f'{item_id}__{subitem_id}', on_click=self._handle_submenu_click)\
									.props(f'flat no-wrap no-caps text-color=teal-4 {"outline" if (item_id, subitem_id)==self.active else ""} align=left')\
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
			component=sel_submenu['component'],
			comp_kwargs=sel_submenu['component_kwargs']
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
	base_classes: str = 'w-full md:max-w-[64rem] h-[90vh] min-h-[32rem] mx-0 md:mx-auto mt-3 p-0 text-sm md:text-base rounded-2xl'

	def __init__(self, cache: Any = None, accordion_menu: bool = True, default_menu: str = 'avrcd') -> None:
		super().__init__()
		self.classes(self.base_classes)

		self.menu = Menu()
		self.state = State()
		self.cache = app.storage.client if cache is None else cache
		self.accordion_menu = accordion_menu
		# Initialize display
		self.main()

	def main(self, **kwargs):
		with self:
			with ui.row(wrap=False).classes('w-full h-full p-0 gap-1'):
				with ui.column().classes('w-56 md:w-96 h-full p-2 md:p-4 border-r-2') as sidemenu:
					ui.label('APLIKASI PERHITUNGAN KINERJA FASOP').classes('w-full text-2xl md:text-3xl font-extrabold text-teal-700')
					ui.separator().classes('m-0')
					# MENU
					self.menu.update()
				with ui.column().classes('w-full h-full p-2 md:p-4'):
					with ui.element('div').classes('w-full gap-0') as div_title:
						MenuTitle('<Title>').bind_text_from(self.menu.state, 'title')
						MenuSubtitle('<Subtitle>').bind_text_from(self.menu.state, 'subtitle')
						ui.separator().classes('my-2').bind_visibility_from(self.menu.state, 'title')
					with ui.element('div').classes('w-full') as div_description:
						ui.label('<Content description>').bind_text_from(self.menu.state, 'description')
					# CONTENT
					self.panel_content = ui.element('div').classes('overflow-y-auto').style('width: 100%; height: 100%;')
					# self.create_process_display()
		with ui.dialog() as debug, ui.card().classes('w-1/2 md:w-full p-0 gap-y-0'):
			with ui.element('div').classes('w-full border overflow-y-auto'):
				debug_menu_state = ObjectDebugger('menu.state', self.menu.state).render()
				debug_config = ObjectDebugger('config', config).render()
				# self.create_debug_table('stepper.filepicker', self.components['stepper']['file'].filepicker)
				# self.create_debug_table('stepper.state', self.components['stepper']['file'].state)
			with ui.row(align_items='center').classes('w-full p-2 gap-1'):
				ui.space()
				Button(icon='close', on_click=debug.close).props('dense size=sm')
		Button(icon='open_in_full', on_click=debug.open).props('dense size=xs').classes('absolute top-1.5 right-1.5')
		self.menu.on_change(self._handle_menu_change)

	async def _handle_menu_change(self, e: events.ValueChangeEventArguments) -> None:
		comp = getattr(components, self.menu.state.component, None)
		kwargs = self.menu.state.comp_kwargs.copy()
		self.panel_content.clear()

		if 'instance' in kwargs:
			kwargs['instance'] = kwargs['instance']()

		with self.panel_content.add_slot('default') as slot:
			if comp is None:
				pass
			else:
				c = comp(**kwargs).render()
				# print(c.__used__)




####################################################################################################
# NEW DEVELOPED CODE
####################################################################################################


@bindable_dataclass
class GUIState(BaseState):
	active_menu: Literal['RCD', 'RTU'] = 'RCD'
	progress_visible: bool = False
	progress_value: float = 0
	progress_message: str = ''

	def event_menu_changed(self, value: str):
		self.active_menu = value

	def set_menu(self, name: Literal['RCD', 'RTU']):
		self.active_menu = name


class WebGUIv3(ui.card):
	_refreshable: List[ui.refreshable]
	panel_rcd: AVTabPanel = None
	panel_rtu: AVTabPanel = None
	statusbar_text: ui.label = None
	statusbar_progress: ui.linear_progress = None

	def __init__(self) -> None:
		super().__init__()
		self._refreshable = list()
		self.classes('w-full md:max-w-xl mx-0 md:mx-auto mt-3 p-0 text-sm md:text-base rounded-lg gap-1')

		self.menu = None
		self.state = GUIState()
		self.av_state = AvStateWrapper(None)
		self.dialog_prompt = DialogPrompt()
		self.cache = app.storage.client

		# Initialize display
		self.about = self.app_info()

		self.main()

	def app_info(self) -> ui.dialog:
		with ui.dialog() as dialog, ui.card(align_items='stretch').props('square').classes('p-2 gap-1'):
			ui.label('About').classes('text-lg text-bold text-center')
			ui.separator()
			with UIColumn(css_padding='p-1'):
				ui.markdown(content=ABOUT)
				ui.separator()
				with UIRow():
					ui.space()
					ui.button('OK', on_click=dialog.close).props('dense size=md').classes('w-8')
		return dialog

	def navbar(self):
		with UIRow(overflow='scroll', gap=0).classes('w-full px-4') as navbar:
			with NavDropdownButton('')\
				.props(add='dropdown-icon=power_settings_new size=md', remove='dropdown-icon=more_vert'):
				ui.item('Restart', on_click=self.prompt_restart).props('dense')
				ui.item('Shutdown', on_click=self.prompt_shutdown).props('dense')
			ui.separator().props('vertical size=1px')
			ui.space()
			NavButton('Reset', icon='restart_alt',
				on_click=self.reset
			).tooltip('Reset parameter ke default')
			ui.separator().props('vertical size=1px')
			NavButton('', icon='',
				on_click=ui.dark_mode(
					value=config.DARK_MODE,
					on_change=lambda e: config.save(DARK_MODE=e.value)
				).toggle)\
				.bind_icon_from(config, 'DARK_MODE', backward=lambda b: 'light_mode' if b else 'dark_mode')\
				.bind_text_from(config, 'DARK_MODE', backward=lambda b: 'Cerah' if b else 'Gelap')\
				.tooltip('Pilih mode cerah / gelap')
			ui.separator().props('vertical size=1px')
			NavButton('Doc', icon='description',
				on_click=lambda: ui.navigate.to('/docs', new_tab=True)
			).tooltip('Dokumentasi')
			ui.separator().props('vertical size=1px')
			NavButton('', icon='info',
				on_click=self.about.open
			).tooltip('Tentang')

	def content(self):
		with ui.tabs()\
			.bind_value_from(self.state, 'active_menu')\
			.props('active-color=primary')\
			.classes('w-full') as tabs:
			ui.tab('RCD', label='Remote Control (RC)').classes('w-1/2')
			ui.tab('RTU', label='Remote Station').classes('w-1/2')

		with ui.tab_panels(tabs=tabs, value=self.state.active_menu)\
			.bind_value_from(self.state, 'active_menu')\
			.props('animated=false')\
			.classes('w-full'):
			self.panel_rcd = RCDTabPanel(name='RCD', options={'': '--------', 'SOE': 'File SOE', 'RCD': 'File AVRCD', 'OFDB': 'Offline Database'})
			self.panel_rtu = RTUTabPanel(name='RTU', options={'': '--------', 'SOE': 'File SOE', 'RTU': 'File AVRS', 'OFDB': 'Offline Database'})

		tabs.on_value_change(
			EventChainsWithArgs(chains=[
				self.event_menu_changed,
				self.update_ui,
			])
		)

	def debug_view(self) -> ui.dialog:
		with ui.dialog() as debug, ui.card().classes('w-1/2 md:w-full p-0 gap-y-0'):
			with ui.element('div').classes('w-full border overflow-y-auto'):
				debug_state = ObjectDebugger('menu.state', self, 'state').render()
				file_state = ObjectDebugger('av.state', self, 'av_state')
				file_state.render()
			with UIRow().classes('w-full'):
				ui.space()
				Button(icon='refresh', on_click=file_state.refresh).props('dense size=sm')
				Button(icon='close', on_click=debug.close).props('dense size=sm')
		return debug

	def statusbar(self):
		with UIRow(overflow='auto', gap=1).classes('w-full min-h-[2rem] px-4 pb-2') as statbar:
			self.statusbar_text = ui.label('Progress')\
				.bind_text_from(self.state, 'progress_message')\
				.classes('w-3/4 text-sm text-italic text-gray-400')
			ui.separator().props('vertical size=1px')
			self.statusbar_progress = ui.linear_progress(value=0, show_value=False, color='cyan')\
				.bind_visibility_from(self.state, 'progress_visible')\
				.bind_value_from(self.state, 'progress_value')\
				.props('animation-speed=500')\
				.classes('w-1/4 h-6')

	def update_ui(self, *args):
		panel = self.get_active_panel()
		if panel is None:
			return

		self.statusbar_text\
			.bind_text_from(panel.av_state.progress, 'message')
		self.statusbar_progress\
			.bind_value_from(panel.av_state.progress, 'value')\
			.bind_visibility_from(panel.state, 'progress_visible')

	def get_active_panel(self) -> Optional[AVTabPanel]:
		self.panel_rcd.set_active(self.state.active_menu=='RCD')
		self.panel_rtu.set_active(self.state.active_menu=='RTU')
		if self.state.active_menu=='RCD':
			return self.panel_rcd
		elif self.state.active_menu=='RTU':
			return self.panel_rtu
		else:
			return

	def event_menu_changed(self, e: events.ValueChangeEventArguments):
		self.state.event_menu_changed(e.value)

	def update_refreshable(self, *args):
		consume(map(lambda comp: comp.refresh(), self._refreshable))

	def wrap_state(self, *args):
		print(datetime.datetime.now(), 'state wrapped')
		self.av_state.wrap(self.state.av)

	def main(self):
		with self:
			with UIColumn(align_items='center'):
				ui.label(settings.APP_TITLE).classes('p-2 text-3xl font-extrabold')
			ui.separator()
			self.navbar()
			ui.separator()
			self.content()
			ui.separator()
			self.statusbar()

		debug = self.debug_view()
		Button(icon='open_in_full', on_click=debug.open).props('dense size=xs').classes('absolute top-1.5 right-1.5')
		self.update_ui()

	async def prompt_restart(self, e: events.ClickEventArguments) -> None:
		self.dialog_prompt.set(
			title='',
			message='Restart application?',
			choices=['ya', 'tidak']
		)
		result = await self.dialog_prompt

		if result=='ya':
			await asyncio.sleep(3)
			# Trigger changes on main.py and only affect if autoreload is True
			os.utime('main.py')

	async def prompt_shutdown(self, e: events.ClickEventArguments) -> None:
		self.dialog_prompt.set(
			title='',
			message='Shutdown application?',
			choices=['ya', 'tidak']
		)
		result = await self.dialog_prompt

		if result=='ya':
			self.clear()
			with self: ui.label('Application stopped.').classes('w-full text-xl text-center')
			await asyncio.sleep(1)
			app.shutdown()

	def reset(self):
		self.state.reset()
		self.panel_rcd.state.reset()
		self.panel_rtu.state.reset()


@ui.page(path='/', title=settings.APP_TITLE)
async def index():
	webgui = WebGUIv3()
	# ui.button('check')

@ui.page('/docs', title=f'(Docs) {settings.APP_TITLE}')
def view_documentation():
	with open('README.md', 'r') as readme:
		text = readme.readlines()

	ui.markdown('\n'.join(text))