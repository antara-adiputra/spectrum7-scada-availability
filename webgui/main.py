import asyncio, time
from functools import partial
from typing import Any, Dict, List, Callable, Generator, Literal, Tuple, TypeAlias, Union

import config
import webgui.components
from webgui.components import Button, MenuSubtitle, MenuTitle, ObjectDebugger
from webgui.state import MenuState, State
from nicegui import app, events, ui
from lib import nested_dict
from avrs import AVRSFromOFDB, AVRSFromFile, AVRSCollective
from rcd import RCDFromOFDB, RCDFromFile, RCDFromFile2, RCDCollective
from lib import rgetattr, instance_factory


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
			'instance': partial(RCDFromOFDB),
			'auto_next': False,
		}
	},
	{
		'id': 'spectrum',
		'label': 'Dari File SOE (Spectrum)',
		'description': 'Menganalisa dan melakukan perhitungan availability RCD dari file SOE Spectrum.',
		'component': 'FileProcessor',
		'component_kwargs': {
			'instance': partial(instance_factory, RCDFromFile, **config.get_config(*AVRCD_PARAMS)),
			'auto_next': False,
		}
	},
	{
		'id': 'survalent',
		'label': 'Dari File SOE (Survalent)',
		'description': 'Menganalisa dan melakukan perhitungan availability RCD dari file SOE Survalent.',
		'component': 'FileProcessor',
		'component_kwargs': {
			'instance': partial(instance_factory, RCDFromFile2, **config.get_config(*AVRCD_PARAMS)),
			'auto_next': False,
		}
	},
	{
		'id': 'accumulative',
		'label': 'Rangkum File Availability',
		'description': 'Menghitung akumulasi availability RCD dari beberapa file availability RCD.',
		'component': 'FileProcessor',
		'component_kwargs': {
			'instance': partial(instance_factory, RCDCollective, **config.get_config(*AVRCD_PARAMS)),
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
			'instance': partial(instance_factory, AVRSFromOFDB),
			'auto_next': False,
		}
	},
	{
		'id': 'spectrum',
		'label': 'Dari File SOE (Spectrum)',
		'description': 'Menganalisa dan melakukan perhitungan availability Link & Remote Station dari file SOE Spectrum.',
		'component': 'FileProcessor',
		'component_kwargs': {
			'instance': partial(instance_factory, AVRSFromFile, **config.get_config(*AVRTU_PARAMS)),
			'auto_next': False,
		}
	},
	{
		'id': 'accumulative',
		'label': 'Rangkum File Availability',
		'description': 'Menghitung akumulasi availability Link & Remote Station dari beberapa file availability Remote Station.',
		'component': 'FileProcessor',
		'component_kwargs': {
			'instance': partial(instance_factory, AVRSCollective, **config.get_config(*AVRTU_PARAMS)),
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
		comp = getattr(webgui.components, self.menu.state.component, None)
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


@ui.page(path='/', title='Aplikasi Perhitungan Availability')
async def index():
	webgui = WebGUIv2(cache=app.storage.client)
	# ui.button('check')