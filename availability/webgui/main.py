import asyncio, datetime, os, time
from dataclasses import dataclass, field
from functools import partial

from nicegui import app, binding, events, ui
from nicegui.binding import bindable_dataclass

from . import components
from .components import Button, DialogPrompt, GUIAvailability, MenuSubtitle, MenuTitle, NavButton, NavDropdownButton, ObjectDebugger, RCDTabPanel, RTUTabPanel, UIColumn, UIRow, dialog_title_section
from .event import EventChainsWithArgs, EventChainsWithoutArgs
from .state import BindableProgress, BaseState, InterlockState, MenuState, State, toggle_attr
from .types import *
from .. import config, settings, version
from ..core import soe, rcd, rtu
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


####################################################################################################
# NEW DEVELOPED CODE
####################################################################################################

soe_survalent = [
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/SOE_Survalent/EVENT_RC-2025_08.XLSX',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/SOE_Survalent/2025_09_Event_Log_Summary.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/10/2025_10_Event_RC_Summary.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/2025_11_Event_Log_Summary.xlsx'
]
sts_survalent = [
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/SOE_Survalent/EVENT_RS-2025_08.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/SOE_Survalent/2025_09_Status_Point_SUMMARY.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/10/2025_10_AV_RS_SUMMARY.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/2025_11_AV_RS_SUMMARY.xlsx'
]

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
	panel_rcd: GUIAvailability = None
	panel_rtu: GUIAvailability = None
	logger: ui.log = None
	statusbar_text: ui.label = None
	statusbar_progress: ui.linear_progress = None

	def __init__(self) -> None:
		super().__init__()
		self._refreshable = list()
		self.classes('w-full md:max-w-xl mx-0 md:mx-auto mt-3 p-0 text-sm md:text-base rounded-lg gap-1')

		self.menu = None
		self.state = GUIState()
		self.dialog_log = self.dialog_logger()
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
		with UIRow(overflow='scroll', gap=0).classes('w-full px-0') as navbar:
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
			self.panel_rcd = RCDTabPanel(name='RCD', logger=self.logger, options={'': '--------', 'soe': 'File SOE', 'rcd': 'File AVRCD', 'ofdb': 'Offline Database'})
			self.panel_rtu = RTUTabPanel(name='RTU', logger=self.logger, options={'': '--------', 'soe': 'File SOE', 'rtu': 'File AVRS', 'ofdb': 'Offline Database'})

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
				# file_state = ObjectDebugger('av.state', self, 'core_state')
				# file_state.render()
				# rcd_state = ObjectDebugger('rcd.state', self.panel_rcd.av, 'state').render()
				# rtu_state = ObjectDebugger('rtu.state', self.panel_rtu.av, 'state').render()
				with UIRow():
					ui.label('Active links')
					ui.label('').bind_text_from(binding, 'active_links', lambda x: len(x))
			with UIRow().classes('w-full'):
				ui.space()
				# Button(icon='refresh', on_click=file_state.refresh).props('dense size=sm')
				Button(icon='close', on_click=debug.close).props('dense size=sm')
		return debug

	def statusbar(self):
		with UIRow(overflow='auto', gap=1).classes('w-full min-h-[2rem] px-2 pb-2') as statbar:
			Button('', icon='notes', on_click=self.dialog_log.open)\
				.props('flat dense size=sm')\
				.tooltip('Buka log')
			self.statusbar_text = ui.label('Progress')\
				.classes('w-3/4 text-sm text-italic text-gray-400 truncate')
			ui.separator().props('vertical size=1px')
			self.statusbar_progress = ui.linear_progress(value=0, show_value=False, color='cyan')\
				.props('animation-speed=500')\
				.classes('w-1/4 h-6')

	def dialog_logger(self) -> ui.dialog:
		with ui.dialog() as dialog, ui.card(align_items='stretch').classes('min-w-3xl pt-0 pb-2 px-0 gap-y-1'):
			dialog_title_section(title='Log', icon='notes')
			with UIRow().classes('p-2'):
				self.logger = ui.log(max_lines=32).classes('w-full h-96 border-solid rounded-sm')
			ui.separator()
			with UIRow(gap=2).classes('px-4'):
				Button('', icon='restore', color='blue-grey', on_click=self.logger.clear)\
					.props('dense outline')\
					.tooltip('Reset log')
				ui.separator().props('vertical size=1px')
				Button('', icon='file_download', color='blue-grey', on_click=self.event_download_log)\
					.props('dense outline')\
					.tooltip('Export log')
				Button('', icon='content_copy', color='blue-grey', on_click=self.event_copy_logtext)\
					.props('dense outline')\
					.tooltip('Salin clipboard')
				ui.space()
				Button('Tutup', on_click=dialog.close)\
					.props('dense')\
					.classes('w-16')

		return dialog

	def update_ui(self, *args):
		panel = self.get_active_panel()
		if panel is None:
			return

		# self.progress_state.stop_tracking()
		# self.progress_state.start_tracking(panel.av.state.progress, ['value', 'message'])
		self.statusbar_text\
			.bind_text_from(panel.av.state.progress, 'message')
		self.statusbar_progress\
			.bind_value_from(panel.av.state.progress, 'value')\
			.bind_visibility(panel.state, 'progress_visible')

	def get_active_panel(self) -> Optional[GUIAvailability]:
		self.panel_rcd.set_active(self.state.active_menu=='RCD')
		# self.panel_rtu.set_active(self.state.active_menu=='RTU')
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

	def main(self):
		with self:
			with UIColumn(align_items='center'):
				ui.label(settings.APP_TITLE).classes('p-2 text-3xl font-extrabold')
			ui.separator()
			self.navbar()
			ui.separator()
			self.content()
			# Button('Active Links', on_click=lambda: print(self.panel_rcd.av.object.state.last_exported_file))
			ui.separator()
			self.statusbar()

		debug = self.debug_view()
		Button(icon='open_in_full', on_click=debug.open).props('dense size=xs').classes('absolute top-1.5 right-1.5')
		self.update_ui()
		self.logger.push(logprint('Aplikasi running..', level='info', cli=False), classes='text-blue')

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
		# self.panel_rtu.state.reset()

	async def event_test_read(self):
		self.state.progress_visible = True
		avcfg = rcd.RCDConfig()
		av = rcd.RCD(avcfg)
		self.progress_state.start_tracking(av.state.progress, ['value', 'message'])
		dfsoe = await av.async_read_soe_file('/media/shared-ntfs/1-scada-makassar/AVAILABILITY/2025/HISWebUI_spectrum_DATA-MESSAGES_202503*.xlsx')
		_soe = soe.SOE(data=dfsoe, config=avcfg, sources=av.reader.sources)
		dfrcd = await av.async_analyze_soe(_soe.data)
		result = av.calculate(start_date=datetime.datetime(2025,3,1), end_date=datetime.datetime(2025,3,31,23,59,59,999999))

	async def event_stop_read(self):
		self.state.progress_visible = False
		self.progress_state.stop_tracking()

	def event_download_log(self):
		logtext = '\n'.join([label.text for label in self.logger.descendants()])
		ui.download.content(logtext, f'Availability_SCADA_Log_Export_{datetime.datetime.timestamp(datetime.datetime.now()):.0f}.log')
		ui.notify('Log telah disalin ke clipboard', type='info')

	def event_copy_logtext(self):
		texts = [label.text for label in self.logger.descendants()]
		ui.clipboard.write('\n'.join(texts))
		ui.notify('Log telah disalin ke clipboard', type='positive')


@ui.page(path='/', title=settings.APP_TITLE)
async def index():
	webgui = WebGUIv3()
	# ui.button('check')

@ui.page('/docs', title=f'(Docs) {settings.APP_TITLE}')
def view_documentation():
	with open('README.md', 'r') as readme:
		text = readme.readlines()

	ui.markdown('\n'.join(text))