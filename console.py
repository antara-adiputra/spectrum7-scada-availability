import os
from avrs import AvRemoteStation
from consolemenu import *
from consolemenu.items import *
from datetime import datetime
from filereader import SpectrumFileReader
from glob import glob
from ofdb import SpectrumOfdbClient
from pathlib import Path
from rcd import RCAnalyzer


class ConsoleApp:

	def __init__(self):
		self.screen = Screen()
		self.title = 'Aplikasi SCADA'
		self.subtitle = ''
		self.prologue = ''
		self.dimension = (80, 30)
		self.exit_text = 'Keluar'
		self.buffer = []

	def from_ofdb(self, *args, **kwargs):
		pu = PromptUtils(screen=self.screen)
		pu.println('')
		# print(args, kwargs)
		reff_menu = kwargs['menu']
		proc = kwargs['processor']

		if args[0]=='rcd':
			title = 'RC Sukses'
		elif args[0]=='avrs':
			title = 'Availability Remote Station'

		dt0 = pu.input('Tanggal mulai (dd-mm-yyyy) : ')
		dt1 = pu.input('Tanggal akhir (dd-mm-yyyy) : ')

		try:
			date_start = datetime.strptime(dt0.input_string, '%d-%m-%Y')
			date_stop = datetime.strptime(dt1.input_string, '%d-%m-%Y')
		except ValueError:
			pu.enter_to_continue('\nTanggal tidak valid!')
			pu.clear()
			reff_menu.show()

		if pu.confirm_answer('y', f'\nHitung "{title}" ({dt0.input_string} s/d {dt1.input_string}).\nLanjutkan?'):
			pu.clear()
			c = SpectrumOfdbClient(date_start=date_start, date_stop=date_stop)
			ps = proc(c)
			ps.calculate()
		else:
			pu.clear()
			reff_menu.show()

		if pu.confirm_answer('y', f'\nExport hasil?'):
			ps.export_result()

		pu.enter_to_continue('\n>> Klik [Enter] untuk lanjut')
		pu.clear()

	def from_file(self, *args, **kwargs):
		pu = PromptUtils(screen=self.screen)
		pu.println('')
		# print(args, kwargs)
		reff_menu = kwargs['menu']
		proc = kwargs['processor']

		if args[0]=='rcd':
			title = 'RC Sukses'
		elif args[0]=='avrs':
			title = 'Availability Remote Station'

		filepaths = pu.input('Gunakan tanda koma (,) untuk menginput lebih dari satu file, atau tanda bintang (*) untuk file dengan nama serupa.\nLokasi file : ')
		f = SpectrumFileReader(filepaths.input_string)

		if f.filepaths:
			if pu.confirm_answer('y', f'\nAnda menginput {len(f.filepaths)} file:\n{print_list(f.filepaths)}\nLanjutkan?'):
				pu.clear()
				f.load()
				ps = proc(f)
				ps.calculate()
			else:
				pu.clear()
				reff_menu.show()
		else:
			pu.enter_to_continue('Lokasi file tidak valid!')
			pu.clear()
			reff_menu.show()

		if pu.confirm_answer('y', f'\nExport hasil?'):
			ps.export_result()

		pu.enter_to_continue('\n>> Klik [Enter] untuk lanjut')
		pu.clear()

	def setup(self):
		os.system(f'title {self.title}')
		os.system(f'mode {self.dimension}')
		self.menu = ConsoleMenu(title=self.title, subtitle=self.subtitle, exit_option_text='Keluar', screen=self.screen)
		
		self.menu_rcd = ConsoleMenu(title='Kalkulasi RC', clear_screen=False, exit_option_text='Kembali', screen=self.screen)
		self.menu_avrs = ConsoleMenu(title='Availability Remote Station', clear_screen=False, exit_option_text='Kembali', screen=self.screen)

		submenu = [
			{'menu': self.menu_rcd, 'args': ['rcd'], 'kwargs': {'processor': RCAnalyzer}},
			{'menu': self.menu_avrs, 'args': ['avrs'], 'kwargs': {'processor': AvRemoteStation}}
		]

		for sm in submenu:
			item1 = FunctionItem('Dari database', function=self.from_ofdb, menu=sm['menu'], args=sm['args'], kwargs={'menu': sm['menu'], **sm['kwargs']})
			item2 = FunctionItem('Dari file', function=self.from_file, menu=sm['menu'], args=sm['args'], kwargs={'menu': sm['menu'], **sm['kwargs']})
			sm['menu'].items = [item1, item2]

		submenu1 = SubmenuItem('Kalkulasi RC', menu=self.menu, submenu=self.menu_rcd)
		submenu2 = SubmenuItem('Availability Remote Station', menu=self.menu, submenu=self.menu_avrs)

		self.menu.items = [submenu1, submenu2]

		self.menu.start()
		self.menu.join()

	def start(self):
		self.setup()


def print_list(arr:list):
	text = ''
	for i, s in enumerate(arr):
		text += f'  {i+1}. {s}\n'
	return text

def input_file_his():
	pu = PromptUtils(Screen())
	file = pu.input('\nGunakan tanda koma (,) untuk menginput lebih dari satu file, atau tanda bintang (*) untuk file dengan nama serupa.\n\n>> Lokasi file : ')
	file_list = []
	for f in file.input_string.split(','):
		if f.strip(): file_list += glob(f.strip())
	if len(file_list)>0:
		if pu.confirm_answer('y', f'\n\nAnda menginput {len(file_list)} file:\n{print_list(file_list)}\nApakah sudah benar?'):
			pu.clear()
			rc = RCAnalyzer(file_list)
			rc.calculate()
			rc.print_result()
			rc.export_result()
			pu.enter_to_continue('>> Klik [Enter] untuk lanjut')
			pu.clear()
		else:
			input_file_his()
	else:
		input_file_his() if file.input_string else pu.clear()

def input_file_rcd():
	pu = PromptUtils(Screen())
	pu.clear()

def main():
	os.system('title Kalkulasi RC')
	os.system('mode 80,30')
	desc = 'Aplikasi ini bertujuan untuk memudahkan dalam menghitung maupun menganalisa event RC SCADA berdasarkan data "Historical Message" pada Master Station UP2B Sistem Makassar.'
	menu = ConsoleMenu(title='Menu Utama', subtitle='KALKULASI RC SCADA'.center(70, ' '), prologue_text=desc, exit_option_text='Keluar')
	item_1 = FunctionItem('Analisa data Historical Messages', input_file_his)
	item_2 = FunctionItem('Rangkum data RC', input_file_rcd)

	# Create a second submenu, but this time use a standard ConsoleMenu instance
	submenu1 = ConsoleMenu('Another Submenu Title', 'Submenu subtitle.')
	item1_1 = FunctionItem('Fun item', Screen().input, ['Enter an input: '])
	item1_2 = MenuItem('Another Item')
	submenu1.append_item(item1_1)
	submenu1.append_item(item1_2)
	submenu1_item = SubmenuItem('Rangkuman RC', submenu=submenu1)
	submenu1_item.set_menu(menu)

	# Add all the items to the root menu
	menu.append_item(item_1)
	# menu.append_item(item_2)

	# Show the menu
	menu.start()
	menu.join()


if __name__ == '__main__':
	# main()
	app = ConsoleApp()
	app.start()