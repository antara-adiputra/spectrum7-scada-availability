import os
from datetime import datetime
from glob import glob

from avrs import AVRSCollective, AVRSFromFile, AVRSFromOFDB
from consolemenu import *
from consolemenu.items import *
from rcd import RCDCollective, RCDFromFile, RCDFromFile2, RCDFromOFDB


class ConsoleApp:
	title = 'Aplikasi SCADA'
	subtitle = ''
	prologue = ''
	dimension = (80, 30)
	exit_text = 'Keluar'
	back_text = 'Kembali'

	def __init__(self):
		self.screen = Screen()
		self.buffer = []
		self.setup()

	def from_ofdb(self, *args, **kwargs):
		instance = args[0]
		reff_menu = kwargs['menu']

		pu = PromptUtils(screen=self.screen)
		pu.println('')

		dt0 = pu.input('Tanggal mulai (dd-mm-yyyy) : ')
		dt1 = pu.input('Tanggal akhir (dd-mm-yyyy) : ')

		try:
			date_start = datetime.strptime(dt0.input_string, '%d-%m-%Y')
			date_stop = datetime.strptime(dt1.input_string, '%d-%m-%Y')
		except ValueError:
			pu.enter_to_continue('\nTanggal tidak valid!')
			pu.clear()
			reff_menu.show()

		if pu.confirm_answer('y', f'\nHitung "{reff_menu.title}" ({dt0.input_string} s/d {dt1.input_string}).\nLanjutkan?'):
			pu.clear()
			calc = instance(date_start=date_start, date_stop=date_stop)
			calc.calculate()

			if pu.confirm_answer('y', f'\nExport hasil?'):
				calc.to_excel()

		pu.enter_to_continue('\n\n>> Klik [Enter] untuk lanjut')
		pu.clear()

	def from_file(self, *args, **kwargs):
		instance = args[0]
		reff_menu = kwargs['menu']
		filepaths = []

		pu = PromptUtils(screen=self.screen)
		pu.println('')

		files = pu.input('Gunakan tanda koma (,) untuk menginput lebih dari satu file, atau tanda bintang (*) untuk file dengan nama serupa.\nLokasi file : ')

		for f in files.input_string.split(','):
			if '*' in f:
				filepaths += glob(f.strip())
			elif f.strip():
				filepaths.append(f.strip())

		if filepaths:
			if pu.confirm_answer('y', f'\nAnda menginput {len(filepaths)} file:\n{print_list(filepaths)}\nLanjutkan?'):
				pu.clear()
				try:
					calc = instance(filepaths)
					calc.calculate()

					if pu.confirm_answer('y', f'\nExport hasil?'):
						calc.to_excel()
				except Exception as e:
					print('Oops! Terjadi kesalahan. ' + '\n'.join(e.args) + '\n')

		pu.enter_to_continue('\n\n>> Klik [Enter] untuk lanjut')
		pu.clear()

	def setup(self):
		os.system(f'title {self.title}')
		os.system(f'mode {self.dimension}')
		# Create main menu screen
		self.menu = ConsoleMenu(title=self.title, subtitle=self.subtitle, exit_option_text=self.exit_text, screen=self.screen)
		# Define submenu RCD
		menu_rcd = ConsoleMenu(title='Remote Control SCADA', clear_screen=False, exit_option_text=self.back_text, screen=self.screen)
		item_rcd1 = FunctionItem('Dari database', function=self.from_ofdb, menu=menu_rcd, args=[RCDFromOFDB], kwargs={'menu': menu_rcd})
		item_rcd2 = FunctionItem('Dari file SOE (Spectrum)', function=self.from_file, menu=menu_rcd, args=[RCDFromFile], kwargs={'menu': menu_rcd})
		item_rcd3 = FunctionItem('Dari file SOE (Survalent)', function=self.from_file, menu=menu_rcd, args=[RCDFromFile2], kwargs={'menu': menu_rcd})
		item_rcd4 = FunctionItem('Rangkum beberapa file', function=self.from_file, menu=menu_rcd, args=[RCDCollective], kwargs={'menu': menu_rcd})
		menu_rcd.items = [item_rcd1, item_rcd2, item_rcd3, item_rcd4]
		submenu1 = SubmenuItem('Remote Control SCADA', menu=self.menu, submenu=menu_rcd)
		# Define submenu AVRS
		menu_avrs = ConsoleMenu(title='Availability Remote Station', clear_screen=False, exit_option_text=self.back_text, screen=self.screen)
		item_avrs1 = FunctionItem('Dari database', function=self.from_ofdb, menu=menu_avrs, args=[AVRSFromOFDB], kwargs={'menu': menu_avrs})
		item_avrs2 = FunctionItem('Dari file SOE (Spectrum)', function=self.from_file, menu=menu_avrs, args=[AVRSFromFile], kwargs={'menu': menu_avrs})
		item_avrs3 = FunctionItem('Rangkum beberapa file', function=self.from_file, menu=menu_avrs, args=[AVRSCollective], kwargs={'menu': menu_avrs})
		menu_avrs.items = [item_avrs1, item_avrs2, item_avrs3]
		submenu2 = SubmenuItem('Availability Remote Station', menu=self.menu, submenu=menu_avrs)
		# Append submenu
		self.menu.items = [submenu1, submenu2]

	def start(self):
		self.menu.start()
		self.menu.join()


def print_list(arr:list):
	text = ''
	for i, s in enumerate(arr):
		text += f'  {i+1}. {s}\n'
	return text


if __name__ == '__main__':
	# main()
	app = ConsoleApp()
	app.start()