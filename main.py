#!/usr/bin/python3

import argparse

from nicegui import ui, binding

from availability import config, settings
from availability import webgui


def local_file(path: str) -> str:
	lines = list()
	try:
		with open(path, 'r') as f:
			lines = f.readlines()
	except Exception:
		pass
	finally:
		content = ''.join(lines)
	return content


ui.add_head_html(local_file('availability/webgui/head.html'), shared=True)
binding.MAX_PROPAGATION_TIME = settings.MAX_PROPAGATION_TIME

if __name__ in {'__mp_main__', '__main__'}:
	parser = argparse.ArgumentParser(description='Aplikasi yang digunakan untuk penghitungan availability SCADA pada bagian Fasilitas Operasi PLN UP2B Sistem Makassar.')

	for arg in settings.ARGS_OPTIONS:
		flags = arg.pop('flags')
		parser.add_argument(*flags, **arg)

	args = parser.parse_args()
	host = args.host or settings.HOST
	port = args.port or settings.PORT
	title = args.title or settings.APP_TITLE
	favicon = args.favicon or settings.FAVICON
	dark = args.dark or config.DARK_MODE
	on_air = args.on_air or settings.ON_AIR
	reload = args.no_reload if args.no_reload is not None else args.reload if args.reload is not None else settings.AUTO_RELOAD
	show = args.no_show if args.no_show is not None else args.show if args.show is not None else settings.AUTO_SHOW

	ui.run(
		host=host,
		port=port,
		title=title,
		favicon=favicon,
		dark=dark,
		show=show,
		reload=reload,
		on_air=on_air,
		viewport=settings.VIEWPORT,
		binding_refresh_interval=settings.BINDING_REFRESH_INTERVAL,
		endpoint_documentation=settings.ENDPOINT_DOCUMENTATION,
		reconnect_timeout=settings.RECONNECT_TIMEOUT
	)