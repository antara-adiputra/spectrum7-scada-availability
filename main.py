#!/usr/bin/python3

from nicegui import binding
from webgui.main import ui

import config


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


binding.MAX_PROPAGATION_TIME = config.MAX_PROPAGATION_TIME
ui.add_head_html(local_file('head.html'), shared=True)

if __name__ in {'__mp_main__', '__main__'}:
	ui.run(
		host=config.HOST,
		port=config.PORT,
		title=config.TITLE,
		viewport=config.VIEWPORT,
		favicon=config.FAVICON,
		binding_refresh_interval=config.BINDING_REFRESH_INTERVAL,
		show=config.AUTO_SHOW,
		reload=config.AUTO_RELOAD,
		on_air=config.ON_AIR,
		endpoint_documentation=config.ENDPOINT_DOCUMENTATION,
		dark=config.DARK_MODE
	)