#!/usr/bin/python3

from nicegui import ui, binding

from availability import config, settings
from availability import webgui		# must imported to initialize GUI


binding.MAX_PROPAGATION_TIME = settings.MAX_PROPAGATION_TIME

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


if __name__=='__main__':
	# Modified in 25-12-2025
	# Prevent code run twice and slightly improve performance!!
	# Reference : https://github.com/zauberzeug/nicegui/issues/794
	# ui.run is blocking function
	try:
		ui.run(
			host=settings.HOST,
			port=settings.PORT,
			title=settings.APP_TITLE,
			favicon=settings.FAVICON,
			dark=config.DARK_MODE,
			show=settings.AUTO_SHOW,
			reload=settings.AUTO_RELOAD,
			on_air=settings.ON_AIR,
			viewport=settings.VIEWPORT,
			binding_refresh_interval=settings.BINDING_REFRESH_INTERVAL,
			endpoint_documentation=settings.ENDPOINT_DOCUMENTATION,
			reconnect_timeout=settings.RECONNECT_TIMEOUT
		)
	except KeyboardInterrupt:
		config.logprint('Application shutdown successfully.', level='info')
