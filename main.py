from nicegui import binding
from webgui.main import ui

import config


binding.MAX_PROPAGATION_TIME = config.MAX_PROPAGATION_TIME
ui.add_head_html("""
<style type="text/tailwindcss">
	/*@media (max-width: 768px) {
		html {
			font-size: 0.875rem
		}
	}*/
	.nicegui-expansion.sidebar-menu .q-expansion-item__content {
		padding-right: 0;
	}
	.nicegui-expansion .q-expansion-item__content::before,
	.nicegui-expansion .q-expansion-item__content::after {
		content: none;
	}
	.multi-line-notification {
		white-space: pre-line;
	}
</style>
""", shared=True)

if __name__ in {'__mp_main__', '__main__'}:
	ui.run(port=8001, dark=config.DARK_MODE)