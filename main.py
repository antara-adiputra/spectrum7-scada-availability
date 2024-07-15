from nicegui import binding
from ui.webgui import ui

MAX_PROPAGATION_TIME = 0.01     # default 0.01
binding.MAX_PROPAGATION_TIME = MAX_PROPAGATION_TIME

ui.add_head_html("""
<style type="text/tailwindcss">
	.nicegui-expansion.sidebar-menu .q-expansion-item__content {
		padding-right: 0;
	}
	.nicegui-expansion .q-expansion-item__content::before,
	.nicegui-expansion .q-expansion-item__content::after {
		content: none;
	}
</style>
""", shared=True)

if __name__ in {'__mp_main__', '__main__'}:
    ui.run(port=8001)