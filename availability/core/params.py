SWITCHING_ELEMENT = ('CB', 'BI1', 'BI2')
FEEDBACK_TAG = ('RC', 'NE', 'R*', 'N*')
ORDER_TAG = ('OR', 'O*')
CSW_STATUS = ('Open', 'Close', 'Dist.')
CD_STATUS = ('Enable', 'Disable', 'Dist.')
LR_STATUS = ('Local', 'Remote', 'Dist.')
CSO_STATUS = ('On', 'Off', 'Dist.')
RTU_STATUS = ('Up', 'Down')
ALARM_STATUS = ('Appeared', 'Disappeared', 'Disapear')
MONITORING_TIME = dict(CB=15, BI1=30, BI2=30)
SOE_STATUS_DICT = dict(
	opened='Open',
	closed='Close',
	enabled='Enable',
	disabled='Disable',
	appear='Appeared',
	disappear='Disappeared',
	disapear='Disappeared',
)

# This based on nicegui ui.log reference for log style & customization
INFOLOG_KWARGS = {'classes': 'text-wrap'}	# {'classes': 'text-cyan-700'}
ERRORLOG_KWARGS = {'classes': 'text-red-600 text-wrap'}
WARNINGLOG_KWARGS = {'classes': 'text-amber-500 text-wrap'}
SUCCESSLOG_KWARGS = {'classes': 'text-green-600 text-wrap'}