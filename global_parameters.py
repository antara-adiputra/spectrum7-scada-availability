df_columns = ['A', 'Time stamp', 'Milliseconds', 'System time stamp', 'System milliseconds', 'B1', 'B2', 'B3', 'Element', 'Status', 'Tag', 'Operator', 'Comment', 'User comment']
df_columns_dtype = {'A': 'str', 'Time stamp': 'datetime64[s]', 'Milliseconds': 'float32', 'System time stamp': 'datetime64[s]', 'System milliseconds': 'float32', 'B1': 'str', 'B2': 'str', 'B3': 'str', 'Element': 'str', 'Description': 'str', 'Status': 'str', 'Priority': 'uint16', 'Tag': 'str', 'Operator': 'str', 'Message class': 'str', 'Comment': 'str', 'User comment': 'str', 'SoE': 'str'}

his_sheet_param = {
	'format': {
		'A': {'num_format': '@'},
		'Time stamp': {'num_format': 'dd-mm-yyyy hh:mm:ss'},
		'Milliseconds': {'num_format': '0'},
		'System time stamp': {'num_format': 'dd-mm-yyyy hh:mm:ss'},
		'System milliseconds': {'num_format': '0'},
		'B1': {'num_format': '@'},
		'B2': {'num_format': '@'},
		'B3': {'num_format': '@'},
		'Element': {'num_format': '@'},
		'Description': {'num_format': '@'},
		'Status': {'num_format': '@'},
		'Priority': {'num_format': 1},
		'Tag': {'num_format': '@'},
		'Operator': {'num_format': '@'},
		'Message class': {'num_format': '@'},
		'Comment': {'num_format': '@', 'text_wrap': True},
		'User comment': {'num_format': '@', 'text_wrap': True},
		'SoE': {'num_format': '@'}
	},
	'width': {
		'Time stamp': 18,
		'Milliseconds': 6,
		'System time stamp': 18,
		'System milliseconds': 6,
		'Description': 18,
		'Comment': 22,
		'User comment': 12
	}
}
rcd_sheet_param = {
	'format': {
		'Order Time': {'num_format': 'dd-mm-yyyy hh:mm:ss.000', 'align': 'center'},
		'Feedback Time': {'num_format': 'dd-mm-yyyy hh:mm:ss.000', 'align': 'center'},
		'Pre Result': {'num_format': '@', 'align': 'center'},
		'Execution (s)': {'num_format': '0.000', 'align': 'center'},
		'Termination (s)': {'num_format': '0.000', 'align': 'center'},
		'TxRx (s)': {'num_format': '0.000', 'align': 'center'},
		'Annotations': {'num_format': '@', 'text_wrap': True},
		'Rep. Flag': {'num_format': '@', 'align': 'center'},
		'Marked Unused': {'num_format': '@', 'align': 'center'},
		'Marked Success': {'num_format': '@', 'align': 'center'},
		'Marked Failed': {'num_format': '@', 'align': 'center'},
		'Final Result': {'align': 'center'},
        'Navigation': {'bold': True, 'font_color': 'blue', 'align': 'center', 'border': 1, 'bg_color': '#dcdcdc'}
	},
	'width': {
		'Order Time': 23,
		'Feedback Time': 23,
		'Annotations': 28,
        'Execution (s)': 10,
        'Termination (s)': 12,
        'TxRx (s)': 10,
        'Rep. Flag': 8,
		'Marked Unused': 8,
		'Marked Success': 8,
		'Marked Failed': 8,
	}
}
rcdgroup_sheet_param = {
	'format': {
		'Success Rate': {'num_format': '0.00%', 'align': 'center'},
		'RC Occurences': {'num_format': '0', 'align': 'center'},
		'RC Success': {'num_format': '0', 'align': 'center'},
		'RC Failed': {'num_format': '0', 'align': 'center'},
		'Execution Avg.': {'num_format': '0.000', 'align': 'center'},
		'Termination Avg.': {'num_format': '0.000', 'align': 'center'},
		'TxRx Avg.': {'num_format': '0.000', 'align': 'center'},
		'Open Success': {'num_format': '0', 'align': 'center'},
		'Open Failed': {'num_format': '0', 'align': 'center'},
		'Close Success': {'num_format': '0', 'align': 'center'},
		'Close Failed': {'num_format': '0', 'align': 'center'}
	},
	'width': {}
}
downtime_sheet_param = {
	'format': {
		'Down Time': {'num_format': 'dd-mm-yyyy hh:mm:ss.000', 'align': 'center'},
		'Up Time': {'num_format': 'dd-mm-yyyy hh:mm:ss.000', 'align': 'center'},
		'RTU': {'num_format': '@'},
		'Long Name': {'num_format': '@'},
		'Duration': {'num_format': '[hh]:mm:ss', 'align': 'center'},
		'Annotations': {'num_format': '@', 'text_wrap': True},
        'Navigation': {'bold': True, 'font_color': 'blue', 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#dcdcdc'}
	},
	'width': {
		'Down Time': 23,
		'Up Time': 23,
		'Duration': 16,
		'Annotations': 28
	}
}
downgroup_sheet_param = {
	'format': {
		'Downtime Occurences': {'num_format': '0', 'align': 'center'},
		'Total Downtime': {'num_format': '[hh]:mm:ss', 'align': 'center'},
		'Average Downtime': {'num_format': '[hh]:mm:ss', 'align': 'center'},
		'Longest Downtime': {'num_format': '[hh]:mm:ss', 'align': 'center'},
		'Longest Downtime Date': {'num_format': 'dd-mm-yyyy hh:mm:ss.000', 'align': 'center'},
		'Time Range': {'num_format': '[hh]:mm:ss', 'align': 'center'},
		'Uptime': {'num_format': '[hh]:mm:ss', 'align': 'center'},
		'Maintenance': {'num_format': '[hh]:mm:ss', 'align': 'center'},
		'Calculated Availability': {'num_format': '0.00%', 'align': 'center'},
		'Quality': {'num_format': '0', 'align': 'center'},
		'Availability': {'num_format': '0.00%', 'align': 'center'}
	},
	'width': {
		'Longest Downtime Date': 23,
		'Time Range': 16,
		'Uptime': 16
	}
}

rcanalyzer_sheet_param = {
	'format': {**his_sheet_param['format'], **rcd_sheet_param['format'], **rcdgroup_sheet_param['format']},
	'width': {**his_sheet_param['width'], **rcd_sheet_param['width'], **rcdgroup_sheet_param['width']}
}
avremote_sheet_param = {
	'format': {**his_sheet_param['format'], **downtime_sheet_param['format'], **downgroup_sheet_param['format']},
	'width': {**his_sheet_param['width'], **downtime_sheet_param['width'], **downgroup_sheet_param['width']}
}