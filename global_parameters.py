SOE_COLUMNS = [
	'A',
	'Time stamp',
	'Milliseconds',
	'System time stamp',
	'System milliseconds',
	'B1',
	'B2',
	'B3',
	'Element',
	'Status',
	'Tag',
	'Operator',
	'Comment',
	'User comment'
]
SOE_COLUMNS_DTYPE = {
	'A': 'str',
	'Time stamp': 'datetime64[s]',
	'Milliseconds': 'float32',
	'System time stamp': 'datetime64[s]',
	'System milliseconds': 'float32',
	'B1': 'str',
	'B2': 'str',
	'B3': 'str',
	'Element': 'str',
	'Description': 'str',
	'Status': 'str',
	'Priority': 'uint16',
	'Tag': 'str',
	'Operator': 'str',
	'Message class': 'str',
	'Comment': 'str',
	'User comment': 'str',
	'SoE': 'str'
}
RCD_COLUMNS = [
	'Order Time',
	'Feedback Time',
	'B1',
	'B2',
	'B3',
	'Element',
	'Status',
	'Tag',
	'Operator',
	'Pre Result',
	'Execution (s)',
	'Termination (s)',
	'TxRx (s)',
	'Rep. Flag',
	'Marked Unused',
	'Marked Success',
	'Marked Failed',
	'Final Result',
	'Annotations'
]
RTU_COLUMNS = [
	'Down Time',
	'Up Time',
	'RTU',
	'Long Name',
	'Duration',
	'Annotations'
]
HIS_SHEET_PARAM = {
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
RCD_SHEET_PARAM = {
	'format': {
		'Order Time': {'num_format': 'dd-mm-yyyy hh:mm:ss.000', 'align': 'center'},
		'Feedback Time': {'num_format': 'dd-mm-yyyy hh:mm:ss.000', 'align': 'center'},
		'Pre Result': {'num_format': '@', 'align': 'center'},
		'Execution (s)': {'num_format': '0.000', 'align': 'center', 'text_wrap': True},
		'Termination (s)': {'num_format': '0.000', 'align': 'center', 'text_wrap': True},
		'TxRx (s)': {'num_format': '0.000', 'align': 'center', 'text_wrap': True},
		'Annotations': {'num_format': '@', 'text_wrap': True},
		'Rep. Flag': {'num_format': '@', 'align': 'center', 'text_wrap': True},
		'Marked Unused': {'num_format': '@', 'align': 'center', 'text_wrap': True},
		'Marked Success': {'num_format': '@', 'align': 'center', 'text_wrap': True},
		'Marked Failed': {'num_format': '@', 'align': 'center', 'text_wrap': True},
		'Final Result': {'align': 'center'},
		'Navigation': {'bold': True, 'font_color': 'blue', 'align': 'center', 'border': 1, 'bg_color': '#dcdcdc'},
		'Order Row (Helper)': {'num_format': 0, 'align': 'center', 'text_wrap': True},
		'Feedback Row (Helper)': {'num_format': 0, 'align': 'center', 'text_wrap': True}
	},
	'width': {
		'Order Time': 25,
		'Feedback Time': 25,
		'Annotations': 28,
		'Execution (s)': 12,
		'Termination (s)': 12,
		'TxRx (s)': 9,
		'Rep. Flag': 4,
		'Marked Unused': 12,
		'Marked Success': 12,
		'Marked Failed': 12,
		'Order Row (Helper)': 10,
		'Feedback Row (Helper)': 10
	}
}
RCDGROUP_SHEET_PARAM = {
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
		'Close Failed': {'num_format': '0', 'align': 'center'},
		'Contribution': {'num_format': '0.00%', 'align': 'center'},
		'Reduction': {'num_format': '0.00%', 'align': 'center'},
		'Tagging': {'num_format': '0', 'align': 'center'}
	},
	'width': {}
}
RTUDOWN_SHEET_PARAM = {
	'format': {
		'Down Time': {'num_format': 'dd-mm-yyyy hh:mm:ss.000', 'align': 'center'},
		'Up Time': {'num_format': 'dd-mm-yyyy hh:mm:ss.000', 'align': 'center'},
		'RTU': {'num_format': '@'},
		'Long Name': {'num_format': '@'},
		'Duration': {'num_format': '[hh]:mm:ss', 'align': 'center'},
		'Acknowledged Down Time': {'num_format': 'dd-mm-yyyy hh:mm:ss', 'align': 'center', 'text_wrap': True},
		'Fix Duration': {'num_format': '[hh]:mm:ss', 'align': 'center'},
		'Annotations': {'num_format': '@', 'text_wrap': True},
		'Marked Maintenance': {'num_format': '@', 'align': 'center', 'text_wrap': True},
		'Marked Link Failure': {'num_format': '@', 'align': 'center', 'text_wrap': True},
		'Marked RTU Failure': {'num_format': '@', 'align': 'center', 'text_wrap': True},
		'Marked Other Failure': {'num_format': '@', 'align': 'center', 'text_wrap': True},
		'Navigation': {'bold': True, 'font_color': 'blue', 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#dcdcdc'}
	},
	'width': {
		'Down Time': 23,
		'Up Time': 23,
		'Duration': 16,
        'Acknowledged Down Time': 23,
		'Fix Duration': 16,
		'Annotations': 28,
		'Marked Maintenance': 16,
		'Marked Link Failure': 12,
		'Marked RTU Failure': 12,
		'Marked Other Failure': 12
	}
}
RTUGROUP_SHEET_PARAM = {
	'format': {
		'Downtime Occurences': {'num_format': '0', 'align': 'center', 'text_wrap': True},
		'Total Downtime': {'num_format': '[hh]:mm:ss', 'align': 'center', 'text_wrap': True},
		'Average Downtime': {'num_format': '[hh]:mm:ss', 'align': 'center', 'text_wrap': True},
		'Longest Downtime': {'num_format': '[hh]:mm:ss', 'align': 'center', 'text_wrap': True},
		'Longest Downtime Date': {'num_format': 'dd-mm-yyyy hh:mm:ss.000', 'align': 'center', 'text_wrap': True},
		'Time Range': {'num_format': '[hh]:mm:ss', 'align': 'center', 'text_wrap': True},
		'Uptime': {'num_format': '[hh]:mm:ss', 'align': 'center'},
		'Unclassified Downtime': {'num_format': '[hh]:mm:ss', 'align': 'center', 'text_wrap': True},
		'RTU Downtime': {'num_format': '[hh]:mm:ss', 'align': 'center', 'text_wrap': True},
		'Link Downtime': {'num_format': '[hh]:mm:ss', 'align': 'center', 'text_wrap': True},
		'Other Downtime': {'num_format': '[hh]:mm:ss', 'align': 'center', 'text_wrap': True},
		'Quality': {'num_format': '0', 'align': 'center'},
		'RTU Availability': {'num_format': '0.00%', 'align': 'center', 'text_wrap': True},
		'Link Availability': {'num_format': '0.00%', 'align': 'center', 'text_wrap': True}
	},
	'width': {
		'Downtime Occurences': 15,
		'Total Downtime': 13,
		'Average Downtime': 13,
		'Longest Downtime': 13,
		'Longest Downtime Date': 23,
		'Time Range': 13,
		'Uptime': 11,
		'Unclassified Downtime': 14,
		'RTU Downtime': 13,
		'Link Downtime': 13,
		'Other Downtime': 13,
		'RTU Availability': 13,
		'Link Availability': 13
	}
}

RCD_BOOK_PARAM = {
	'format': {
		**HIS_SHEET_PARAM['format'],
		**RCD_SHEET_PARAM['format'],
		**RCDGROUP_SHEET_PARAM['format']
	},
	'width': {
		**HIS_SHEET_PARAM['width'],
		**RCD_SHEET_PARAM['width'],
		**RCDGROUP_SHEET_PARAM['width']
	}
}
RTU_BOOK_PARAM = {
	'format': {
		**HIS_SHEET_PARAM['format'],
		**RTUDOWN_SHEET_PARAM['format'],
		**RTUGROUP_SHEET_PARAM['format']
	},
	'width': {
		**HIS_SHEET_PARAM['width'],
		**RTUDOWN_SHEET_PARAM['width'],
		**RTUGROUP_SHEET_PARAM['width']
	}
}