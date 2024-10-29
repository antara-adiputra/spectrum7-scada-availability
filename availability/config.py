# This is user configurable variable which can be updated on runtime 

import json, pyodbc
from configparser import ConfigParser
from dotenv import load_dotenv
from typing import Any, Dict, List, Literal, Optional, Tuple, Union


__conf = ConfigParser(default_section='GENERAL')
__conf.read('.config')
__DEFAULT__: Dict[str, Any] = dict()
__EXCLUDED__: List[str] = ['DB_DRIVERS', 'PARAMETER_OFDB', 'PARAMETER_RCD', 'PARAMETER_AVRS']
DARK_MODE: bool = False
MAX_FILES: Optional[int] = None
MAX_FILE_SIZE: Optional[int] = None
MAX_TOTAL_SIZE: Optional[int] = None
# OFDB
DB_DRIVERS: List[str] = pyodbc.drivers()
OFDB_HOSTNAME: str = '192.168.1.1'
OFDB_PORT: Union[str, int] = 1024
OFDB_USER: str = ''
OFDB_TOKEN: str = ''
OFDB_DATABASE: str = ''
OFDB_DRIVER: str = ''
OFDB_SCHEMA: str = 'dbo'
OFDB_TABLE_ANALOG: str = 'scd_his_10_anat'
OFDB_TABLE_POINT: str = 'scd_c_point'
OFDB_TABLE_DIGITAL: str = 'scd_his_11_digitalt'
OFDB_TABLE_HISTORICAL: str = 'scd_his_message'
COMMUNICATION_TIMEOUT: float = 5.0
# RCD
CALCULATE_BI: bool = False
CHECK_REPETITION: bool = True
SUCCESS_MARK: str = '**success**'
FAILED_MARK: str = '**failed**'
UNUSED_MARK: str = '**unused**'
REDUCTION_RATIO_THRESHOLD: float = 1.00
# AVRS
MAINTENANCE_MARK: str = '**maintenance**'
LINK_FAILURE_MARK: str = '**link**'
RTU_FAILURE_MARK: str = '**rtu**'
OTHER_FAILURE_MARK: str = '**other**'
DOWNTIME_RULES: List[Tuple[str, int]] = [
	['Critical', 72],
	['Major', 24],
	['Intermediate', 8],
	['Minor', 3]
]

def _satisfy(key: str) -> bool:
	if key.isupper() and not (key.startswith('_') or key in __EXCLUDED__):
		return True
	else:
		return False

def load():
	try:
		with open('config.json', 'rb') as file:
			config: Dict[str, Any] = json.load(file)
		for ckey, cval in config.items():
			if ckey.upper() in globals():
				globals()[ckey.upper()] = cval
	except FileNotFoundError:
		print('Warning! File pengaturan tidak ditemukan, menggunakan pengaturan awal.')
	except Exception as e:
		print('Error!', e.args)

def save(**newconfig):
	_params: List[str] = [par for par in globals() if _satisfy(par)]
	# Get new value value in newconfig, or get current configuration by default
	config: Dict[str, Any] = {param: newconfig.get(param, globals()[param]) for param in _params}
	globals().update(config)

	with open('config.json', 'w', encoding='utf-8') as file:
		json.dump({ckey.lower(): cval for ckey, cval in config.items()}, file)

def store_default():
	global __DEFAULT__
	config = {pkey: pval for pkey, pval in globals().items() if _satisfy(pkey)}
	__DEFAULT__.update(config)

def get_config(*params: str) -> Dict[str, Any]:
	config = dict()
	for param in params:
		config[param] = globals()[param.upper()]
	return config

# Save default config first for fallback mode
load_dotenv()
store_default()
load()

PARAMETER_OFDB = [
	{
		'config_name': 'ofdb_hostname',
		'config_type': 'string',
		'config_label': 'Hostname',
		'config_group': 'Koneksi Database',
		'description': 'Hostname atau IP dari Offline Database.',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Tidak boleh kosong': lambda value: value!='',
				'IP tidak valid': lambda value: len(value.split('.'))==4
			}
		},
		'comp_props': {
			'dense': True,
			'filled': True,
			'square': True,
			'readonly': True,
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	},
	{
		'config_name': 'ofdb_port',
		'config_type': 'integer',
		'config_label': 'Port',
		'config_group': 'Koneksi Database',
		'description': 'Remote port dari Offline Database.',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Port tidak valid': lambda value: int(value)>0,
			}
		},
		'comp_props': {
			'dense': True,
			'filled': True,
			'square': True,
			'readonly': True,
			'type': 'number',
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	},
	{
		'config_name': 'ofdb_user',
		'config_type': 'string',
		'config_label': 'User',
		'config_group': 'Koneksi Database',
		'description': '',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Tidak boleh kosong': lambda value: value!='',
			}
		},
		'comp_props': {
			'dense': True,
			'filled': True,
			'square': True,
			'readonly': True,
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	},
	{
		'config_name': 'ofdb_token',
		'config_type': 'string',
		'config_label': 'Password',
		'config_group': 'Koneksi Database',
		'description': '',
		'comp': 'input',
		'comp_kwargs': {
			'password': True,
			'password_toggle_button': False
		},
		'comp_props': {
			'dense': True,
			'filled': True,
			'square': True,
			'readonly': True,
			'input-class': 'md:w-80',	# set width 320px (290px + 30px for toggle button) when in medium screen
		},
	},
	{
		'config_name': 'ofdb_database',
		'config_type': 'string',
		'config_label': 'Database',
		'config_group': 'Koneksi Database',
		'description': 'Nama database.',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Tidak boleh kosong': lambda value: value!='',
			}
		},
		'comp_props': {
			'dense': True,
			'filled': True,
			'square': True,
			'readonly': True,
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	},
	{
		'config_name': 'ofdb_driver',
		'config_type': 'string',
		'config_label': 'DB Driver',
		'config_group': 'Koneksi Database',
		'description': 'Driver untuk mengakses remote database.',
		'comp': 'select',
		'comp_kwargs': {
			'options': DB_DRIVERS,
			'value': OFDB_DRIVER
		},
		'comp_props': {
			'dense': True,
			'filled': True,
			'square': True,
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
		'comp_style': {
			'width': '344px'
		}
	},
]

PARAMETER_RCD = [
	{
		'config_name': 'calculate_bi',
		'config_type': 'bool',
		'config_label': 'PMS Bus',
		'config_group': 'Perhitungan',
		'description': 'Jika aktif, program akan menghitung keberhasilan kontrol PMT dan PMS Bus. Jika tidak, program hanya akan menghitung keberhasilan kontrol PMT. Default "Tidak Aktif"',
		'comp': 'switch',
		'comp_props': {
			'color': 'teal-5'
		},
	},
	{
		'config_name': 'check_repetition',
		'config_type': 'bool',
		'config_label': 'Repetisi Kontrol',
		'config_group': 'Perhitungan',
		'description': 'Jika aktif, program akan menghitung kegagalan kontrol berulang dalam hari yang sama sebagai satu kali gagal kontrol. Jika tidak, program akan menghitung semua kegagalan kontrol. Default "Aktif"',
		'comp': 'switch',
		'comp_props': {
			'color': 'teal-5'
		},
	},
	{
		'config_name': 'success_mark',
		'config_type': 'string',
		'config_label': 'Sukses',
		'config_group': 'Tag',
		'description': 'Tag yang menandai kontrol dinyatakan sukses/berhasil.',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Minimal 5 karakter': lambda value: len(value)>=5,
				'Harus diawali dan diakhiri dengan **, eg: **mark**': lambda value: value.startswith('**') and value.endswith('**'),
			}
		},
		'comp_props': {
			'dense': True,
			'hide-hint': True,
			'hint': 'Default **success**',
			'color': 'teal',
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	},
	{
		'config_name': 'failed_mark',
		'config_type': 'string',
		'config_label': 'Gagal',
		'config_group': 'Tag',
		'description': 'Tag yang menandai kontrol dinyatakan gagal.',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Minimal 5 karakter': lambda value: len(value)>=5,
				'Harus diawali dan diakhiri dengan **, eg: **mark**': lambda value: value.startswith('**') and value.endswith('**'),
			}
		},
		'comp_props': {
			'dense': True,
			'hide-hint': True,
			'hint': 'Default **failed**',
			'color': 'teal',
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	},
	{
		'config_name': 'unused_mark',
		'config_type': 'string',
		'config_label': 'Tidak dihitung',
		'config_group': 'Tag',
		'description': 'Tag yang menandai kontrol dinyatakan dianulir/tidak dihitung.',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Minimal 5 karakter': lambda value: len(value)>=5,
				'Harus diawali dan diakhiri dengan **, eg: **mark**': lambda value: value.startswith('**') and value.endswith('**'),
			}
		},
		'comp_props': {
			'dense': True,
			'hide-hint': True,
			'hint': 'Default **unused**',
			'color': 'teal',
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	},
	{
		'config_name': 'reduction_ratio_threshold',
		'config_type': 'number',
		'config_label': 'Batas Rasio Reduksi',
		'config_group': '',
		'description': 'Nilai rasio antara gagal Open/Close terhadap jumlah sukses & gagal sebagai acuan dalam pemberian rekomendasi tagging.',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Tidak boleh kosong': lambda value: value!='',
			}
		},
		'comp_props': {
			'dense': True,
			'hide-hint': True,
			'hint': 'Default 1.00',
			'type': 'number',
			'mask': '#.##',
			'fill-mask': '0',
			'color': 'teal',
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	}
]

PARAMETER_AVRS = [
	{
		'config_name': 'maintenance_mark',
		'config_type': 'string',
		'config_label': 'Pemeliharaan',
		'config_group': 'Tag (Gangguan Dengan Penyebab Pasti)',
		'description': 'Tag yang menandai downtime karena pemeliharaan peralatan.',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Minimal 5 karakter': lambda value: len(value)>=5,
				'Harus diawali dan diakhiri dengan **, eg: **mark**': lambda value: value.startswith('**') and value.endswith('**'),
			}
		},
		'comp_props': {
			'dense': True,
			'hide-hint': True,
			'hint': 'Default **maintenance**',
			'color': 'teal',
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	},
	{
		'config_name': 'link_failure_mark',
		'config_type': 'string',
		'config_label': 'Link/Telekomunikasi',
		'config_group': 'Tag (Gangguan Dengan Penyebab Pasti)',
		'description': 'Tag yang menandai downtime karena gangguan disisi link / peralatan telekomunikasi.',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Minimal 5 karakter': lambda value: len(value)>=5,
				'Harus diawali dan diakhiri dengan **, eg: **mark**': lambda value: value.startswith('**') and value.endswith('**'),
			}
		},
		'comp_props': {
			'dense': True,
			'hide-hint': True,
			'hint': 'Default **link**',
			'color': 'teal',
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	},
	{
		'config_name': 'rtu_failure_mark',
		'config_type': 'string',
		'config_label': 'Remote Station',
		'config_group': 'Tag (Gangguan Dengan Penyebab Pasti)',
		'description': 'Tag yang menandai downtime karena gangguan disisi peralatan Remote Station SCADA.',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Minimal 5 karakter': lambda value: len(value)>=5,
				'Harus diawali dan diakhiri dengan **, eg: **mark**': lambda value: value.startswith('**') and value.endswith('**'),
			}
		},
		'comp_props': {
			'dense': True,
			'hide-hint': True,
			'hint': 'Default **rtu**',
			'color': 'teal',
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	},
	{
		'config_name': 'other_failure_mark',
		'config_type': 'string',
		'config_label': 'Lain-lain',
		'config_group': 'Tag (Gangguan Dengan Penyebab Pasti)',
		'description': 'Tag yang menandai downtime karena gangguan peralatan lainnya, contoh (Catu Daya, Peripheral, dll.).',
		'comp': 'input',
		'comp_kwargs': {
			'validation': {
				'Minimal 5 karakter': lambda value: len(value)>=5,
				'Harus diawali dan diakhiri dengan **, eg: **mark**': lambda value: value.startswith('**') and value.endswith('**'),
			}
		},
		'comp_props': {
			'dense': True,
			'hide-hint': True,
			'hint': 'Default **other**',
			'color': 'teal',
			'input-class': 'md:w-80',	# set width 320px when in medium screen
		},
	},
	{
		'config_name': 'downtime_rules',
		'config_type': 'list',
		'config_label': 'Klasifikasi',
		'config_group': 'downtime',
		'description': 'Klasifikasi downtime berdasarkan lama/durasi down dalam satuan jam.',
		'comp': 'DowntimeRulesInput',
		'comp_kwargs': {
			'value': DOWNTIME_RULES
		},
	}
]
