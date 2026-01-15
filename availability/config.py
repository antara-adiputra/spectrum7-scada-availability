# This is user configurable variable which can be updated on runtime 

import json, pyodbc
import pandas as pd
from configparser import ConfigParser
from dataclasses import dataclass, fields
from dotenv import load_dotenv
from glob import glob
import yaml
try:
	from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
	from yaml import Loader, Dumper

from . import settings
from .lib import load_cpoint, logprint
from .types import *


__conf = ConfigParser(default_section='GENERAL')
__conf.read('.config')

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


def try_load_file_config() -> Optional[Dict[str, Any]]:
	file = 'config.yaml'
	try:
		cfg = yaml.safe_load(open(file))
	except FileNotFoundError:
		cfg = None
		logprint('No configuration found. Create a new one.', level='info')
	except Exception:
		cfg = None
	finally:
		return cfg

def load_rtunames_config() -> Dict[str, Dict[str, str]]:
	logprint('Loading RTU names config...', level='info')
	config_paths = glob(str(settings.CONFIG_DIR / 'rtu*.yaml'))
	return {path.split('/')[-1]: yaml.safe_load(open(path)) for path in config_paths}

def dump_rtunames_config(data: Dict[str, str], filename: str):
	if not filename.startswith('rtu'):
		filename = 'rtu_' + filename

	if '.yaml' not in filename:
		filename += '.yaml'

	yaml.safe_dump(data, open(settings.CONFIG_DIR / filename, 'w'))
	logprint(f'RTU names config "{filename}" is saved', level='info')

def load_point_library() -> pd.DataFrame:
	logprint('Loading point library...', level='info')
	file_paths = str(settings.CONFIG_DIR / 'cpoint*.xlsx')
	return load_cpoint(file_paths)

# Save default config first for fallback mode
load_dotenv()

RTU_NAMES_CONFIG = load_rtunames_config()
POINT_LIBRARY = load_point_library()