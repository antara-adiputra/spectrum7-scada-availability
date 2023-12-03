import re
from configparser import ConfigParser
from glob import glob
from typing import Union

import pandas as pd
from global_parameters import RCD_COLUMNS, SOE_COLUMNS, SOE_COLUMNS_DTYPE
from lib import calc_time, immutable_dict, load_cpoint, load_workbook, read_xml, test_datetime_format


class _FileReader:
	__slot__ = ['switching_element']
	column_list = []
	keep_duplicate = 'last'
	sheet_name = 'Sheet1'
	time_series_column = 'Timestamp'

	def __init__(self, filepaths:Union[str, list], **kwargs):
		self._date_range = None
		self.cached_file = {}
		self.filepaths = []

		if isinstance(filepaths, str):
			for f in filepaths.split(','):
				if '*' in f:
					self.filepaths += glob(f.strip())
				elif f.strip():
					self.filepaths.append(f.strip())
		elif isinstance(filepaths, list):
			self.filepaths = filepaths

		self.switching_element = kwargs['switching_element'] if 'switching_element' in kwargs else ['CB']
			
		# Set date_range if defined in kwargs
		if 'date_start' in kwargs and 'date_stop' in kwargs:
			self.set_date_range(
				date_start=kwargs['date_start'].replace(hour=0, minute=0, second=0, microsecond=0),
				date_stop=kwargs['date_stop'].replace(hour=23, minute=59, second=59, microsecond=999999)
			)

		# Dynamically update defined variable
		# This code should be placed at the end of __init__
		for key, value in kwargs.items():
			if key in self.__slot__:
				setattr(self, key, value)

		df = self.load()
		self.post_load(df)
		# Need this for cooperative multiple-inheritance
		super().__init__(**kwargs)

	@calc_time
	def load(self, **kwargs):
		"""
		Load every file in filepaths.
		"""

		data = []

		if self.filepaths:
			# If paths is not None
			for file in self.filepaths:
				df = self.open_file(filepath=file)
				if isinstance(df, pd.DataFrame):
					df = self.post_open_file(df)
					data.append(df)
				else:
					print('Gagal.', end='', flush=True)
					raise ValueError('Gagal mengimport file.')

			# Combine each Dataframe in data list into one and eleminate duplicated rows
			df_merged = pd.concat(data).drop_duplicates(keep=self.keep_duplicate)
		
			self.sources = ',\n'.join(self.filepaths)
			print('Selesai.', end='', flush=True)
			return df_merged
		else:
			raise FileExistsError('Error lokasi file.')

	def post_load(self, df:pd.DataFrame):
		"""
		Executed after load completed.
		"""

		if self._date_range==None: self.set_date_range(date_start=df[self.time_series_column].min(), date_stop=df[self.time_series_column].max())

	def open_file(self, filepath:str, **kwargs):
		"""
		Loads single excel file into pandas Dataframe.
		"""

		wb = {}
		df = None

		print(f'Membuka file "{filepath}"...', end='', flush=True)
		try:
			wb = load_workbook(filepath)
			if self.sheet_name in wb:
				ws = wb[self.sheet_name]
				if set(self.column_list).issubset(ws.columns):
					df = ws[ws[self.time_series_column].notnull()].fillna('')
					print('\tOK!')
			else:
				for ws_name, sheet in wb.items():
					if set(self.column_list).issubset(sheet.columns):
						df = sheet[sheet[self.time_series_column].notnull()].fillna('')
						print('\tOK!')
						break
				
				if df==None: print(f'\nData "{self.sheet_name}" tidak ditemukan!')

		except (ValueError, ImportError):
			try:
				# Retry to open file with xml format
				df = read_xml(filepath, **kwargs)
			except (ValueError, ImportError):
				print('\tNOK!\nGagal membuka file.')
		except FileNotFoundError:
			print('\tNOK!\nFile tidak ditemukan.')

		self.cached_file[filepath] = df

		return df

	def post_open_file(self, df:pd.DataFrame):
		"""
		Post processing after file opened.
		"""

		return df

	def set_date_range(self, date_start, date_stop):
		"""
		"""

		dtstart = date_start.to_pydatetime() if isinstance(date_start, pd.Timestamp) else date_start
		dtstop = date_stop.to_pydatetime() if isinstance(date_stop, pd.Timestamp) else date_stop

		self._date_start = dtstart
		self._date_stop = dtstop
		self._date_range = (dtstart, dtstop)


	@property
	def date_range(self):
		return self._date_range

	@property
	def date_start(self):
		return self._date_start

	@property
	def date_stop(self):
		return self._date_stop


class SpectrumFileReader(_FileReader):
	column_dtype = immutable_dict(SOE_COLUMNS_DTYPE)
	column_list = SOE_COLUMNS
	sheet_name = 'HIS_MESSAGES'
	time_series_column = 'Time stamp'
	cpoint_file = 'cpoint.xlsx'

	def __init__(self, filepaths:Union[str, list], **kwargs):
		# Need this for cooperative multiple-inheritance
		super().__init__(filepaths, **kwargs)
		# Load point description
		self.cpoint_description = load_cpoint(self.cpoint_file)

	def post_load(self, df:pd.DataFrame):
		"""
		Executed after load completed.
		"""

		super().post_load(df)
		self._soe_all = self.prepare_data(df)

	def post_open_file(self, df:pd.DataFrame):
		"""
		"""

		if pd.api.types.is_object_dtype(df['Time stamp']) or pd.api.types.is_object_dtype(df['System time stamp']):
			df['Time stamp'] = df['Time stamp'].map(lambda x: test_datetime_format(x))
			df['System time stamp'] = df['System time stamp'].map(lambda x: test_datetime_format(x))
		# Format B2 as string value
		df['B2'] = df['B2'].map(lambda x: re.sub(r'\.\d+', '', str(x)))

		return super().post_open_file(df)

	def prepare_data(self, df:pd.DataFrame, **kwargs):
		"""
		"""

		col1 = ['B1', 'B2', 'B3']
		col2 = ['B1 text', 'B2 text', 'B3 text']

		for col in df.columns:
			# Remove unnecessary spaces on begining and or trailing string object
			if pd.api.types.is_object_dtype(df[col]): df[col] = df[col].str.strip()

		# Filter new DataFrame
		new_df = df.loc[(df['A']=='') & (df[self.time_series_column]>=self.date_range[0]) & (df[self.time_series_column]<=self.date_range[1]), self.column_list].copy()
		new_dftype = {key: value for key, value in self.column_dtype.items() if key in new_df.columns}

		new_df = new_df.astype(new_dftype).fillna('')
		new_df = new_df.sort_values(['System time stamp', 'System milliseconds', 'Time stamp', 'Milliseconds'], ascending=[True, True, True, True]).reset_index(drop=True)
		new_df['Status'] = new_df['Status'].str.title()

		# Change B1, B2, B3 from description style into mnemonic style
		if hasattr(self, 'cpoint_description'):
			df_trans = self.cpoint_description.copy()
			is_longtext = new_df['B1'].str.len().max()>9 or new_df['B3'].str.len().max()>9
			if is_longtext:
				# Swap column labels, because exported His. Messages using description text
				# ['B1', 'B2', 'B3', 'B1 text', 'B2 text', 'B3 text'] -> ['B1 text', 'B2 text', 'B3 text', 'B1', 'B2', 'B3']
				df_trans.columns = col2 + col1
			# Double check duplicated keys
			df_trans.drop_duplicates(subset=col1, keep='first', inplace=True)
			# Merge B1, B2, B3 translation with existing table
			new_df = new_df.merge(df_trans, on=col1, how='left')
			without_description = new_df['B1 text'].isna()

			if new_df[without_description].shape[0]>0:
				# List unknown (nan) Point Description
				no_description = new_df.loc[without_description, col1].drop_duplicates(keep='first').values
				print(f'{len(no_description)} poin tidak terdaftar dalam "Point Description".\n{"; ".join([str(x) for i, x in enumerate(no_description) if i<5])}{" ..." if len(no_description)>5 else ""}\nSilahkan update melalui SpectrumOfdbClient atau menambahkan manual pada file cpoint.xlsx!')
				# Fill unknown (nan) Point Description B1, B2, B3 with its own text
				new_df.loc[without_description, col2] = new_df.loc[without_description, col1].values

			if is_longtext:
				# Swap column labels, because exported His. Messages using description text
				new_df[col1 + col2] = new_df[col2 + col1]
			# Rearrange columns
			new_df = new_df[SOE_COLUMNS + col2]

		# Split into DataFrames for each purposes, not reset index
		self._soe_control_disable = new_df[(new_df['Element']=='CD') & (new_df['Status'].isin(['Disable', 'Enable', 'Dist.']))].copy()
		self._soe_local_remote = new_df[(new_df['Element']=='LR') & (new_df['Status'].isin(['Local', 'Remote', 'Dist.']))].copy()
		self._soe_rtu_updown = new_df[(new_df['B1']=='IFS') & (new_df['B2']=='RTU_P1') & (new_df['Status'].isin(['Up', 'Down']))].copy()
		self._soe_switching = new_df[(new_df['Element'].isin(self.switching_element)) & (new_df['Status'].isin(['Open', 'Close', 'Dist.']))].copy()
		self._soe_synchro = new_df[(new_df['Element']=='CSO') & (new_df['Status'].isin(['Off', 'On', 'Dist.']))].copy()
		self._soe_trip = new_df[new_df['Element'].isin(['CBTR', 'MTO'])].copy()

		return new_df


	@property
	def soe_all(self):
		return self._soe_all if hasattr(self, '_soe_all') else self.load()

	@property
	def soe_control_disable(self):
		return self._soe_control_disable if hasattr(self, '_soe_control_disable') else self.load()

	@property
	def soe_local_remote(self):
		return self._soe_local_remote if hasattr(self, '_soe_local_remote') else self.load()

	@property
	def soe_rtu_updown(self):
		return self._soe_rtu_updown if hasattr(self, '_soe_rtu_updown') else self.load()

	@property
	def soe_switching(self):
		return self._soe_switching if hasattr(self, '_soe_switching') else self.load()

	@property
	def soe_synchro(self):
		return self._soe_synchro if hasattr(self, '_soe_synchro') else self.load()

	@property
	def soe_trip(self):
		return self._soe_trip if hasattr(self, '_soe_trip') else self.load()
	

class RCFileReader(_FileReader):
	column_list = RCD_COLUMNS
	sheet_name = 'RC_ONLY'
	time_series_column = 'Order Time'

	def __init__(self, filepaths:Union[str, list], **kwargs):
		# Need this for cooperative multiple-inheritance
		super().__init__(filepaths, **kwargs)

	def post_load(self, df:pd.DataFrame):
		"""
		Executed after load completed.
		"""

		super().post_load(df)
		self._rcd_all = self.prepare_data(df)

	def post_open_file(self, df:pd.DataFrame):
		"""
		"""

		# Format B2 as string value
		df['B2'] = df['B2'].map(lambda x: re.sub(r'\.\d+', '', str(x)))

		return super().post_open_file(df)

	def prepare_data(self, df:pd.DataFrame, **kwargs):
		"""
		"""

		# Filter new DataFrame
		new_df = df.loc[(df[self.time_series_column]>=self.date_range[0]) & (df[self.time_series_column]<=self.date_range[1]), self.column_list].copy()
		new_df = new_df.sort_values([self.time_series_column], ascending=[True]).reset_index(drop=True)

		return new_df


	@property
	def rcd_all(self):
		return self._rcd_all if hasattr(self, '_rcd_all') else self.load()


def main():
	pass

if __name__=='__main__':
	main()