import pandas as pd
import re
from datetime import datetime
from glob import glob
from global_parameters import df_columns, df_columns_dtype
from lib import calc_time, immutable_dict, read_xls, read_xml, similarity_ratio, test_datetime_format
from pathlib import Path
from typing import Union


class SpectrumFileReader:
	column_dtype = immutable_dict(df_columns_dtype)
	
	def __init__(self, filepaths:Union[str, list], **kwargs):
		self.cpoint_file = 'cpoint.xlsx'
		self.keep_duplicate = 'last'

		if isinstance(filepaths, str):
			self.filepaths = []
			for f in filepaths.split(','):
				if '*' in f:
					self.filepaths += glob(f.strip())
				elif f.strip():
					self.filepaths.append(f.strip())
		elif isinstance(filepaths, list):
			self.filepaths = filepaths
		else:
			self.filepaths = None

		self.switching_element = kwargs['switching_element'] if 'switching_element' in kwargs else ['CB']
			
		# Set date_range if defined in kwargs
		if 'date_start' in kwargs and 'date_stop' in kwargs:
			self._date_range = (kwargs['date_start'].replace(hour=0, minute=0, second=0, microsecond=0), kwargs['date_stop'].replace(hour=23, minute=59, second=59, microsecond=999999))
		else:
			self._date_range = None

	@calc_time
	def load(self, force:bool=False, **kwargs):
		"""
		Load every file in filepaths.
		"""

		if force or not hasattr(self, '_soe_all'):
			data = []

			if self.filepaths:
				# If paths is not None
				for file in self.filepaths:
					df = self.load_file(file)
					if type(df)==pd.DataFrame:
						data.append(df)
					else:
						return print('Gagal.', end='', flush=True)

			# Combine each Dataframe in data list into one and eleminate duplicated rows
			df = pd.concat(data).drop_duplicates(keep=self.keep_duplicate)

			# Load point description
			print('Memuat data "Point Name Description"...', end='', flush=True)
			try:
				df_cpoint = read_xls(self.cpoint_file, is_soe=False).fillna('')
				# Remove duplicates to prevent duplication in merge process
				self.cpoint_description = self.validate_cpoint(df_cpoint)

				print('\tOK!')
			except FileNotFoundError:
				raise FileNotFoundError(f'\tNOK!\nFile "{self.cpoint_file}" tidak ditemukan.')
			except Exception:
				raise ValueError(f'\tNOK!\nGagal membuka file "{self.cpoint_file}".')

			if self._date_range==None: self._date_range = (df['Time stamp'].min(), df['Time stamp'].max())
			self.sources = ',\n'.join(self.filepaths)
			
			self._soe_all = self.prepare_data(df, **kwargs)

			print('Selesai.', end='', flush=True)

	def load_file(self, filepath:str):
		"""
		Loads single excel file into pandas Dataframe.
		"""

		wb_path = Path(filepath.replace('"', ''))
		df = None

		print(f'Membuka file "{filepath}"...', end='', flush=True)
		try:
			df = read_xls(wb_path)
		except (ValueError, ImportError):
			try:
				# Retry to open file with xml format
				df = read_xml(wb_path)
			except (ValueError, ImportError):
				print('\tNOK!\nGagal membuka file.')
		except FileNotFoundError:
			print('\tNOK!\nFile tidak ditemukan.')
		
		if type(df)==pd.DataFrame:
			if pd.api.types.is_object_dtype(df['Time stamp']) or pd.api.types.is_object_dtype(df['System time stamp']):
				df['Time stamp'] = df['Time stamp'].map(lambda x: test_datetime_format(x))
				df['System time stamp'] = df['System time stamp'].map(lambda x: test_datetime_format(x))
			# Format B2 as string value
			df['B2'] = df['B2'].map(lambda x: re.sub(r'\.\d+', '', str(x)))

			print('\tOK!')

		return df
	
	def prepare_data(self, df:pd.DataFrame, **kwargs):
		"""
		"""

		col1 = ['B1', 'B2', 'B3']
		col2 = ['B1 text', 'B2 text', 'B3 text']

		for col in df.columns:
			# Remove unnecessary spaces on begining and or trailing string object
			if pd.api.types.is_object_dtype(df[col]): df[col] = df[col].str.strip()

		# Filter new DataFrame
		new_df = df.loc[(df['A']=='') & (df['Time stamp']>=self.date_range[0]) & (df['Time stamp']<=self.date_range[1]), df_columns].copy()
		new_dftype = {key: value for key, value in self.column_dtype.items() if key in new_df.columns}

		new_df = new_df.astype(new_dftype).fillna('')
		new_df = new_df.sort_values(['System time stamp', 'System milliseconds', 'Time stamp', 'Milliseconds'], ascending=[True, True, True, True]).reset_index(drop=True)
		new_df['Status'] = new_df['Status'].str.title()

		# Change B1, B2, B3 from description style into mnemonic style
		if hasattr(self, 'cpoint_description'):
			df_trans = self.cpoint_description.copy()
			if new_df['B1'].str.len().max()>7 or new_df['B3'].str.len().max()>7:
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

			if new_df['B1'].str.len().max()>7 or new_df['B3'].str.len().max()>7:
				# Swap column labels, because exported His. Messages using description text
				# Swap columns name
				new_df[col1 + col2] = new_df[col2 + col1]
			# Rearrange columns
			new_df = new_df[df_columns + col2]

		# Split into DataFrames for each purposes, not reset index
		self._soe_control_disable = new_df[(new_df['Element']=='CD') & (new_df['Status'].isin(['Disable', 'Enable', 'Dist.']))].copy()
		self._soe_local_remote = new_df[(new_df['Element']=='LR') & (new_df['Status'].isin(['Local', 'Remote', 'Dist.']))].copy()
		self._soe_rtu_updown = new_df[(new_df['B1']=='IFS') & (new_df['B2']=='RTU_P1') & (new_df['Status'].isin(['Up', 'Down']))].copy()
		self._soe_switching = new_df[(new_df['Element'].isin(self.switching_element)) & (new_df['Status'].isin(['Open', 'Close', 'Dist.']))].copy()
		self._soe_synchro = new_df[(new_df['Element']=='CSO') & (new_df['Status'].isin(['Off', 'On', 'Dist.']))].copy()
		self._soe_trip = new_df[new_df['Element'].isin(['CBTR', 'MTO'])].copy()

		return new_df

	def validate_cpoint(self, df:pd.DataFrame, verbose:bool=False):
		"""
		"""

		columns_base = ['B1', 'B2', 'B3']
		columns_text = ['B1 text', 'B2 text', 'B3 text']

		new_df = df.copy().drop_duplicates(subset=columns_base+columns_text).sort_values(['B1', 'B2', 'B3'])
		# similarity ratio to get better description and remove unwanted data
		for col in columns_base:
			new_df[f'{col} ratio'] = new_df.apply(lambda d: similarity_ratio(d[col], d[f'{col} text']), axis=1)

		new_df['Ratio'] = new_df['B1 ratio'] * new_df['B2 ratio'] * new_df['B3 ratio']
		# get highest similarity ratio
		new_df = new_df[(new_df['B1']!='') & (new_df['Ratio']>0)]

		filter_highest_ratio = new_df.groupby(columns_base, as_index=False)['Ratio'].transform('max')==new_df['Ratio']
		new_df = new_df[filter_highest_ratio]

		if verbose:
			return new_df
		else:
			return new_df[columns_base + columns_text]
	
	@property
	def date_range(self):
		return self._date_range

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
	

def main():
	filepaths = input('Lokasi file : ')
	df = SpectrumFileReader(filepaths)
	print('Date Range\n', df.date_range)
	print('SOE All\n', df.soe_all)
	print('SOE CD\n', df.soe_control_disable)
	print('SOE LR\n', df.soe_local_remote)
	print('SOE IFS\n', df.soe_rtu_updown)
	print('SOE Switching\n', df.soe_switching)
	print('SOE Synchro\n', df.soe_synchro)
	print('SOE Trip\n', df.soe_trip)

if __name__=='__main__':
	main()