import random

from availability.core.base import *
from availability.core.filereader import *
from availability.core.rcd import RCDConfig, RCDCore, RCDData, AvRCDResult, RCD
from availability.core.rtu import RTUConfig, RTUCore, RTUDownData, AvRTUResult, RTU
from availability.core.soe import SOE, SOEData, SOEModel, SurvalentSOEModel, SurvalentSPModel
from availability.core.main import *
from availability.webgui.state import AvStateWrapper
from availability import config
# from availability.utils.writer import *

def list_of_int(n: int = 3) -> List[int]:
	return list(map(lambda _: random.randint(1, 100), range(n)))

class X(TypedDict):
	a: str
	b: int
	c: List[str]


x: X = {'a': '123', 'b': 123, 'c': [1,2,3]}

soe_spectrum = [
	'/media/shared-ntfs/1-scada-makassar/AVAILABILITY/2025/HISWebUI_spectrum_DATA-MESSAGES_202503*.xlsx'
]
soe_survalent = [
	# '/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/SOE_Survalent/EVENT_RC-2025_06.XLSX',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/07/EVENT_RCC_2025_07_SUMMARY.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/SOE_Survalent/EVENT_RC-2025_08.XLSX',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/SOE_Survalent/2025_09_Event_Log_Summary.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/10/2025_10_Event_RC_Summary.xlsx',
]
sts_survalent = [
	# '/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/SOE_Survalent/EVENT_RS-2025_06.XLSX',
	# '/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/SOE_Survalent/EVENT_RS-2025_07.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/SOE_Survalent/EVENT_RS-2025_08.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/SOE_Survalent/2025_09_Status_Point_SUMMARY.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/10/2025_10_AV_RS_SUMMARY.xlsx',
]
file_rcd = [
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/07/AV_RCD_KDI_2025_07.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/08/AV_RCD_KDI_2025_08.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/09/AV_RCD_KDI_2025_09.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/10/AV_RCD_KDI_2025_10.xlsx',
]
file_rtu = [
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/07/AV_RS_KDI_2025_07.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/08/AV_RS_KDI_2025_08.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/09/AV_RS_KDI_2025_09.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/10/AV_RS_KDI_2025_10.xlsx',
]


cfg = None
bases = None
files = None
start_date = None
end_date = None
fr = None
df = None
soe = None
av = None
df1 = None
data = None
result = None
writer = None

def test_avrcd(master: SCDMasterType):
	global cfg, bases, files, start_date, end_date, fr, df, soe, av, df1, data, result
	if master=='spectrum':
		bases = (SOEModel,)
		files = soe_spectrum
		start_date = datetime.datetime(2025,3,1)
		end_date = datetime.datetime(2025,3,31,23,59,59,999999)
	else:
		bases = (SurvalentSOEModel, SurvalentSPModel)
		files = soe_survalent + sts_survalent
		start_date = datetime.datetime(2025,8,1)
		end_date = datetime.datetime(2025,9,30,23,59,59,999999)

	cfg = RCDConfig(master=master)
	fr = FileReader(*bases, files=files)
	df = fr.load()
	soe = SOEData(df, rc_element=cfg.elements)
	av = RCDCore(soe, config=cfg)
	df1 = av.fast_analyze(start_date=start_date, end_date=end_date)
	data = RCDData(df1, config=cfg, start_date=start_date, end_date=end_date)
	result = AvRCDResult(data=data)

def test_avrtu(master: SCDMasterType):
	global cfg, bases, files, start_date, end_date, fr, df, soe, av, df1, data, result
	if master=='spectrum':
		bases = (SOEModel,)
		files = soe_spectrum
		start_date = datetime.datetime(2025,3,1)
		end_date = datetime.datetime(2025,3,31,23,59,59,999999)
	else:
		bases = (SurvalentSOEModel, SurvalentSPModel)
		files = soe_survalent + sts_survalent
		start_date = datetime.datetime(2025,8,1)
		end_date = datetime.datetime(2025,9,30,23,59,59,999999)

	cfg = RTUConfig(master=master)
	fr = FileReader(*bases, files=files)
	df = fr.load()
	soe = SOEData(df)
	av = RTUCore(soe, config=cfg)
	df1 = av.fast_analyze(start_date=start_date, end_date=end_date)
	data = RTUDownData(df1, config=cfg, start_date=start_date, end_date=end_date)
	result = AvRTUResult(data=data)

# rtu_names = config.RTU_NAMES_CONFIG['rtu_sultra.yaml']
# conf = RTUConfig(master='survalent', rtu_names=rtu_names, known_rtus_only=True)
# fr = SurvalentStatusFileReader(sts_survalent[-1])
# df =  pd.read_excel(sts_survalent[-1])
# obj = SurvalentSPModel.from_dataframe(df)
# df = obj.to_dataframe()
# soe = SOEData(df)
# av = RTUCore(soe, config=conf)
# df1 = av.fast_analyze(start_date=datetime.datetime(2025,9,1), end_date=datetime.datetime(2025,9,30))
# result = RTUDownData(df1, config=conf, start_date=datetime.datetime(2025,9,1), end_date=datetime.datetime(2025,9,30,23,59,59,999999))
# stats = AvRTUStatistics(data=result)

rcdcfg = RCDConfig(master='survalent')
rtucfg = RTUConfig(master='survalent', rtu_names=config.RTU_NAMES_CONFIG['rtu_sultra.yaml'], known_rtus_only=True)
# rtu = RTU(rtucfg)
# data = rtu.read_file('/home/pyproject/spectrum7-scada-availability/output/AV_RS_Survalent_Output_20251001-20251031_rev1.xlsx')
# result = rtu.calculate(start_date=datetime.datetime(2025,10,1), end_date=datetime.datetime(2025,10,31,23,59,59,999999))
# rtu.write_file()

# cfg = RTUConfig()
# rtu = RTU(cfg)
# data = rtu.read_file('/media/shared-ntfs/1-scada-makassar/AVAILABILITY/2025/RTU/AVRS_Output_2025*.xlsx')
# result = rtu.calculate(start_date=datetime.datetime(2025,1,1), end_date=datetime.datetime(2025,9,30,23,59,59,999999))

# cfg = RCDConfig(master='survalent')
# rcd = RCD(cfg)
# data = rcd.read_file('/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/*/AV_RCD_KDI_2025_*.xlsx')
# result = rcd.calculate(start_date=datetime.datetime(2025,7,1), end_date=datetime.datetime(2025,9,30,23,59,59,999999))

# fr0 = RCFileReader(file_rcd)
# df0 = fr0.load()
# df0.to_excel('test_load_rcd.xlsx', index=False)

# fr1 = RSFileReader(file_rtu)
# df1 = fr1.load()
# df1.to_excel('test_load_rtu.xlsx', index=False)

# df = SpectrumFileReader(files).load()
# rc1 = SOEtoRCD()
# rc1.soe_all = df
# rc2 = _SOEAnalyzeTool(df)

# def _compare_df(df1: pd.DataFrame, df2: pd.DataFrame):
# 	print(df1.shape, df2.shape)
# 	for col in df1.columns:
# 		if any(df1[col]!=df2[col]): print(col, ': Differs!')

# def compare_his():
# 	_compare_df(rc1.soe_all, rc2.data.his)

# def compare_csw():
# 	_compare_df(rc1.soe_ctrl, rc2.data.csw)

# def compare_cso():
# 	_compare_df(rc1.soe_sync, rc2.data.cso)

# def compare_cd():
# 	_compare_df(rc1.soe_cd, rc2.data.cd)

# def compare_ifs():
# 	_compare_df(rc1.soe_ifs, rc2.data.ifs)

# def compare_lr():
# 	_compare_df(rc1.soe_lr, rc2.data.lr)

# def compare_prt():
# 	_compare_df(rc1.soe_prot, rc2.data.prt)

# def compare_speed1():
# 	t0 = time.time()
# 	_ = rc1.analyze()
# 	print('Old script :', time.time()-t0, 's')
# 	t1 = time.time()
# 	rcd_all = rc2.analyze()
# 	result = RCStatistics(rcd_all, rc2.config)
# 	print('New script :', time.time()-t1, 's')

# def compare_speed2():
# 	t0 = time.time()
# 	_ = rc1.fast_analyze()
# 	print('Old script :', time.time()-t0, 's')
# 	t1 = time.time()
# 	rcd_all = rc2.fast_analyze()
# 	result = RCStatistics(rcd_all, rc2.config)
# 	print('New script :', time.time()-t1, 's')
# 	t2 = time.time()
# 	rcd_all1 = asyncio.run(rc2.async_analyze())
# 	result1 = RCStatistics(rcd_all1, rc2.config)
# 	print('New script (async) :', time.time()-t2, 's')
# 	_compare_df(rcd_all, rcd_all1)

# def compare_annotations():
# 	rcd_all = rc2.fast_analyze()
# 	fltr = rc1.rcd_all['Annotations']!=rcd_all['Annotations']
# 	for i in fltr.index:
# 		if fltr[i]==np.True_: print(i, repr(rc1.rcd_all.loc[i, 'Annotations']), repr(rcd_all.loc[i, 'Annotations']), sep='\n')


# av = AvRCFromFile('/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/07/EVENT_RCC_2025_07_SUMMARY.xlsx', server='survalent')
# wrapped = AvStateWrapper(av)
# av.load()
# av._core.data.his.to_excel('test_dump.xlsx')
# av.calculate()
# av.to_excel()

# writer = RCFileWriter(
# 	his_data=av._core.data.get_cleaned_his(),
# 	rcd_data=av.result.all,
# 	gi_data=av.result.station,
# 	bay_data=av.result.bay,
# 	opr_data=av.result.operator,
# 	server='spectrum'
# )
# rd = SurvalentStatusFileReader('/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/RCD/SOE_Survalent/AV_RS_07_SUMMARY.xlsx')
# df = rd.load()
# soe = SOEData(df, ())
# anz = RTUEventAnalyzeTool(data=soe)
# df_down = anz.fast_analyze()
# av = AVRSCollective()
# av.rtudown_all = df_down
# av.calculate()