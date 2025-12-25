import random

from availability.core.base import *
from availability.core.filereader import *
from availability.core.rcd import RCDConfig, RCDCore, RCDData, AvRCDResult, RCD
from availability.core.rtu import RTUConfig, RTUCore, RTUDownData, AvRTUResult, RTU
from availability.core.soe import SOE, SOEData, SOEModel, SurvalentSOEModel, SurvalentSPModel
from availability.core.main import *
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
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/2025_11_AV_RS_SUMMARY.xlsx',
]
file_rcd = [
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/07/AV_RCD_KDI_2025_07.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/08/AV_RCD_KDI_2025_08.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/09/AV_RCD_KDI_2025_09.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/10/AV_RCD_KDI_2025_10.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/AV_RCD_KDI_2025_11.xlsx',
]
file_rtu = [
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/07/AV_RS_KDI_2025_07.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/08/AV_RS_KDI_2025_08.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/09/AV_RS_KDI_2025_09.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/10/AV_RS_KDI_2025_10.xlsx',
	'/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/AV_RS_KDI_2025_11.xlsx',
]

rcdcfg = RCDConfig(master='survalent')
rtucfg = RTUConfig(master='survalent', rtu_file_name='rtu_sultra.yaml', known_rtus_only=True)


# reader = FileReader(SurvalentSOEModel, SurvalentSPModel, files=soe_survalent + sts_survalent + ['/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/2025_11_AV_RS_SUMMARY.xlsx', '/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/2025_11_Event_Log_Summary.xlsx'])
# print()
# t1 = time.perf_counter()
# f1 = reader.load()
# t2 = time.perf_counter()
# print()
# print('Old code :', t2-t1, 's')


# reader = FileReader(SurvalentSOEModel, SurvalentSPModel, files=['/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/2025_11_AV_RS_SUMMARY.xlsx', '/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/2025_11_Event_Log_Summary.xlsx'])
# f1 = reader.load()
# soe = SOE(data=f1, config=rcdcfg, sources=reader.sources)
# print(soe.data)
# rcd = RCDCore(soe.data, rcdcfg)
# print()
# t1 = time.perf_counter()
# r1 = rcd.fast_analyze(start_date=datetime.datetime(2025,11,1), end_date=datetime.datetime(2025,11,30,23,59,59,999999))
# t2 = time.perf_counter()
# print()
# r2 = rcd.fast_analyze2(start_date=datetime.datetime(2025,11,1), end_date=datetime.datetime(2025,11,30,23,59,59,999999))
# t3 = time.perf_counter()
# print('Old code :', t2-t1, 's')
# print('New code :', t3-t2, 's')


# reader = FileReader(SurvalentSOEModel, SurvalentSPModel)
# f1 = reader.load(soe_survalent)
# soe = SOE(data=f1, config=rtucfg, sources=reader.sources)
# print(soe.data)
# rtu = RTUCore(soe.data, rtucfg)
# print()
# t1 = time.perf_counter()
# r1 = rtu.fast_analyze(start_date=datetime.datetime(2025,11,1), end_date=datetime.datetime(2025,11,30,23,59,59,999999))
# t2 = time.perf_counter()
# print()
# r2 = rtu.fast_analyze2(start_date=datetime.datetime(2025,11,1), end_date=datetime.datetime(2025,11,30,23,59,59,999999))
# t3 = time.perf_counter()
# print('Old code :', t2-t1, 's')
# print('New code :', t3-t2, 's')


# rcd = RCD(rcdcfg)
# dfsoe = rcd.read_soe_file('/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/2025_11_AV_RS_SUMMARY.xlsx, /media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/2025_11_Event_Log_Summary.xlsx')
# soe = SOE(data=dfsoe, config=rcdcfg, sources=rcd.reader.sources)
# dfrcd = rcd.analyze_soe(soe.data)
# dfrcd = rcd.read_file(file_rcd)
# result = rcd.calculate(start_date=datetime.datetime(2025,7,1), end_date=datetime.datetime(2025,10,31,23,59,59,999999))
# rcd.write_file()


# rtu = RTU(rtucfg)
# dfsoe = rtu.read_soe_file('/media/shared-ntfs/2-fasop-kendari/Laporan_EOB/2025/11/2025_11_AV_RS_SUMMARY.xlsx')
# soe = SOE(data=dfsoe, config=rtucfg, sources=rtu.reader.sources)
# dfrtu = rtu.analyze_soe(soe.data)
# # dfrtu = rtu.read_file(file_rtu)
# result = rtu.calculate(start_date=datetime.datetime(2025,11,1), end_date=datetime.datetime(2025,11,30,23,59,59,999999))
# rtu.write_file()

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