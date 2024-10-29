import asyncio
from typing import Any, Dict, List, Callable, Generator, Literal, Iterable, Optional, Tuple, TypeAlias, Union

from .lib import CONSOLE_WIDTH


def test_file(handler, **params):
	def asynced():
		print(f' TEST FILE {title.upper()} ASYNCHRONOUS '.center(CONSOLE_WIDTH, '#'))
		obj2 = handler(filepaths)
		res2, err2, t2 = asyncio.run(obj2.async_load())
		return obj2, t2
	
	def optimized():
		print(f' TEST FILE {title.upper()} MULTIPROCESS '.center(CONSOLE_WIDTH, '#'))
		obj1 = handler(filepaths)
		res1, err1, t1 = obj1.load()
		return obj1, t1

	title: str = params.get('title', '')
	filepaths: str = params.get('filepaths', '')
	print(f' TEST FILE {title.upper()} '.center(CONSOLE_WIDTH, '#'))
	obj = handler(filepaths)
	res = obj.load()


def test_analyze(handler, **params):
	def asynced():
		print(f' TEST ANALYZE {title.upper()} ASYNCHRONOUS '.center(CONSOLE_WIDTH, '#'))
		obj2 = handler(filepaths)
		res2 = obj2.load()
		_ = asyncio.run(obj2.async_calculate(force=True, test=True))
		return obj2, obj2.process_duration
	
	def optimized():
		print(f' TEST ANALYZE {title.upper()} MULTIPROCESS '.center(CONSOLE_WIDTH, '#'))
		obj1 = handler(filepaths)
		res1 = obj1.load()
		_ = obj1.calculate(force=True, test=True)
		return obj1, obj1.process_duration

	def basic():
		print(f' TEST ANALYZE {title.upper()} SYNCHRONOUS '.center(CONSOLE_WIDTH, '#'))
		obj0 = handler(filepaths)
		res0 = obj0.load()
		_ = obj0.calculate(force=True, fast=False, test=True)
		return obj0, obj0.process_duration
	
	def compare():
		print(f' TEST COMPARE ANALYZE {title.upper()} '.center(CONSOLE_WIDTH, '#'))
		obj1, dt1 = optimized()
		obj2, dt2 = asynced()
		obj0, dt0 = basic()
		print(f"""
{'#'*CONSOLE_WIDTH}
HASIL PERBANDINGAN
 - Synchronous\t: {dt0:.3f}s
 - Multiprocess\t: {dt1:.3f}s\t({dt0/dt1:.1f}x lebih cepat)
 - Combined\t: {dt2:.3f}s\t({dt0/dt2:.1f}x lebih cepat)
SELESAI
{'#'*CONSOLE_WIDTH}
""")
		return obj1, dt1

	subtest_list = [
		('Test analisa SOE', basic),
		('Test analisa SOE (Basic Multiprocess)', optimized),
		('Test analisa SOE (Asyncio & Multiprocess)', asynced),
		('Bandingkan test', compare)
	]
	title: str = params.get('title', '')
	filepaths: str = params.get('filepaths', '')
	master: str = params.get('master', 'spectrum')
	print('\r\n'.join([f'{no+1}.'.ljust(6) + tst[0] for no, tst in enumerate(subtest_list)]))
	choice = int(input(f'\r\nPilih submodul test [1-{len(subtest_list)}] :  ')) - 1
	if choice in range(len(subtest_list)):
		print()
		obj, dt = subtest_list[choice][1]()
		if 'y' in input('Export hasil test? [y/n]  '):
			obj.to_excel(filename=f'test_analyze_{title.lower()}_{master.lower()}')
	else:
		print('Pilihan tidak valid!')
	return obj

def test_collective(handler, **params):
	title: str = params.get('title', '')
	filepaths: str = params.get('filepaths', '')
	obj = handler(filepaths)
	print()
	print(f' TEST COLLECTIVE {title.upper()} '.center(CONSOLE_WIDTH, '#'))
	res = obj.load()
	print(res)
	obj.calculate()
	if 'y' in input('Export hasil test? [y/n]  '):
		obj.to_excel(filename=f'test_collective_{title.lower()}')
	return obj

def main():
	pass
    

if __name__=='__main__':
	main()