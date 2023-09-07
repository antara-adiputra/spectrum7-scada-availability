from filereader import SpectrumFileReader
from rcd import RCAnalyzer


def main():
    global f
    fpath = r'/media/shared-ntfs/1-scada-makassar/SCADA_RCD/2023/RC_Output_CB_2023_01-07_full_fix.xlsx'
    # fpath = r'/media/shared-ntfs/1-scada-makassar/SCADA_RCD/2023/RC_Output_CB_2023_01_fix.xlsx,/media/shared-ntfs/1-scada-makassar/SCADA_RCD/2023/RC_Output_CB_2023_05_fix.xlsx,/media/shared-ntfs/1-scada-makassar/SCADA_RCD/2023/RC_Output_CB_2023_06_fix.xlsx,/media/shared-ntfs/1-scada-makassar/SCADA_RCD/2023/RC_Output_CB_2023_08_fix.xlsx'
    f = SpectrumFileReader(fpath)
    f.load()
    # rc = RCAnalyzer(f)
    # rc.calculate()
    # rc.export_result()
    

if __name__=='__main__':
    main()