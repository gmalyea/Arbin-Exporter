import os
import math
import datetime
import pyodbc
import pandas as pd
import openpyxl
import openpyxl.utils.dataframe
from arbin.ArbinTest import ArbinTest
from arbin.ArbinWorkbook import ArbinWorkbook

# =============================================================================
# ArbinExport
# -----------------------------------------------------------------------------
# 
# 
# 
# 
#
# =============================================================================


# Constants: Excel
# -----------------------------------------------------------------------------
MAXDATAPOINTS = 900000
TIMEZONE = -5  # Hours


class ArbinExport( object ):

    # Class Initialization
    # -----------------------------------------------------------------------------
    def __init__( self, ArbinTest ):
        self.ArbinTest = ArbinTest
        self.wb_list = []
        
        # Excel has a maximum of 1,000,000 rows
        wb_count = math.ceil( ArbinTest.count_raw_data() / MAXDATAPOINTS )
        
        for wb_num in range(wb_count):
            file_name = ArbinTest.test_name
            if wb_num > 0:
                file_name = file_name + "_" + str(wb_num+1)
            
            wb = ArbinWorkbook(file_name)
            
            self.export_global_info_sheet( wb.ws1 )
            self.export_channel_sheet( wb.ws2, wb_num )
            self.export_statistics_sheet( wb.ws3 )
            
            self.wb_list.append( wb )


    def save_workbook( self, path ):
        for wb in self.wb_list:
            wb.save_workbook( path )
    

    def export_global_info_sheet( self, worksheet ):
        test_name = self.ArbinTest.test_name
        arbin_number = self.ArbinTest.arbin_number
        current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        
        worksheet.append(['Processed by exporter.py','','','TEST REPORT'])
        worksheet.append(['','','Test Name',test_name,'Serial Number'])
        worksheet.append(['','','Export Time',current_date,arbin_number])
        
        df = self.ArbinTest.global_info_df
        
        # Change Timezone and Format Date-Time
        df = self.convert_date_time( df, 'Start DateTime', 's', 1 )
        
        for row in openpyxl.utils.dataframe.dataframe_to_rows( df, index=False, header=True ):
            worksheet.append( row )
             
        # Format Cells       
        ArbinWorkbook.background_color( worksheet, 3, 'CEFFCE' ) # Green     
        ArbinWorkbook.border_bottom( worksheet, 3 )
        ArbinWorkbook.justify_right( worksheet, 'C2' )
        ArbinWorkbook.justify_right( worksheet, 'C3' )
        ArbinWorkbook.justify_left( worksheet, 'D2' )
        ArbinWorkbook.justify_left( worksheet, 'D3' )
        ArbinWorkbook.justify_right( worksheet, 'E2' )
        ArbinWorkbook.justify_right( worksheet, 'E3' )
        ArbinWorkbook.resize_cells( worksheet, slice(1,5) )
             

    def export_channel_sheet( self, worksheet, wb_num ):
        datapoint_start = wb_num * MAXDATAPOINTS
        datapoint_end = datapoint_start + MAXDATAPOINTS
      
        df = self.ArbinTest.raw_data_df.iloc[datapoint_start:datapoint_end]
        
        # Change Timezone and Format Date-Time
        df = self.convert_date_time( df, 'Date_Time', 'ns', 100 )
        
        for row in openpyxl.utils.dataframe.dataframe_to_rows( df, index=False, header=True ):
            worksheet.append( row )
    
        # Format Cells
        ArbinWorkbook.background_color( worksheet, 0, 'CEFFFF' ) # Blue 
        ArbinWorkbook.border_bottom( worksheet, 0 )
        ArbinWorkbook.resize_cells( worksheet, slice(0,2) )

    
    def export_statistics_sheet( self, worksheet ):
        df = self.ArbinTest.cycle_statistics_df
        
        # Change Timezone and Format Date-Time
        df = self.convert_date_time( df, 'Date_Time', 'ns', 100 )
        
        for row in openpyxl.utils.dataframe.dataframe_to_rows( df, index=False, header=True ):
            worksheet.append( row )
    
        # Format Cells
        ArbinWorkbook.background_color( worksheet, 0, 'CEFFFF' ) # Blue     
        ArbinWorkbook.border_bottom( worksheet, 0 )
        ArbinWorkbook.resize_cells( worksheet, slice(0,2) )
        
    
        
    # --------------------------------------------------------------------------------------
    # Utilities
    # --------------------------------------------------------------------------------------
    @staticmethod
    def convert_date_time( df, column_name, unit, multiplier ):
        df[column_name] = df[column_name].apply( lambda x: x * multiplier )
        df[column_name] = pd.to_datetime(df[column_name], unit=unit, errors = 'coerce' )
        # Offset for timezone
        df[column_name] = df[column_name] + pd.DateOffset(hours=TIMEZONE)
        # Change to string to keep precision when going to Excel
        df[column_name] = pd.DatetimeIndex(df[column_name]).strftime('%Y-%m-%d %H:%M:%S.%f')
        
        return df
        