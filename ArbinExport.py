import pyodbc
import pandas as pd
import openpyxl
import openpyxl.utils.dataframe
import openpyxl.styles.colors
import datetime
import dateutil
import os
import math
from ArbinTestItem import ArbinTestItem
from ArbinWorkbook import ArbinWorkbook

# =============================================================================
# ArbinExport
# -----------------------------------------------------------------------------
# 
# 
# 
# 
#
# =============================================================================

# Constants
# -----------------------------------------------------------------------------
MAXDATAPOINTS = 20
TIMEZONE = "America/New_York"


class ArbinExport( object ):

    def __init__( self, arbinTestItem ):
        self.arbinTestItem = arbinTestItem
        self.wb_list = []
        
        # Excel has a maximum of 1,000,000 rows
        wb_count = math.ceil( arbinTestItem.raw_data_count() / MAXDATAPOINTS )
        
        for wb_num in range(wb_count):
            file_name = arbinTestItem.test_name
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
        worksheet.append(['','','','TEST REPORT'])
        worksheet.append(['','','Test Name'])
        worksheet.append(['','','Export Time'])
        
        df = self.arbinTestItem.global_info_df        
        df = self.convert_date_time( df, 'Start DateTime', 's', 1 )
        
        for row in openpyxl.utils.dataframe.dataframe_to_rows( df, index=False, header=True ):
            worksheet.append( row )
             
        # Green header background        
        self.background_color( worksheet, 3, 'CEFFCE' )
        
        # Resize cells to fit  
        self.resize_cells( worksheet, slice(3,5) )
             

    def export_channel_sheet( self, worksheet, wb_num ):
        datapoint_start = wb_num * MAXDATAPOINTS
        datapoint_end = datapoint_start + MAXDATAPOINTS
    
        df = self.arbinTestItem.raw_data_df.iloc[datapoint_start:datapoint_end]       
        #df = self.convert_date_time( df, 'Date_Time', 'ns', 100 )
        
        for row in openpyxl.utils.dataframe.dataframe_to_rows( df, index=False, header=True ):
            worksheet.append( row )
    
        # Blue header background
        self.background_color( worksheet, 0, 'CEFFFF' )
    
        # Resize cells to fit
        self.resize_cells( worksheet, slice(0,2) )
    
    
    def export_statistics_sheet( self, worksheet ):
        df = self.arbinTestItem.cycle_statistics_df  
        #df = self.convert_date_time( df, 'Date_Time', 'ns', 100 )
        
        for row in openpyxl.utils.dataframe.dataframe_to_rows( df, index=False, header=True ):
            worksheet.append( row )
    
        # Blue header background
        self.background_color( worksheet, 0, 'CEFFFF' )
    
        # Resize cells to fit
        self.resize_cells( worksheet, slice(0,2) )    
    
    
    # --------------------------------------------------------------------------------------
    # Utilities
    # --------------------------------------------------------------------------------------
    def background_color( self, worksheet, row, color ):
        backgroundFill = openpyxl.styles.fills.PatternFill( start_color=color, end_color=color, fill_type='solid' )
        for cell in list(worksheet.rows)[row]:
            cell.fill = backgroundFill
    
    
    def resize_cells( self, worksheet, rows_to_check ):
        dims = {}
        for row in list(worksheet.rows)[rows_to_check]:
            for cell in row:
                if cell.value:
                    dims[cell.column_letter] = max( (dims.get(cell.column_letter, 0), len(str(cell.value))) ) 
        for col, value in dims.items():
            worksheet.column_dimensions[col].width = value
        
    def convert_date_time( self, df, column_name, unit, multiplier ):
        df[column_name] = df[column_name].apply( lambda x: x * multiplier )
        df[column_name] = pd.to_datetime(df[column_name], unit=unit, errors = 'coerce' )
        df[column_name] = pd.DatetimeIndex(df[column_name]).tz_localize('UTC').tz_convert(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S.%f')
        
        return df
    
    
