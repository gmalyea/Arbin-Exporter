import os
import math
import pyodbc
import pandas as pd
import openpyxl
import openpyxl.utils.dataframe
import openpyxl.styles.colors
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
MAXDATAPOINTS = 900000


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
        
        for row in openpyxl.utils.dataframe.dataframe_to_rows( df, index=False, header=True ):
            worksheet.append( row )
             
        # Green header background        
        ArbinWorkbook.background_color( worksheet, 3, 'CEFFCE' )
        
        # Resize cells to fit  
        ArbinWorkbook.resize_cells( worksheet, slice(3,5) )
             

    def export_channel_sheet( self, worksheet, wb_num ):
        datapoint_start = wb_num * MAXDATAPOINTS
        datapoint_end = datapoint_start + MAXDATAPOINTS
    
        df = self.arbinTestItem.raw_data_df.iloc[datapoint_start:datapoint_end]
        
        for row in openpyxl.utils.dataframe.dataframe_to_rows( df, index=False, header=True ):
            worksheet.append( row )
    
        # Blue header background
        ArbinWorkbook.background_color( worksheet, 0, 'CEFFFF' )
    
        # Resize cells to fit
        ArbinWorkbook.resize_cells( worksheet, slice(0,2) )
    
    
    def export_statistics_sheet( self, worksheet ):
        df = self.arbinTestItem.cycle_statistics_df
        
        for row in openpyxl.utils.dataframe.dataframe_to_rows( df, index=False, header=True ):
            worksheet.append( row )
    
        # Blue header background
        ArbinWorkbook.background_color( worksheet, 0, 'CEFFFF' )
    
        # Resize cells to fit
        ArbinWorkbook.resize_cells( worksheet, slice(0,2) )
