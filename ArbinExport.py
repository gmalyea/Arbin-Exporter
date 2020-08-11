import pyodbc
import pandas as pd
import openpyxl
import openpyxl.utils.dataframe
import openpyxl.styles.colors
import datetime
import dateutil
import os
from ArbinTestItem import ArbinTestItem

# =============================================================================
# ArbinExport
# -----------------------------------------------------------------------------
# 
# 
# 
# 
#
# =============================================================================

class ArbinExport( object ):

    def __init__( self, arbinTestItem ):
        self.arbinTestItem = arbinTestItem
        
        current_date = str( datetime.datetime.now() )
        current_date = current_date.split(" ")[0]
        self.file_name = arbinTestItem.test_name + "_" + current_date + ".xlsx"
        
        self.wb = openpyxl.Workbook()
        self.ws1 = self.wb.active
        self.ws1.title = "Global Info"
        self.ws2 = self.wb.create_sheet("Channel")
        self.ws3 = self.wb.create_sheet("Statistics")


    def save_workbook( self, path ):
        self.export_global_info_sheet( self.ws1 )
        self.export_channel_sheet( self.ws2 )
        self.export_statistics_sheet( self.ws3 )
        
        if( not os.path.isdir(path) ): os.makedirs(path)
        self.wb.save( path + self.file_name )
    

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
             

    def export_channel_sheet( self, worksheet ):
        df = self.arbinTestItem.raw_data_df        
        df = self.convert_date_time( df, 'Date_Time', 'ns', 100 )

        for row in openpyxl.utils.dataframe.dataframe_to_rows( df, index=False, header=True ):
            worksheet.append( row )
    
        # Blue header background
        self.background_color( worksheet, 0, 'CEFFFF' )
    
        # Resize cells to fit
        self.resize_cells( worksheet, slice(0,2) )
    
    
    def export_statistics_sheet( self, worksheet ):
        df = self.arbinTestItem.cycle_statistics_df  
        df = self.convert_date_time( df, 'Date_Time', 'ns', 100 )
        
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
        return df
    
    
