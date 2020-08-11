import pyodbc
import pandas as pd
import datetime
import dateutil
import os
import re

# =============================================================================
# ArbinDatabase
# -----------------------------------------------------------------------------
# 
#
#
#
#
# =============================================================================


# SQL Server Arbin Database Constants
# -----------------------------------------------------------------------------
DATABASE_MASTER = 'ArbinPro8MasterInfo'
DATABASE_INFO = 'ArbinPro8Info_1'
DATABASE_DATA = 'ArbinPro8Data_1'


class ArbinDatabase( object ):

    # Connections to SQL Server
    # -----------------------------------------------------------------------------
    def __init__( self, server, username, password ):
        try:
            self.conn_master = pyodbc.connect( 'DRIVER={ODBC Driver 17 for SQL Server}; SERVER='+server+'; DATABASE='+DATABASE_MASTER+'; UID='+username+'; PWD='+password )
            self.conn_info = pyodbc.connect( 'DRIVER={ODBC Driver 17 for SQL Server}; SERVER='+server+'; DATABASE='+DATABASE_INFO+'; UID='+username+'; PWD='+password )
            self.conn_data = pyodbc.connect( 'DRIVER={ODBC Driver 17 for SQL Server}; SERVER='+server+'; DATABASE='+DATABASE_DATA+'; UID='+username+'; PWD='+password )
        except Exception as e:
            print( "Could not connect to SQL Server, check the connection setting and try again." )


    
    def test_list( self ):
        query = ( "SELECT Test_ID from TestList_Table" )
        return pd.read_sql( query, self.conn_master )


    def test_list_for( self, testID ):
        query = ( "SELECT * from TestList_Table WHERE [Test_ID] = " + str(testID) )
        return pd.read_sql( query, self.conn_master )


    def test_channel_list( self, testID ):
        query = ( "SELECT * from TestIVChList_Table WHERE [Test_ID] = " + str(testID) )
        return pd.read_sql( query, self.conn_master )
        

    def data_basic( self, testID ):
        query = ( "SELECT * FROM IV_Basic_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY date_time" )
        return pd.read_sql( query, self.conn_data )

        
    def data_extended( self, testID ):
        query = ( "SELECT [Date_Time], [6] as [ACR], [27] as [dV/dt],[30] as [Internal_Resistance], [82] as [dQ/dV], [83] as [dV/dQ]"
                       "FROM (SELECT * FROM IV_Extended_Table WHERE [Test_ID] = " + str(testID) + ") as tbl "
                       "PIVOT (SUM([Data_value]) FOR [Data_Type] IN ([6],[27],[30],[82],[83])) as pvt ORDER BY date_time" )
        return pd.read_sql( query, self.conn_data )
      
      
    def data_statistic( self, testID ):
        query = ( "SELECT * from StatisticData_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY date_time" )
        return pd.read_sql( query, self.conn_data )
        
        
    def data_auxiliary( self, testID ):
        # Get the aux map
        query = ( "SELECT [Aux_Map] from TestIVChList_Table WHERE [Test_ID] = " + str(testID) )
        df = pd.read_sql( query, self.conn_master )
        auxiliary_map = df.at[0, "Aux_Map"]
        auxiliary_type_channel_pair = re.findall( '([0-9]+)\^([0-9]+);', auxiliary_map )
        
        # Get start and end times
        query = ( "SELECT [Date_Time] from IV_Basic_Table WHERE [Test_ID] = " + str(testID) )
        df = pd.read_sql( query, self.conn_data )
        start_date_time = df.at[0, "Date_Time"]
        end_date_time = df.at[df.index[-1], "Date_Time"]

        df = pd.DataFrame()
        
        for pair in auxiliary_type_channel_pair:
            aux_type = pair[0]
            aux_channel = pair[1]
            
            aux_type_expanded = self.get_aux_data_type( aux_type )
            aux_name = self.get_aux_column_name( aux_type_expanded, aux_channel )
            
            query = ( "SELECT [Date_Time] as [Date_Time_Aux], [" + str(aux_type_expanded[0]) + "] as [" + str(aux_name[0]) + "], [" + str(aux_type_expanded[1]) + "] as [" + str(aux_name[1]) + "] "
                              "FROM (SELECT * FROM Auxiliary_Table WHERE [AuxCh_Type] = " + str(aux_type_expanded[0]) + " AND [AuxCh_ID] = " + str(aux_channel) + " AND [Date_Time] > " + str(start_date_time) + " AND [Date_Time] < " + str(end_date_time) + ") AS tbl "
                              "PIVOT (SUM(Data_Value) FOR Data_Type IN ([" + str(aux_type_expanded[0]) + "], [" + str(aux_type_expanded[1]) + "])) AS pvt ORDER BY [Date_Time_Aux]" )
            
            aux_df = pd.read_sql( query, self.conn_data )
            
            df = pd.concat( [df, aux_df], axis=1 )
            df = df.drop( columns=['Date_Time_Aux'] )
        
        return df
        
        
    # --------------------------------------------------------------------------------------
    # Utilities
    # --------------------------------------------------------------------------------------
    
    def get_aux_data_type( self, data_code ):
        if data_code == "0": #Voltage and dV/dt
            return [0,30]
        elif data_code == "1": #Temperature and dT/dt
            return [1,31]
        
        val = int( data_code )
        return [val]


    def get_aux_column_name( self, dtype, channel ):
        nameList = []
        for type in dtype:
            if type == 0:
                nameList.append( "Aux_Voltage_" + str(channel) + "(V)" )
            if type == 1: 
                nameList.append( "Aux_Temperature_" + str(channel) + "(C)" )
            if type == 2: 
                nameList.append( "Aux_Pressure_" + str(channel) + "(psi)" )
            if type == 3: 
                nameList.append( "Aux_pH_" + str(channel) + "(V)" )
            if type == 4: 
                nameList.append( "Aux_Flow_Rate_" + str(channel) + "(C)" )
            if type == 5: 
                nameList.append( "Aux_Density_" + str(channel) + "(psi)" )
            if type == 6: 
                nameList.append( "Aux_Digital_Input_" + str(channel) + "(V/s)" )
            if type == 7: 
                nameList.append( "Aux_Digital_Output_" + str(channel) + "(C/s)" )
            if type == 8: 
                nameList.append( "Aux_EC_" + str(channel) + "(C)" )
            if type == 9: 
                nameList.append( "Aux_Safety_" + str(channel) + "(psi)" )
            if type == 10: 
                nameList.append( "Aux_Humidity_" + str(channel) + "(V/s)" )
            if type == 11: 
                nameList.append( "Aux_Analog_Output_" + str(channel) + "(C/s)" )
            if type == 30: 
                nameList.append( "Aux_dV/dt_" + str(channel) + "(V/s)" )
            if type == 31: 
                nameList.append( "Aux_dT/dt_" + str(channel) + "(C/s)" )

        return nameList
