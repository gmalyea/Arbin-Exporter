import os
import re
import pyodbc
import pandas as pd

# =============================================================================
# ArbinDatabase
# -----------------------------------------------------------------------------
# On initialization the following SQL Server connections are made:
#   self.conn.master        (ArbinPro8MasterInfo)
#   self.conn.data[]        (ArbinPro8Data_1, 2, etc)
#
#
# ASSUMPTIONS
# -----------------------------------------------------------------------------
# - A test will write to multiple databases in order.
# - All info is in the Master Info database.
#
# =============================================================================


# Constants: SQL Server Arbin Database
# -----------------------------------------------------------------------------
DATABASE_MASTER = 'ArbinPro8MasterInfo'


class ArbinDatabase( object ):

    # Class Initialization
    # -----------------------------------------------------------------------------
    def __init__( self, server, username, password ):
        # SQL Server Connections
        try:
            self.conn_master = pyodbc.connect( 'DRIVER={ODBC Driver 17 for SQL Server}; SERVER='+server+'; DATABASE='+DATABASE_MASTER+'; UID='+username+'; PWD='+password )
        except Exception as e:
            print( "Could not connect to SQL Server, check the connection setting and try again." )
        
        self.conn_data = {}
        databases = self.list_data_databases()
        for db_name in databases:
            try:
                self.conn_data[db_name] = pyodbc.connect( 'DRIVER={ODBC Driver 17 for SQL Server}; SERVER='+server+'; DATABASE='+db_name+'; UID='+username+'; PWD='+password )
            except Exception as e:
                print( "Could not connect to SQL Server, check the connection setting and try again." )
            
            
    # Single Values
    # -----------------------------------------------------------------------------        
    def last_datetime_for( self, testID ):
        db_df = self.list_data_databases_for( testID )
        db = db_df[-1]
        
        query = ( "SELECT TOP 1 [Date_Time] FROM IV_Basic_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY [Date_Time] DESC" )
        df = pd.read_sql( query, self.conn_data[db] )
        time_raw = df.iloc[0,0]
        return int( time_raw / 10000000 )
        
        
    def arbin_number_for( self, deviceID ):        
        query = ( "SELECT [Arbin_Number] FROM Device_Table WHERE [Device_ID] = " + str(deviceID) )
        df = pd.read_sql( query, self.conn_master )
        return df.at[0,'Arbin_Number']
    
    
    # Python Lists
    # -----------------------------------------------------------------------------
    def list_data_databases( self ):
        query = ( "SELECT [Database_Name] FROM DatabaseName_Table" )
        df = pd.read_sql( query, self.conn_master )
        return df['Database_Name'].tolist()

    
    def list_data_databases_for( self, testID ):
        query = ( "SELECT [Databases] FROM TestIVChList_Table WHERE [Test_ID] = " + str(testID) )
        df = pd.read_sql( query, self.conn_master )
        return re.findall( '(\w+),', df.at[0, "Databases"] )


    def list_tests( self ):
        query = ( "SELECT [Test_ID] FROM TestIVChList_Table" )
        df = pd.read_sql( query, self.conn_master )
        return df['Test_ID'].tolist()


    # Pandas DataFrames
    # -----------------------------------------------------------------------------
    def test_list_display( self ):
        query = ( "SELECT TestList_Table.Test_ID, TestList_Table.Test_Name, TestList_Table.First_Start_DateTime, "
                    "TestIVChList_Table.IV_Ch_ID, TestIVChList_Table.Schedule_File_Name "
                    "FROM TestList_Table INNER JOIN TestIVChList_Table ON TestList_Table.Test_ID = TestIVChList_Table.Test_ID" )
        return pd.read_sql( query, self.conn_master )

    
    def test_list_for( self, testID ):
        query = ( "SELECT * FROM TestList_Table WHERE [Test_ID] = " + str(testID) )
        return pd.read_sql( query, self.conn_master )


    def test_channel_list( self, testID ):
        query = ( "SELECT * FROM TestIVChList_Table WHERE [Test_ID] = " + str(testID) )
        return pd.read_sql( query, self.conn_master )
        

    def data_basic( self, testID ):
        #query = ( "SELECT * FROM IV_Basic_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY [Date_Time]" )
        query = ( "SELECT [Data_Point], [Date_Time], [Test_Time], [Step_Time], [Cycle_ID], [Step_ID], [Current], [Voltage], [Charge_Capacity], [Discharge_Capacity], [Charge_Energy], [Discharge_Energy] FROM IV_Basic_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY [Date_Time]" )
        df_combined = pd.DataFrame()
        databases = self.list_data_databases_for( testID )
        for db in databases:
            df = pd.read_sql( query, self.conn_data[db] )
            df_combined = df_combined.append(df, ignore_index=True)
        
        return df_combined
        

        
        
        
    
    
    def data_extended( self, testID ):
        query = ( "SELECT [Date_Time], [6] as [ACR], [27] as [dV/dt],[30] as [Internal_Resistance], [82] as [dQ/dV], [83] as [dV/dQ] "
                    "FROM (SELECT * FROM IV_Extended_Table WHERE [Test_ID] = " + str(testID) + ") as tbl "
                    "PIVOT (SUM([Data_value]) FOR [Data_Type] IN ([6],[27],[30],[82],[83])) as pvt ORDER BY [Date_Time]" )
        df_combined = pd.DataFrame()
        databases = self.list_data_databases_for( testID )
        for db in databases:
            df = pd.read_sql( query, self.conn_data[db] )
            df_combined = df_combined.append(df, ignore_index=True)
        
        return df_combined
      
      
    def data_statistic( self, testID ):
        query = ( "SELECT * FROM StatisticData_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY [Date_Time]" )
        df_combined = pd.DataFrame()
        databases = self.list_data_databases_for( testID )
        for db in databases:
            df = pd.read_sql( query, self.conn_data[db] )
            df_combined = df_combined.append(df, ignore_index=True)
        
        return df_combined
        
        
#    def data_auxiliary( self, testID ):
#        # Get the aux map
#        # The aux map is a set of type & channel pairs, that specify the type of aux data
#        #    collected on which channel number.
#        query = ( "SELECT [Aux_Map] FROM TestIVChList_Table WHERE [Test_ID] = " + str(testID) )
#        df = pd.read_sql( query, self.conn_master )
#        auxiliary_map = df.at[0, "Aux_Map"]
#        auxiliary_type_channel_pairs = re.findall( '([0-9]+)\^([0-9]+)', auxiliary_map )
#        
#        # Query each data database
#        df_combined = pd.DataFrame()
#        databases = self.list_data_databases_for( testID )
#        for db in databases:
#            
#            # Start Time
#            query = ( "SELECT TOP 1 [Date_Time] FROM IV_Basic_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY [Date_Time]" )
#            df = pd.read_sql( query, self.conn_data[db] )
#            start_date_time = df.at[0, "Date_Time"]
#            
#            # End Time
#            query = ( "SELECT TOP 1 [Date_Time] FROM IV_Basic_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY [Date_Time] DESC" )
#            df = pd.read_sql( query, self.conn_data[db] )
#            end_date_time = df.at[0, "Date_Time"]
#        
#            # Query for the aux data
#            df = pd.DataFrame()
#            for pair in auxiliary_type_channel_pairs:
#                aux_type = pair[0]
#                aux_channel = pair[1]
#                
#                # Expand the aux type because codes 0 & 1 each map to two other data types
#                # The aux name is for human readable output
#                aux_type_expanded = self.get_aux_data_type( aux_type )
#                aux_name = self.get_aux_column_name( aux_type_expanded, aux_channel )
#                
#                # Substrings for 2-part auxiliary types (to keep query more human readable)
#                query_sub_1 = ""
#                query_sub_2 = ""
#                if len(aux_type_expanded) == 2:
#                    query_sub_1 = ", [" + str(aux_type_expanded[1]) + "] as [" + str(aux_name[1]) + "] "
#                    query_sub_2 = ", [" + str(aux_type_expanded[1]) + "] "
#                
#                query = ( "SELECT [Date_Time] as [Date_Time_Aux], [" + str(aux_type_expanded[0]) + "] as [" + str(aux_name[0]) + "] " + query_sub_1 +
#                            "FROM (SELECT * FROM Auxiliary_Table WHERE [AuxCh_Type] = " + str(aux_type_expanded[0]) + " AND [AuxCh_ID] = " + str(aux_channel) + " AND [Date_Time] > " + str(start_date_time) + " AND [Date_Time] < " + str(end_date_time) + ") AS tbl "
#                            "PIVOT (SUM(Data_Value) FOR Data_Type IN ([" + str(aux_type_expanded[0]) + "]" + query_sub_2 + ")) AS pvt ORDER BY [Date_Time_Aux]" )
#                
#                aux_df = pd.read_sql( query, self.conn_data[db] )
#                
#                # Merge each type of aux data into one dataframe
#                df = pd.concat( [df, aux_df], axis=1 )
#                
#                # Remove duplicate 'Date_Time_Aux' columns
#                # df = df.loc[:,~df.columns.duplicated()]
#                #df = df.drop( columns=['Date_Time_Aux'] )
#        
#            # Merge across data databases
#            df_combined = df_combined.append(df, ignore_index=True)
#            
#        return df_combined
        
        
        
    def data_auxiliary( self, testID ):
        # Get the aux map
        # The aux map is a set of type & channel pairs, that specify the type of aux data
        #    collected on which channel number.
        query = ( "SELECT [Aux_Map] FROM TestIVChList_Table WHERE [Test_ID] = " + str(testID) )
        df = pd.read_sql( query, self.conn_master )
        auxiliary_map = df.at[0, "Aux_Map"]
        auxiliary_type_channel_pairs = re.findall( '([0-9]+)\^([0-9]+)', auxiliary_map )
        
        databases = self.list_data_databases_for( testID )
        
        # Query each data pair
        df_combined = pd.DataFrame()
        list_aux_df = []
        
        for pair in auxiliary_type_channel_pairs:
            aux_type = pair[0]
            aux_channel = pair[1]
    
            print("-------------------------------------")
            print( aux_type + " | " + aux_channel )
    
            # Query for the aux data
            df_aux = pd.DataFrame()
        
            for db in databases:
                
                # Start Time
                query = ( "SELECT TOP 1 [Date_Time] FROM IV_Basic_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY [Date_Time]" )
                df = pd.read_sql( query, self.conn_data[db] )
                start_date_time = df.at[0, "Date_Time"]
                
                # End Time
                query = ( "SELECT TOP 1 [Date_Time] FROM IV_Basic_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY [Date_Time] DESC" )
                df = pd.read_sql( query, self.conn_data[db] )
                end_date_time = df.at[0, "Date_Time"]
                
                print( str(start_date_time) + " | " + str(end_date_time) )
                
                
                # Expand the aux type because codes 0 & 1 each map to two other data types
                # The aux name is for human readable output
                aux_type_expanded = self.get_aux_data_type( aux_type )
                aux_name = self.get_aux_column_name( aux_type_expanded, aux_channel )
                
                # Substrings for 2-part auxiliary types (to keep query more human readable)
                query_sub_1 = ""
                query_sub_2 = ""
                if len(aux_type_expanded) == 2:
                    query_sub_1 = ", [" + str(aux_type_expanded[1]) + "] as [" + str(aux_name[1]) + "] "
                    query_sub_2 = ", [" + str(aux_type_expanded[1]) + "] "
                
                query = ( "SELECT [Date_Time] as [Date_Time_Aux], [" + str(aux_type_expanded[0]) + "] as [" + str(aux_name[0]) + "] " + query_sub_1 +
                            "FROM (SELECT * FROM Auxiliary_Table WHERE [AuxCh_Type] = " + str(aux_type_expanded[0]) + " AND [AuxCh_ID] = " + str(aux_channel) + " AND [Date_Time] > " + str(start_date_time) + " AND [Date_Time] < " + str(end_date_time) + ") AS tbl "
                            "PIVOT (SUM(Data_Value) FOR Data_Type IN ([" + str(aux_type_expanded[0]) + "]" + query_sub_2 + ")) AS pvt ORDER BY [Date_Time_Aux]" )
                
                print(query)
                
                
                df = pd.read_sql( query, self.conn_data[db] )
                
                print(df)
                
                # Merge each type of aux data into one dataframe
                #df = pd.concat( [df, aux_df], axis=1 )
                df_aux = df_aux.append( df, ignore_index=True )
                
                # Remove duplicate 'Date_Time_Aux' columns
                # df = df.loc[:,~df.columns.duplicated()]
                #df = df.drop( columns=['Date_Time_Aux'] )
        
            # Merge across data databases
            #df_combined = df_combined.append( df, ignore_index=True )
            
            list_aux_df.append( df_aux )
            
        # Find the biggest Aux Table
        df_sorter = pd.DataFrame( columns=('index', 'count') )
        for index, aux_df in enumerate(list_aux_df):
            row_count =  aux_df['Date_Time_Aux'].count()
            
            new_row = { 'index':index, 'count':row_count }
            df_sorter = df_sorter.append( new_row, ignore_index=True )
            
        # Find which tables need fixing
        df_sorter.sort_values( 'count' )
        reference_aux = df_sorter.loc[0, 'index' ]
        largest_count = df_sorter.loc[0, 'count' ]
        df_need_fixing = df_sorter[df_sorter['count'] < largest_count]
        
        list_need_fixing = []
        for i, row in df_need_fixing.iterrows():
            list_need_fixing.append( row['index'] )
        
        
        # Fix the tables
        
        #num_aux = self.aux_data_df['Date_Time_Aux'].count()
        num_reference = largest_count

        #for need_fixing in list_need_fixing:
        for i, row in df_need_fixing.iterrows():  
            offset = 0
            df_to_fix = row['index']
            num_fix = row['count']
            for index, row in list_aux_df[reference_aux].iterrows():
                #if( index < num_aux ):
                fix_time = list_aux_df[df_to_fix].loc[index-offset, 'Date_Time_Aux']
                reference_time = row['Date_Time_Aux']
            
                if( fix_time > reference_time ):
                    list_aux_df[df_to_fix].loc[index-offset+0.5] = list_aux_df[df_to_fix].iloc[index-offset]
                    offset += 1
                    
                if( offset == (num_reference-num_fix) ):
                    break

            list_aux_df[df_to_fix] = list_aux_df[df_to_fix].sort_index()
            list_aux_df[df_to_fix] = list_aux_df[df_to_fix].reset_index(drop=True) 
            list_aux_df[df_to_fix] = list_aux_df[df_to_fix].drop( columns=['Date_Time_Aux'] )
        
        
        df_combined = pd.DataFrame()
        for table in list_aux_df:
        
            df_combined = pd.concat( [df_combined, table], axis=1 )
        
        
        
        
        print("======================================")
        print(df_combined)
        
        
        
        return df_combined
        
        
        
        
        
   
        
    # --------------------------------------------------------------------------------------
    # Utilities
    # --------------------------------------------------------------------------------------
    def get_aux_data_type( self, data_code ):
        if data_code == "0":   # Voltage and dV/dt
            return [0,30]
        elif data_code == "1": # Temperature and dT/dt
            return [1,31]
        
        val = int( data_code ) # Other
        return [val]


    def get_aux_column_name( self, dtype, channel ):
        nameList = []
        for type in dtype:
            if type == 0:
                nameList.append( "Aux_Voltage_" + str(channel) + " (V)" )
            if type == 1: 
                nameList.append( "Aux_Temperature_" + str(channel) + " (C)" )
            if type == 2: 
                nameList.append( "Aux_Pressure_" + str(channel) + " (psi)" )
            if type == 3: 
                nameList.append( "Aux_pH_" + str(channel) + " (V)" )
            if type == 4: 
                nameList.append( "Aux_Flow_Rate_" + str(channel) + " (C)" )
            if type == 5: 
                nameList.append( "Aux_Density_" + str(channel) + " (psi)" )
            if type == 6: 
                nameList.append( "Aux_Digital_Input_" + str(channel) + " (V/s)" )
            if type == 7: 
                nameList.append( "Aux_Digital_Output_" + str(channel) + " (C/s)" )
            if type == 8: 
                nameList.append( "Aux_EC_" + str(channel) + " (C)" )
            if type == 9: 
                nameList.append( "Aux_Safety_" + str(channel) + " (psi)" )
            if type == 10: 
                nameList.append( "Aux_Humidity_" + str(channel) + " (V/s)" )
            if type == 11: 
                nameList.append( "Aux_Analog_Output_" + str(channel) + " (C/s)" )
            if type == 30: 
                nameList.append( "Aux_dV/dt_" + str(channel) + " (V/s)" )
            if type == 31: 
                nameList.append( "Aux_dT/dt_" + str(channel) + " (C/s)" )

        return nameList
