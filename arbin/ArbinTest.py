import sys
import pandas as pd
from arbin.ArbinDatabase import ArbinDatabase

# =============================================================================
# ArbinTestItem
# -----------------------------------------------------------------------------
# 
# Notes:
#   DateTime columns are processed to change timezone from UTC.  Because
#       DateTime columns are not the DataFrame index, they are also converted
#       to strings (Pandas limitation).
#
# =============================================================================

# Constants
# -----------------------------------------------------------------------------
TIMEZONE = -5 # Hours


class ArbinTestItem( object ):
    
    # Class Initialization
    # -----------------------------------------------------------------------------
    def __init__( self, testID, arbin_database ):
        self.testID = testID
        self.arbin_database = arbin_database
        
        self.test_name = self.test_name()
        self.arbin_number = self.arbin_number()
        self.has_auxiliary = self.has_auxiliary()

        self.global_info_df = self.get_global_info()
        self.raw_data_df = self.get_raw_data()
        self.cycle_statistics_df = self.get_cycle_statistics()

        # Change Timezone
        self.global_info_df = self.convert_date_time( self.global_info_df, 'Start DateTime', 's', 1 )
        self.raw_data_df = self.convert_date_time( self.raw_data_df, 'Date_Time', 'ns', 100 )
        self.cycle_statistics_df = self.convert_date_time( self.cycle_statistics_df, 'Date_Time', 'ns', 100 )

   
    # Properties
    # -----------------------------------------------------------------------------
    def test_name( self ):
        test_list_df = self.arbin_database.test_list_for( self.testID )
        return test_list_df.at[0,'Test_Name']
        
    
    def arbin_number( self ):
        device_id = self.device_id()
        return self.arbin_database.arbin_number_for( device_id )
    
    
    def device_id( self ):
        test_list_df = self.arbin_database.test_list_for( self.testID )
        return test_list_df.at[0,'Device_ID']
    
    
    def has_auxiliary( self ):
        test_channel_list_df = self.arbin_database.test_channel_list( self.testID )
        
        if( test_channel_list_df.at[0, 'Log_Aux_Data_Flag'] == 1):
            return True
        return False
    

    # Dataframes
    # -----------------------------------------------------------------------------
    def get_global_info( self ):
        test_channel_list_df = self.arbin_database.test_channel_list( self.testID )
        test_list_df = self.arbin_database.test_list_for( self.testID )
        
        rows_list = []
        for index, row in test_channel_list_df.iterrows():
            dict_values = {            'Channel': row['IV_Ch_ID'],
                                'Start DateTime': row['First_Start_DateTime'],
                            'Schedule File Name': row['Schedule_File_Name'],
                                       'Creator': test_list_df.at[0,'Creator'],
                                      'Comments': test_list_df.at[0,'Comment'],
                              'Software Version': test_list_df.at[0,'Software_Version'], # ??? DO I WANT TO GET THIS THIS WAY???
                              'Schedule Version': row['Schedule_Version'],
                                      'MASS (g)': row['SpecificMASS'],
                      'Specific Capacity (Ah/g)': row['SpecificCapacity'],
                                 'Capacity (Ah)': row['Capacity'],
                                       'Item ID': row['Item_ID'],
                                       'Has Aux': row['Has_Aux'],
                                   'Has Special': row['Has_Special'],
                             'Log Aux Data Flag': bool(row['Log_Aux_Data_Flag']),
                              'Log Special Flag': bool(row['Log_Special_Data_Flag']) }
            
            rows_list.append( dict_values )
            
        return pd.DataFrame( rows_list )   
        
        
    def get_raw_data( self ):
        data_basic_df = self.arbin_database.data_basic( self.testID )
        data_extended_df = self.arbin_database.data_extended( self.testID )
        merged_df = pd.merge( data_basic_df, data_extended_df, on='Date_Time', how='outer' )
        
        data_auxiliary_df = self.arbin_database.data_auxiliary( self.testID )
        merged_df = pd.concat( [merged_df, data_auxiliary_df], axis=1 )
        
        rows_list = []
        for index, row in merged_df.iterrows():
            dict_values = {         'Data_Point': row['Data_Point'],
                                     'Date_Time': row['Date_Time'],
                                 'Test_Times (s)': row['Test_Time'],
                                 'Step_Times (s)': row['Step_Time'],
                                   'Cycle_Index': row['Cycle_ID'],
                                    'Step_Index': row['Step_ID'],
                                    'Current (A)': row['Current'],
                                    'Voltage (V)': row['Voltage'],
                                      'Power (W)': '', # Calculated Value
                           'Charge_Capacity (Ah)': row['Charge_Capacity'],
                        'Discharge_Capacity (Ah)': row['Discharge_Capacity'],
                             'Charge_Energy (Wh)': row['Charge_Energy'],
                          'Discharge_Energy (Wh)': row['Discharge_Energy'],
                                      'ACR (Ohm)': row['ACR'],
                                    'dV/dt (V/s)': row['dV/dt'],
                      'Internal_Resistance (Ohm)': row['Internal_Resistance'],
                                   'dQ/dV (Ah/V)': row['dQ/dV'],
                                   'dV/dQ (V/Ah)': row['dV/dQ'] }
            
            for column in list( data_auxiliary_df.columns ):
                dict_values[column] = row[column]
            
            rows_list.append( dict_values )
            
        return pd.DataFrame( rows_list )


    def count_raw_data( self ):
        return self.raw_data_df['Data_Point'].count()


    def get_cycle_statistics( self ):
        statistic_data_df = self.arbin_database.data_statistic( self.testID )
        
        rows_list = []
        cycle_id = 0
        for index, row in statistic_data_df.iterrows():
        
            # Only keep one row for each Cycle_ID
            if row['Cycle_ID'] == cycle_id:
                rows_list.pop()
            
            cycle_id = row['Cycle_ID']
        
            coulombic_efficiency = 0
            if row['Charge_Capacity'] != 0:
                coulombic_efficiency = (row['Discharge_Capacity'] / row['Charge_Capacity']) * 100
        
            dict_values = {          'Date_Time': row['Date_Time'],
                                 'Test_Times (s)': row['Test_Time'],
                                 'Step_Times (s)': row['Step_Time'],
                                   'Cycle_Index': row['Cycle_ID'],
                                    'Step_Index': row['Step_ID'],
                                    'Current (A)': row['Current'],
                                    'Voltage (V)': row['Voltage'],
                                      'Power (W)': '', # Calculated Value
                           'Charge_Capacity (Ah)': row['Charge_Capacity'],
                        'Discharge_Capacity (Ah)': row['Discharge_Capacity'],
                             'Charge_Energy (Wh)': row['Charge_Energy'],
                          'Discharge_Energy (Wh)': row['Discharge_Energy'],
                                'Charge_Time (s)': row['Charge_Time'],
                             'Discharge_Time (s)': row['Discharge_Time'],
                            'V_Max_On_Cycle (V)': row['V_Max_On_Cycle'],
                       'Coulombic Efficiency (%)': coulombic_efficiency }

            rows_list.append( dict_values )

        return pd.DataFrame( rows_list )
        


    # --------------------------------------------------------------------------------------
    # Utilities
    # --------------------------------------------------------------------------------------
    @staticmethod
    def convert_date_time( df, column_name, unit, multiplier ):
        df[column_name] = df[column_name].apply( lambda x: x * multiplier )
        df[column_name] = pd.to_datetime(df[column_name], unit=unit, errors = 'coerce' )
        # Offset for timezone
        df[column_name] = df[column_name] + pd.DateOffset(hours=TIMEZONE)
        
        #df[column_name] = pd.DatetimeIndex(df[column_name]).strftime('%Y-%m-%d %H:%M:%S.%f')
        
        return df
    
    
