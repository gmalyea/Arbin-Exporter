import sys
import pandas as pd
from arbin.ArbinDatabase import ArbinDatabase

# =============================================================================
# ArbinTest
# -----------------------------------------------------------------------------
# Copyright (c) 2020 Garret Alyea. All Rights Reserved.
# 
# ArbinTest class encapsulates the data from one Arbin Test ID using
# Pandas dataframes.
#
# Notes:
#   DateTime columns are processed to change timezone from UTC.  Because
#       DateTime columns are not the DataFrame index, they are also converted
#       to strings (Pandas limitation).
#
# =============================================================================


class ArbinTest( object ):
    
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
                              'Software Version': test_list_df.at[0,'Software_Version'],
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
        basic_extended_df = pd.merge( data_basic_df, data_extended_df, on='Date_Time', how='outer' )
        
        rows_list = []
        for index, row in basic_extended_df.iterrows():
            dict_values = {       'Data_Point': row['Data_Point'],
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
            
            rows_list.append( dict_values )
        
        merged_df = pd.DataFrame( rows_list )
        

        # Aux Data
        list_auxiliary_df = self.arbin_database.data_auxiliary( self.testID )
        
        # Lineup each auxiliary table with the basic-extended table by date_times
        num_basic_extended = merged_df['Date_Time'].count()
        
        for aux_df in list_auxiliary_df: 
            num_aux = aux_df['Date_Time_Aux'].count()
            df = pd.DataFrame()
            offset = 0
        
            for index, row in merged_df.iterrows():
                basic_extended_time = row['Date_Time']
                aux_time = aux_df.loc[index-offset, 'Date_Time_Aux']
                if( index-offset < 1 ):
                    aux_time_previous = 0
                else:
                    aux_time_previous = aux_df.loc[index-offset-1, 'Date_Time_Aux']
            
                if( index-offset >= num_aux-1 ):
                    aux_time_next = 0
                else:
                    aux_time_next = aux_df.loc[index-offset+1, 'Date_Time_Aux']
            
                time_delta = abs(aux_time - basic_extended_time)
                time_delta_previous = abs(aux_time_previous - basic_extended_time)
                time_delta_next = abs(aux_time_next - basic_extended_time)
                
                time_shift = 0
                if( time_delta_previous < time_delta ):
                    if( time_delta_previous < time_delta_next ):
                        time_shift = 1
                    else:
                        time_shift = -1
                elif( time_delta_next < time_delta ):
                    time_shift = -1
                
                # Easy version - but is it slow?
                # ====================================================
                offset += time_shift
                df = df.append([aux_df.iloc[index-offset]], ignore_index=True)
                offset += 1
                
            df = df.drop( columns=['Date_Time_Aux'] )
            merged_df = pd.concat( [merged_df, df], axis=1 )

        return merged_df
        
    
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
                       'Coulombic Efficiency (%)': coulombic_efficiency,
                                          'mAh/g': '' } # Entered Value

            rows_list.append( dict_values )

        return pd.DataFrame( rows_list )
        