import sys
import pandas as pd
from ArbinDatabase import ArbinDatabase

# =============================================================================
# ArbinTestItem
# -----------------------------------------------------------------------------
# 
# 
# 
# 
#
# =============================================================================

class ArbinTestItem( object ):
    
    def __init__( self, testID, arbin_database ):
        self.testID = testID
        self.arbin_database = arbin_database
        
        self.test_name = self.test_name()
        self.has_auxiliary = self.has_auxiliary()

        self.global_info_df = self.get_global_info()
        self.raw_data_df = self.get_raw_data()
        self.cycle_statistics_df = self.get_cycle_statistics()


    def has_auxiliary( self ):
        test_channel_list_df = self.arbin_database.test_channel_list( self.testID )
        
        if( test_channel_list_df.at[0, 'Log_Aux_Data_Flag'] == 1):
            return True
        return False

            
    def test_name( self ):
        test_list_df = self.arbin_database.test_list_for( self.testID )
        return test_list_df.at[0,'Test_Name']
    

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
                                       'MASS(g)': row['SpecificMASS'],
                       'Specific Capacity(Ah/g)': row['SpecificCapacity'],
                                  'Capacity(Ah)': row['Capacity'],
                                       'Item ID': row['Item_ID'],
                                       'Has Aux': row['Has_Aux'],
                                   'Has Special': row['Has_Special'],
                             'Log Aux Data Flag': row['Log_Aux_Data_Flag'],
                              'Log Special Flag': row['Log_Special_Data_Flag'] }
            
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
                                 'Test_Times(s)': row['Test_Time'],
                                 'Step_Times(s)': row['Step_Time'],
                                   'Cycle_Index': row['Cycle_ID'],
                                    'Step_Index': row['Step_ID'],
                                    'Current(A)': row['Current'],
                                    'Voltage(V)': row['Voltage'],
                                      'Power(W)': '',
                           'Charge_Capacity(Ah)': row['Charge_Capacity'],
                        'Discharge_Capacity(Ah)': row['Discharge_Capacity'],
                             'Charge_Energy(Wh)': row['Charge_Energy'],
                          'Discharge_Energy(Wh)': row['Discharge_Energy'],
                                      'ACR(Ohm)': row['ACR'],
                                    'dV/dt(V/s)': row['dV/dt'],
                      'Internal_Resistance(Ohm)': row['Internal_Resistance'],
                                   'dQ/dV(Ah/V)': row['dQ/dV'],
                                   'dV/dQ(V/Ah)': row['dV/dQ'] }
            
            for column in list( data_auxiliary_df.columns ):
                dict_values[column] = row[column]
            
            rows_list.append( dict_values )

        df = pd.DataFrame( rows_list )
        df.dropna(subset=['Data_Point'], inplace=True)
        return df
        #return pd.DataFrame( rows_list )


    def get_cycle_statistics( self ):
        statistic_data_df = self.arbin_database.data_statistic( self.testID )
        
        rows_list = []
        for index, row in statistic_data_df.iterrows():
        
            dict_values = {          'Date_Time': row['Date_Time'],
                                 'Test_Times(s)': row['Test_Time'],
                                 'Step_Times(s)': row['Step_Time'],
                                   'Cycle_Index': row['Cycle_ID'],
                                    'Step_Index': row['Step_ID'],
                                    'Current(A)': row['Current'],
                                    'Voltage(V)': row['Voltage'],
                           'Charge_Capacity(Ah)': row['Charge_Capacity'],
                        'Discharge_Capacity(Ah)': row['Discharge_Capacity'],
                             'Charge_Energy(Wh)': row['Charge_Energy'],
                          'Discharge_Energy(Wh)': row['Discharge_Energy'],
                                'Charge_Time(s)': row['Charge_Time'],
                             'Discharge_Time(s)': row['Discharge_Time'],
                            'V_Max_On_Cycle (V)': row['V_Max_On_Cycle'],
                       'Coulombic Efficiency %%': ''}#row['Discharge_Capacity'] * 100 /NULLIF(row['Charge_Capacity'],0) }

            rows_list.append( dict_values )

        return pd.DataFrame( rows_list )
        

