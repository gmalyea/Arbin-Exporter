import sys
import pandas as pd
from arbin.ArbinDatabase import ArbinDatabase

# =============================================================================
# ArbinTest
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
        
        
        # Change Timezone
        #self.global_info_df = self.convert_date_time( self.global_info_df, 'Start DateTime', 's', 1 )
        #self.raw_data_df = self.convert_date_time( self.raw_data_df, 'Date_Time', 'ns', 100 )
        #self.cycle_statistics_df = self.convert_date_time( self.cycle_statistics_df, 'Date_Time', 'ns', 100 )
        #self.raw_aux_data_df = self.convert_date_time( self.raw_aux_data_df, 'Date_Time', 'ns', 100 )
        
   
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
            if( 1 == 0 ):
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
                
                    time_delta_df = pd.DataFrame([[0,time_delta],[1,time_delta_previous],[-1,time_delta_next]])
                    time_delta_df = time_delta_df.sort_values(by=[1])
                    smallest_time_delta = time_delta_df.iloc[0,0]
                
                
                # TEMP
                #if( index == 1000 ):
                #    break
           #     offset += smallest_time_delta
           #     df = df.append([aux_df.iloc[index-offset]], ignore_index=True)
                
           #     offset += 1
                
                
                #if( aux_time > basic_extended_time ):
                #    aux_df.loc[index-offset+shift] = aux_df.iloc[index-offset]
                #    offset += 1
                #    shift += base_shift
                    
            #aux_df = aux_df.sort_index()
            #aux_df = aux_df.reset_index(drop=True) 
            #aux_df = aux_df.drop( columns=['Date_Time_Aux'] )
            
      #      pd.options.display.float_format = '{:.1f}'.format
      #      print(merged_df[:28])
      #      print(df[:28])
            
       #     diff = merged_df["Date_Time"] - df["Date_Time_Aux"]
       #     print(diff[:28])
            #print(merged_df[8115:8125])
            #print(aux_df[8110:8120])
                
              
            merged_df = pd.concat( [merged_df, aux_df], axis=1 )
            
        
        #for aux_df in list_auxiliary_df:
        #    print(aux_df.info(verbose=True))
        #    print(aux_df)
        
        
        #print(merged_df)
       # # Lineup basic and auxiliary date_times
       # num_basic_extended = basic_extended_df['Date_Time'].count()
       # num_aux = data_auxiliary_df['Date_Time_Aux'].count()
       # 
       # if( num_basic_extended > num_aux ):
       #     offset = 0
       #     for index, row in basic_extended_df.iterrows():
       #         #print(str(index) + " | " + str(offset))
       #         aux_time = data_auxiliary_df.loc[index-offset, 'Date_Time_Aux']
       #         basic_extended_time = row['Date_Time']
       #         
       #         if( aux_time > basic_extended_time ):
       #             #if( (index-offset+0.5) in data_auxiliary_df.index ):
       #             #    data_auxiliary_df.loc[index-offset+0.6] = data_auxiliary_df.iloc[index-offset]
       #             #else:
       #             #    data_auxiliary_df.loc[index-offset+0.5] = data_auxiliary_df.iloc[index-offset]
       #             print(str(index) + " " + str(data_auxiliary_df.iloc[index-offset]))
       #             data_auxiliary_df.loc[index-offset+(0.0001*(offset+1))] = data_auxiliary_df.iloc[index-offset]
       #             offset += 1
       #             #print( offset )
       #     
       #         #if( offset == (num_basic_extended-num_aux) ):
       #         #    print( "final: " + str(offset) )
       #         #    break
       #     
       #     #num_aux_new = data_auxiliary_df['Date_Time_Aux'].count()
       #     #for _ in range(num_basic_extended-num_aux_new):
       #     #    data_auxiliary_df.loc[index-offset+0.5] = data_auxiliary_df.iloc[index-offset]
       #     #    offset += 1
       #  
       #     data_auxiliary_df = data_auxiliary_df.sort_index()
       #     data_auxiliary_df = data_auxiliary_df.reset_index(drop=True) 
       #     #data_auxiliary_df = data_auxiliary_df.drop( columns=['Date_Time_Aux'] )
        
        #print(basic_extended_df[:25])
        #print(data_auxiliary_df[:25])
        #print(basic_extended_df[8115:8125])
        #print(data_auxiliary_df[8110:8120])
        # Need to add an extra Aux line for every Cycle_Index
        # Also, add one at the beginning and end
        
#        data_auxiliary_df.loc[0.5] = data_auxiliary_df.iloc[0]
#        
#        
#        cycle_index = 1
#        offset = 1
#        for index, row in basic_extended_df.iterrows():
#            if( cycle_index != row['Cycle_Index'] ):
#                cycle_index = row['Cycle_Index']
#                
#                
#                data_auxiliary_df.loc[index-offset+0.5] = data_auxiliary_df.iloc[index-offset]
#                offset += 1
#                    
#                    
#                    
#        
#                #aux_time = data_auxiliary_df.loc[index-offset, 'Date_Time_Aux']
#                #basic_extended_time = row['Date_Time']
#                
#                #if( aux_time > basic_extended_time ):
#                    #if( (index-offset+0.5) in data_auxiliary_df.index ):
#                    #    data_auxiliary_df.loc[index-offset+0.6] = data_auxiliary_df.iloc[index-offset]
#                    #else:
#                    #    data_auxiliary_df.loc[index-offset+0.5] = data_auxiliary_df.iloc[index-offset]
#                    #print(str(index) + " " + str(data_auxiliary_df.iloc[index-offset]))
#                    #data_auxiliary_df.loc[index-offset+(0.0001*(offset+1))] = data_auxiliary_df.iloc[index-offset]
#                    #offset += 1
#                    #print( offset )
#        
#        
#        # Add end
#        num_aux = data_auxiliary_df['Date_Time_Aux'].count()
#        data_auxiliary_df.loc[num_aux] = data_auxiliary_df.iloc[num_aux-1]
#        
#        data_auxiliary_df = data_auxiliary_df.sort_index()
#        data_auxiliary_df = data_auxiliary_df.reset_index(drop=True)
#        
        
        
        
        
        
#        print("===========================")
#        print(basic_extended_df['Date_Time'].count())
#        print(data_auxiliary_df['Date_Time_Aux'].count())
        
#        merged_df = pd.concat( [basic_extended_df, data_auxiliary_df], axis=1 )
        return merged_df
        
        
        
   
    def merge_raw_data( self ):
        
        offset = 0
        num_aux = self.aux_data_df['Date_Time_Aux'].count()
        num_raw = self.basic_extended_data_df['Date_Time'].count()

        for index, row in self.basic_extended_data_df.iterrows():
            if( index < num_aux ):
                aux_time = self.aux_data_df.loc[index-offset, 'Date_Time_Aux']
                be_time = row['Date_Time']
                
                if( aux_time > be_time ):
                    self.aux_data_df.loc[index-offset+0.5] = self.aux_data_df.iloc[index-offset]
                    offset += 1


        self.aux_data_df = self.aux_data_df.sort_index()
        self.aux_data_df = self.aux_data_df.reset_index(drop=True) 
        #merged_df = merged_df.drop( columns=['Date_Time_Aux'] )
        
        
        merged_df = pd.concat( [self.basic_extended_data_df, self.aux_data_df], axis=1 )
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
        # Change to string to keep precision when going to Excel
        df[column_name] = pd.DatetimeIndex(df[column_name]).strftime('%Y-%m-%d %H:%M:%S.%f')
        
        return df
    
    
