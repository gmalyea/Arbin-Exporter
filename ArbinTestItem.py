import sys
import pandas as pd
from ArbinDatabase import ArbinDatabase



class ArbinTestItem(object):
    
    def __init__( self, testID, arbinDatabase ):
        self.testID = testID
        self.arbin_database = arbinDatabase
        
        self.test_name = self.testItemName()
        self.has_auxiliary = self.has_auxiliary()

        self.dbGlobalInfo_df = self.globalInfo()
        self.dbRawData_df = self.rawData()
        self.dbCycleStatistics_df = self.cycleStatistics()

        ### Need to combine auxiliary like extended
        self.auxiliary_df = self.auxiliary_data()

        print( self.auxiliary_df )

        AuxChannels = []  #2D List of all Auxillary Channels, first index is the parent channel the aux is mapped to, second index is the aux channel
        AuxTypes = []


    def has_auxiliary( self ):
        testIVChannelList_df = self.arbin_database.testIVChannelList( self.testID )
        
        if testIVChannelList_df.at[0, 'Log_Aux_Data_Flag'] == 1:
            return True
        
        return False
    
    
    def auxiliary_data( self ):
        return self.arbin_database.data_auxiliary_table( self.testID )

            
    def testItemName( self ):
        testList_df = self.arbin_database.testListFor( self.testID )
        return testList_df.at[0,'Test_Name']
    

    def globalInfo( self ):
        testIVChannelList_df = self.arbin_database.testIVChannelList( self.testID )
        testList_df = self.arbin_database.testListFor( self.testID )
        
        rows_list = []
        for index, row in testIVChannelList_df.iterrows():
        
            dict_values = {            'Channel': row['IV_Ch_ID'],
                                'Start DateTime': row['First_Start_DateTime'],
                            'Schedule File Name': row['Schedule_File_Name'],
                                       'Creator': testList_df.at[0,'Creator'],
                                      'Comments': testList_df.at[0,'Comment'],
                              'Software Version': testList_df.at[0,'Software_Version'], # ??? DO I WANT TO GET THIS THIS WAY???
                              'Schedule Version': row['Schedule_Version'],
                                       'MASS(g)': row['SpecificMASS'],
                       'Specific Capacity(Ah/g)': row['SpecificCapacity'],
                                  'Capacity(Ah)': row['Capacity'],
                                       'Item ID': row['Item_ID'],
                                       'Has Aux': row['Has_Aux'],
                                   'Has Special': row['Has_Special'],
                             'Log Aux Data Flag': row['Log_Aux_Data_Flag'],
                              'Log Special Flag': row['Log_Special_Data_Flag'] }
            
            rows_list.append(dict_values)
            
        return pd.DataFrame(rows_list)   
        
        
    def rawData( self ):
        data_basic_df = self.arbin_database.dataIVBasic( self.testID )
        data_extended_df = self.arbin_database.dataIVExtended( self.testID )
        
        data_auxiliary_df = self.arbin_database.data_auxiliary_table( self.testID )
        
        
        
        mergedBasicExtended_df = pd.merge(data_basic_df, data_extended_df, on='Date_Time', how='outer')
        
        mergedAux_df = pd.merge( mergedBasicExtended_df, data_auxiliary_df, on='Date_Time', how='outer')
        
        
        rows_list = []
        for index, row in mergedBasicExtended_df.iterrows():
        
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
                                   'dV/dQ(V/Ah)': row['dV/dQ'],
                              'Aux_Voltage_3(V)': '',
                                'Aux_dV/dt_3(V)': '',
                          'Aux_Temperature_3(C)': '',
                                'Aux_dT/dt_3(C)': ''}
            
            
            aux_values = {}
            listofvalues = list(data_auxiliary_df.columns)
            for column in listofvalues:
                aux_values[column] = ""
            
            
            
            #rows_list.append( dict_values )
            rows_list.append( aux_values )


        return pd.DataFrame( rows_list )


    def cycleStatistics( self ):
        statisticData_df = self.arbin_database.statisticData( self.testID )
        
        rows_list = []
        for index, row in statisticData_df.iterrows():
        
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

            rows_list.append(dict_values)

        return pd.DataFrame(rows_list)
        







