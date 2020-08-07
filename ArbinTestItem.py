import sys
import pandas as pd
from ArbinDatabase import ArbinDatabase



class ArbinTestItem(object):
    
    def __init__( self, testID, arbinDatabase ):
        
        self.testID = testID
        self.arbinDatabase = arbinDatabase
        
        testList_df = arbinDatabase.testListFor( testID )
        self.testName = testList_df.at[0,'Test_Name']

        self.dbGlobalInfo_df = self.globalInfo()
        self.dbRawData_df = self.rawData()
        self.dbCycleStatistics_df = self.cycleStatistics()


#            AuxParentChanList = []
#            AuxTypeParentChanList = []
#
#            if row["Log_Aux_Data_Flag"] == 1:
#             IncludeAuxData = True;
#             AuxMapping = row["Aux_Map"]
#             AuxMapping = AuxMapping.split(";")
#             for AuxChanPair in AuxMapping:
#                 if(AuxChanPair == ""):
#                     continue
#                 AuxTypeParentChanList.append(AuxChanPair.split("^")[0])
#                 AuxParentChanList.append(AuxChanPair.split("^")[1])
#
#            AuxChannels.append(AuxParentChanList)
#            AuxTypes.append(AuxTypeParentChanList);



    def globalInfo( self ):

        testIVChannelList_df = self.arbinDatabase.testIVChannelList( self.testID )
        testList_df = self.arbinDatabase.testListFor( self.testID )
        
        rows_list = []
        for index, row in testIVChannelList_df.iterrows():
        
            dict_values = {            'Channel': row['IV_Ch_ID'],
                                'Start DateTime': row['First_Start_DateTime'],
                            'Schedule File Name': row['Schedule_File_Name'],
                                       'Creator': '',
                                      'Comments': '',
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

        dataIVBasic_df = self.arbinDatabase.dataIVBasic( self.testID )
        
        rows_list = []
        for index, row in dataIVBasic_df.iterrows():
        
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
                                      'ACR(Ohm)': '',
                                    'dV/dt(V/s)': '',
                      'Internal_Resistance(Ohm)': '',
                                   'dQ/dV(Ah/V)': '',
                                   'dV/dQ(V/Ah)': '',
                              'Aux_Voltage_3(V)': '',
                                'Aux_dV/dt_3(V)': '',
                          'Aux_Temperature_3(C)': '',
                                'Aux_dT/dt_3(C)': '' }
            
            rows_list.append(dict_values)

        return pd.DataFrame(rows_list)   


    def cycleStatistics( self ):

        statisticData_df = self.arbinDatabase.statisticData( self.testID )
        
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
        







