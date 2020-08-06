import sys
import pandas as pd
from ArbinDatabase import ArbinDatabase



# What do we need in this?
# Dataframe from TestIVCHList_Table
# Max datapoints? for file export - just do this on the fly during export?
# Dataframes for data, info, auxiliary data, extended data

# Global Info
# -----------------------
# Test Name
# Export Time
# 
# Serial Number
# 
# Channel	
# Start DateTime	
# Schedule File Name	
# Creator	Comments	
# Software Version	
# Schedule Version	
# MASS(g)	
# Specific Capacity(Ah/g)	
# Capacity(Ah)
# Item ID	
# Has Aux	
# Has Specail	
# Log Aux Data Flag	
# Log Special Flag







class ArbinTestItem(object):
    
    def __init__( self, testID, arbinDatabase ):
        self.testID = testID
        self.arbinDatabase = arbinDatabase

        #self.testName = []
        #self.serialNumber = []
        
        #self.channel = []
        #self.startDateTime = []
        #self.scheduleFileName = []
        #self.scheduleVersion = []
        #self.specificMass = []
        #self.specificCapacity = []
        #self.capacity = []
        
        self.testList_df = arbinDatabase.testList( testID )
        #self.testIVChannelList_df = arbinDatabase.testIVChannelList( testID )
        #self.dataIVBasic_df = arbinDatabase.dataIVBasic( testID )
        #dataIVExtended_df = arbinDatabase.dataIVExtended( testID )
        
        
        
        
        self.testName = self.testList_df.at[0,'Test_Name']

        self.df_globalInfo = self.globalInfo()
        self.df_channel = self.channel()


        #for index, row in self.testIVChannelList_df.iterrows():
        #    print ("Here")
        #    self.channel.append(row["IV_Ch_ID"])
        #    self.startDateTime.append(row["First_Start_DateTime"])
        #    self.scheduleFileName.append(row["Schedule_File_Name"])
        #    self.scheduleVersion.append(row["Schedule_Version"])
        #    self.specificMass.append(row["SpecificMASS"])
        #    self.specificCapacity.append(row["SpecificCapacity"])
        #    self.capacity.append(row["Capacity"])

        
        #print (self.testName[0])

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


#        self.ResultDB = ResultDB
#        self.Channels = Channels
#        self.AuxChannels = AuxChannels
#        self.AuxTypes = AuxTypes
#        self.IncludeAuxData = IncludeAuxData
#        self.StartTimes = StartTimes
#        self.EndTimes = EndTimes
#        self.StartDateTimes = StartDateTimes
#        self.EndDateTimes = EndDateTimes
#        self.StartCycles = StartCycle
#        self.EndCycles = EndCycle
#        self.maxDataPoints = maxDataPoints
#





    def globalInfo( self ):

        testIVChannelList_df = self.arbinDatabase.testIVChannelList( self.testID )
        testList_df = self.arbinDatabase.testList( self.testID )
        
        rows_list = []
        for index, row in testIVChannelList_df.iterrows():
        
            dict_values = { 'Channel': row['IV_Ch_ID'],
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
        
        
    def channel( self ):

        dataIVBasic_df = self.arbinDatabase.dataIVBasic( self.testID )
        
        rows_list = []
        for index, row in dataIVBasic_df.iterrows():
        
            dict_values = { 'Data_Point': row['Data_Point'],
                            'Date_Time': row['Date_Time'],
                            'Test_Times(s)': row['Test_Time'],
                            'Step_Times(s)': row['Step_Time'],
                            'Cycle_Index': row['Cycle_ID'],
                            'Step_Index': row['Step_ID'] }
                            #'Current(A)': row['Current'],
                            #'Voltage(V)': row['Voltage'],
                            #'Power(W)': '',
                            #'Charge_Capacity(Ah)': row['Charge_Capacity'],
                            #'Discharge_Capacity(Ah)': row['Discharge_Capacity'],
                            #'Charge_Energy(Wh)': row['Charge_Energy'],
                            #'Discharge_Energy(Wh)': row['Discharge_Energy'],
                            #'ACR(Ohm)': '',
                            #'dV/dt(V/s)': '',
                            #'Internal_Resistance(Ohm)': '',
                            #'dQ/dV(Ah/V)': '',
                            #'dV/dQ(V/Ah)': '',
                            #'Aux_Voltage_3(V)': '',
                            #'Aux_dV/dt_3(V)': '',
                            #'Aux_Temperature_3(C)': '',
                            #'Aux_dT/dt_3(C)': '' }
            
            rows_list.append(dict_values)

        return pd.DataFrame(rows_list)   









