import pyodbc
import pandas as pd
import datetime
import dateutil
import os


DATABASE_MASTER = 'ArbinPro8MasterInfo'
DATABASE_INFO = 'ArbinPro8Info_1'
DATABASE_DATA = 'ArbinPro8Data_1'

class ArbinDatabase( object ):

    def __init__( self, server, username, password ):
        
        try:
            self.conn_master = pyodbc.connect( 'DRIVER={ODBC Driver 17 for SQL Server}; SERVER='+server+'; DATABASE='+DATABASE_MASTER+'; UID='+username+'; PWD='+password )
        except Exception as e:
            print( "Could not connect to SQL Server, check the connection setting and try again." )

        try:
            self.conn_info = pyodbc.connect( 'DRIVER={ODBC Driver 17 for SQL Server}; SERVER='+server+'; DATABASE='+DATABASE_INFO+'; UID='+username+'; PWD='+password )
        except Exception as e:
            print( "Could not connect to SQL Server, check the connection setting and try again." )
            
        try:
            self.conn_data = pyodbc.connect( 'DRIVER={ODBC Driver 17 for SQL Server}; SERVER='+server+'; DATABASE='+DATABASE_DATA+'; UID='+username+'; PWD='+password )
        except Exception as e:
            print( "Could not connect to SQL Server, check the connection setting and try again." )


    def testList( self ):
        query = ("SELECT Test_ID from TestList_Table")
        return pd.read_sql( query, self.conn_master )


    def testListFor( self, testID ):
        query = ("SELECT * from TestList_Table WHERE [Test_ID] = " + str(testID))
        return pd.read_sql( query, self.conn_master )


    def dataIVBasic( self, testID ):
        query = ( "SELECT * FROM IV_Basic_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY date_time" )
        #query = ("SELECT [Data_Point], [Date_Time], [Test_Time], [Step_Time], [Cycle_ID], [Step_ID], [Current] as [Current (A)], [Voltage] as [Voltage (V)],"
        #            "[Charge_Capacity] as [Charge_Capacity (Ah)], [Discharge_Capacity] as [Discharge_Capacity (Ah)], [Charge_Energy] as [Charge_Energy (Wh)],"
        #            "[Discharge_Energy] as [Discharge_Energy (Wh)] FROM IV_Basic_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY date_time")
        
        return pd.read_sql( query, self.conn_data )

        
    def dataIVExtended( self, testID ):
        #query = ( ("SELECT [Date_Time], [6] as [ACR(Ohm)], [27] as [dV/dt(V/s)],[30] as [Internal_Resistance(Ohm)], [82] as [dQ/dV(Ah/V)], [83] as [dV/dQ(V/Ah)]" +
        #               "FROM (select * from IV_Extended_Table WHERE [Test_ID] = %s) as tbl " +
        #               "PIVOT (SUM([Data_value]) FOR [Data_Type] IN ([6],[27],[30],[82],[83])) as pvt%d" )
        #                % (TestID, Channel,startDateTime, endDateTime, firstPoint, lastPoint, StartTime, EndTime, firstCycle, LastCycle, i))
        
        return pd.read_sql( query, self.conn_data )
        
        
    def testIVChannelList( self, testID ):
        query = ("SELECT * from TestIVChList_Table WHERE [Test_ID] = " + str(testID))
        
        return pd.read_sql( query, self.conn_master )
      
      
    def statisticData( self, testID ):
        query = ("SELECT * from StatisticData_Table WHERE [Test_ID] = " + str(testID) + " ORDER BY date_time")
        
        return pd.read_sql( query, self.conn_data )
        
        
    def IVExtendedQuery(self, DBList, TestID, Channel,startDateTime, endDateTime, firstPoint, lastPoint, StartTime, EndTime, firstCycle, LastCycle):
        query = ""

        for i in range(len(DBList)): #Loops through each result database to form data query 
            query += (("SELECT [Date_Time], [6] as [ACR(Ohm)], [27] as [dV/dt(V/s)],[30] as [Internal_Resistance(Ohm)], [82] as [dQ/dV(Ah/V)], [83] as [dV/dQ(V/Ah)]" +
                       "FROM (select * from %s.dbo.IV_Extended_Table WHERE [Test_ID] = %s AND [Channel_ID] = %s AND [Date_Time] >= %s AND [Date_Time] <= %s " +
                       "AND [Data_Point] >= %d AND [Data_Point] <= %d AND [Test_Time] >= %s AND [Test_Time] <= %s AND [Cycle_ID] >= %s AND [Cycle_ID] <= %s) as tbl " +
                       "PIVOT (SUM([Data_value]) FOR [Data_Type] IN ([6],[27],[30],[82],[83])) as pvt%d" )
                        % (DBList[i], TestID, Channel,startDateTime, endDateTime, firstPoint, lastPoint, StartTime, EndTime, firstCycle, LastCycle, i))

            if i != len(DBList)- 1:
                query += " UNION ALL ";


        query += " ORDER BY date_time"
        #Excecute the Query 
        tbl = pd.read_sql(query, self.conn)

        return tbl
        
        
        
        
        

    def doExport(self, path):
        """Performs the query to get the IV data associated with the Test Object"""

        test = self.testItem

        FileType = ""
        if(self.exportType == 0):
            FileType = ".xlsx"
        elif(self.exportType == 1):
            FileType = ".csv"

        currentDate = str(datetime.datetime.now())
        currentDate = currentDate.split(" ")[0]
        path += test.testName + "_" + currentDate

        if(not os.path.isdir(path)):
            os.makedirs(path)

        for i in range(len(test.Channels)): #Loops through all Channels
            wbIndex = 1;
            for j in range(0,test.maxDataPoints[i],1000000): #Creates a new file every 1000000 rows
                query = ""

                IV_Basic_tbl = self.IVBasicQuery(test.ResultDB, test.testID, test.Channels[i], test.StartDateTimes[i],test.EndDateTimes[i], j, j + 1000000, test.StartTimes[i],test.EndTimes[i], test.StartCycles[i], test.EndCycles[i])
                IV_Extended_tbl = self.IVExtendedQuery(test.ResultDB, test.testID, test.Channels[i], test.StartDateTimes[i],test.EndDateTimes[i], j, j + 1000000, test.StartTimes[i],test.EndTimes[i], test.StartCycles[i], test.EndCycles[i])

                tbl = pd.merge(IV_Basic_tbl, IV_Extended_tbl, on='Date_Time', how='outer')

                if(test.IncludeAuxData):
                    #Merge Aux Table into IV table
                    Aux_tbl = self.AuxQuery(test.ResultDB, test.testID, i,test.StartDateTimes[i],test.EndDateTimes[i], j, j + 1000000, test.StartTimes[i],test.EndTimes[i], test.StartCycles[i], test.EndCycles[i])
                    tbl = pd.concat([tbl, Aux_tbl], axis=1)
                    tbl = tbl.drop(columns=['Date_Time_Aux'])
                    tbl.dropna(subset=['Data_Point'], inplace=True)

                #Convert from UTC to date
                tbl['Date_Time'] = tbl['Date_Time'].apply(lambda x: x * 100)
                tbl['Date_Time'] = str(pd.to_datetime(tbl['Date_Time'], unit='ns'))

                #Write to CSV
                fileName = path + "\\" + test.testName + "_Channel_" + str(test.Channels[i]) + "_IV&Aux__Wb_" + str(wbIndex) + FileType

                if self.timeBounds:
                    fileName = path + "\\" + test.testName + "_Channel_" + str(test.Channels[i]) + "_TestTime_" + test.StartTimes[i] + "-" + test.EndTimes[i] + "_IV&Aux__Wb_" + str(wbIndex) + FileType

                if self.cycleBounds:
                    fileName = path + "\\" + test.testName + "_Channel_" + str(test.Channels[i]) + "_Cycle_" + test.StartCycles[i] + "-" + test.EndCycles[i] + "_IV&Aux__Wb_" + str(wbIndex) + FileType

                if self.exportType == 1:
                    tbl.to_csv(fileName, index=False)
                elif self.exportType == 0:
                    tbl.to_excel(fileName, index=False)

                wbIndex += 1





    def AuxQuery(self, DBList, TestID, ChanIndex,startDateTime, endDateTime, firstPoint, lastPoint, StartTime, EndTime, firstCycle, LastCycle):

        auxChanList = self.testItem.AuxChannels
        auxTypeList = self.testItem.AuxTypes

        tbl = ""
        query = ""

        for i in range(len(auxChanList[ChanIndex])):
            query = ""
            for j in range(len(DBList)): #Loops through each result database to form data query 

                dtype = self.getAuxDataType(auxTypeList[ChanIndex][i])
                dname = self.getAuxColumnName(dtype, str(auxChanList[ChanIndex][i]))

                if(len(dtype) == 2):
                    query += (("SELECT [Date_Time] as [Date_Time_Aux], [%d] as [%s], [%d] as [%s]" +
                               "FROM(SELECT * FROM %s.dbo.Auxiliary_Table WHERE [AuxCh_Type] = %s AND [AuxCh_ID] = %s AND [Date_Time] > %d AND [Date_Time] < %d) AS tbl " +
                               "PIVOT( SUM(Data_Value) FOR Data_Type IN ([%d],[%d])) AS pvt%d" )
                                % (dtype[0], dname[0], dtype[1], dname[1], DBList[j], dtype[0], auxChanList[ChanIndex][i], startDateTime, endDateTime,  dtype[0], dtype[1], j))
                else:
                    query += (("SELECT [Date_Time] as [Date_Time_Aux], [Data_Value] as [%s]" +
                               "FROM %s.dbo.Auxiliary_Table WHERE [Data_Type] = %d AND [AuxCh_Type] = %d AND [AuxCh_ID] = %s AND [Date_Time] > %d AND [Date_Time] < %d")
                                % (dname[0], DBList[j], dtype[0], dtype[0], auxChanList[ChanIndex][i], startDateTime, endDateTime))

                if j != len(DBList)- 1:
                    query += " UNION ALL ";


            query += " ORDER BY [Date_Time_Aux] "
            #Excecute the Query 
            col = pd.read_sql(query, self.conn)

            if i != 0:
                tbl = pd.concat([tbl, col], axis=1)
            else:
                tbl = col


        return tbl

    def ExportStatsByCycle(self, path):
        test = self.testItem

        currentDate = str(datetime.datetime.now())
        currentDate = currentDate.split(" ")[0]
        path += test.testName + "_" + currentDate

        if(not os.path.isdir(path)):
            os.makedirs(path)

        for i in range(len(test.Channels)):
            query = ""
            for j in range(len(test.ResultDB)):    
                query += (("SELECT [Date_Time], [Test_Time] as [Test_Time(s)], [Step_Time] as [Step_Time(s)]" +
                           " ,[Cycle_ID] as [Cycle_Index]" 
                           " ,[Step_ID] as [Step_Index]"
                           " ,[Voltage] as [Voltage(V)]"
                           " ,[Current] as [Current(A)]"
                           " ,[Charge_Capacity] as [Charge_Capacity(Ah)]"
                           " ,[Discharge_Capacity] as [Discharge_Capacity(Ah)]"
                           " ,[Charge_Energy] as [Charge_Energy(Wh)]"
                           " ,[Discharge_Energy] as [Discharge_Energy(Wh)]"
                           " ,[Charge_Time] as [Charge_Time(s)]"
                           " ,[Discharge_Time] as [Discharge_Time(s)]"
                           " ,[V_Max_On_Cycle] as [V_Max_On_Cycle (V)]" 
                           " ,[Discharge_Capacity] * 100 /NULLIF([Charge_Capacity],0) as [Coulombic Efficiency %%]"
                           "FROM %s.dbo.StatisticData_Table WHERE [Test_ID] = %s AND [Channel_ID] = %d"
                           "AND [Step_ID] = (SELECT Max([Step_ID]) FROM %s.dbo.StatisticData_Table WHERE [Test_ID] = %s AND [Channel_ID] = %d)"
                           )

                        % (test.ResultDB[j], test.testID, test.Channels[i],test.ResultDB[j], test.testID, test.Channels[i]))

                if j != len(test.ResultDB)- 1:
                    query += " UNION ALL ";


            query += " ORDER BY date_time"

            #Excecute the Query 
            tbl = pd.read_sql(query, self.conn)

            #Write to CSV
            if self.exportType == 1:
                tbl.to_csv(path + "\\" + test.testName + "_Channel_" + str(test.Channels[i]) + "_StatisticsByCycle.csv", index=False)
            elif self.exportType == 0:
                tbl.to_excel(path + "\\" + test.testName + "_Channel_" + str(test.Channels[i]) + "_StatisticsByCycle.xlsx", index=False)




    def getAuxDataType(self,dataCode):
        if dataCode == "0": #Voltage and dV/dt
            return [0,30]
        elif dataCode == "1": #Temperature and dT/dt
            return [1,31]
        else:
            val = int(dataCode)
            return [val]


    def getAuxColumnName(self, dtype, chan):
        nameList = []
        for type in dtype:
            if type == 0:
                nameList.append("Aux_Voltage_" + chan + "(V)")
            if type == 1:
                nameList.append("Aux_Temperature_" + chan + "(C)")
            if type == 2:
                nameList.append("Aux_Pressure_" + chan + "(psi)")
            if type == 3:
                nameList.append("Aux_pH_" + chan + "(V)")
            if type == 4:
                nameList.append("Aux_Flow_Rate_" + chan + "(C)")
            if type == 5:
                nameList.append("Aux_Density_" + chan + "(psi)")
            if type == 6:
                nameList.append("Aux_Digital_Input_" + chan + "(V/s)")
            if type == 7:
                nameList.append("Aux_Digital_Output_" + chan + "(C/s)")
            if type == 8:
                nameList.append("Aux_EC_" + chan + "(C)")
            if type == 9:
                nameList.append("Aux_Safety_" + chan + "(psi)")
            if type == 10:
                nameList.append("Aux_Humidity_" + chan + "(V/s)")
            if type == 11:
                nameList.append("Aux_Analog_Output_" + chan + "(C/s)")
            if type == 30:
                nameList.append("Aux_dV/dt_" + chan + "(V/s)")
            if type == 31:
                nameList.append("Aux_dT/dt_" + chan + "(C/s)")

        return nameList
