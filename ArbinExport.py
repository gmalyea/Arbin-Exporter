import pyodbc
import pandas as pd
import openpyxl
import openpyxl.utils.dataframe
import openpyxl.styles.colors
import datetime
import dateutil
import os
from ArbinTestItem import ArbinTestItem


class ArbinExport( object ):

    def __init__( self, arbinTestItem ):
        
        self.arbinTestItem = arbinTestItem
        
        currentDate = str( datetime.datetime.now() )
        currentDate = currentDate.split(" ")[0]
        self.fileName = arbinTestItem.testName + "_" + currentDate + ".xlsx"
        
        self.wb = openpyxl.Workbook()
        self.ws1 = self.wb.active
        self.ws1.title = "Global Info"
        self.ws2 = self.wb.create_sheet("Channel")
        self.ws3 = self.wb.create_sheet("Statistics")


    def saveWorkbook( self, path ):
        
        self.exportGlobalInfoSheet( self.ws1 )
        self.exportChannelSheet( self.ws2 )
        self.exportStatisticsSheet( self.ws3 )
        
        if( not os.path.isdir(path) ): os.makedirs(path)
        self.wb.save( path + self.fileName )
    
        

    def exportGlobalInfoSheet( self, worksheet ):

        worksheet.append(['','','','TEST REPORT'])
        worksheet.append(['','','Test Name'])
        worksheet.append(['','','Export Time'])
        
        for r in openpyxl.utils.dataframe.dataframe_to_rows(self.arbinTestItem.dbGlobalInfo_df, index=False, header=True):
            worksheet.append(r)
             
        # Green header background        
        self.backgroundColor( worksheet, 3, 'CEFFCE' )
        
        # Resize cells to fit  
        self.resizeCells( worksheet, slice(3,5) )
             

    def exportChannelSheet( self, worksheet ):
        
        for r in openpyxl.utils.dataframe.dataframe_to_rows(self.arbinTestItem.dbRawData_df, index=False, header=True):
            worksheet.append(r)
    
        # Blue header background
        self.backgroundColor( worksheet, 0, 'CEFFFF' )
    
        # Resize cells to fit
        self.resizeCells( worksheet, slice(0,2) )
    
    
    def exportStatisticsSheet( self, worksheet ):
        
        for r in openpyxl.utils.dataframe.dataframe_to_rows(self.arbinTestItem.dbCycleStatistics_df, index=False, header=True):
            worksheet.append(r)
    
        # Blue header background
        self.backgroundColor( worksheet, 0, 'CEFFFF' )
    
        # Resize cells to fit
        self.resizeCells( worksheet, slice(0,2) )    
    
    
    
    # --------------------------------------------------------------------------------------
    # Utilities
    # --------------------------------------------------------------------------------------
    def backgroundColor( self, worksheet, row, color ):
    
        backgroundFill = openpyxl.styles.fills.PatternFill(start_color=color, end_color=color, fill_type='solid')
        for cell in list(worksheet.rows)[row]:
            cell.fill = backgroundFill
    
    
    def resizeCells( self, worksheet, rows_to_check ):
    
        dims = {}
        for row in list (worksheet.rows)[rows_to_check]:
            for cell in row:
                if cell.value:
                    dims[cell.column_letter] = max((dims.get(cell.column_letter, 0), len(str(cell.value)))) 
        for col, value in dims.items():
            worksheet.column_dimensions[col].width = value
        
        
    
    
    
#    def exportExcel(self, df):
#
#        test = self.testItem
#
#        FileType = ""
#        if(self.exportType == 0):
#            FileType = ".xlsx"
#        elif(self.exportType == 1):
#            FileType = ".csv"
#
#        currentDate = str(datetime.datetime.now())
#        currentDate = currentDate.split(" ")[0]
#        path += test.testName + "_" + currentDate
#
#        if(not os.path.isdir(path)):
#            os.makedirs(path)
#
#        
#        # LIMIT TO 1,000,000 data points per file?
#
#        #tbl = pd.merge(IV_Basic_tbl, IV_Extended_tbl, on='Date_Time', how='outer')
#
#
#
#                #Convert from UTC to date
#                tbl['Date_Time'] = tbl['Date_Time'].apply(lambda x: x * 100)
#                tbl['Date_Time'] = str(pd.to_datetime(tbl['Date_Time'], unit='ns'))
#
#                #Write to CSV
#                fileName = path + "\\" + test.testName + "_Channel_" + str(test.Channels[i]) + "_IV&Aux__Wb_" + str(wbIndex) + FileType
#
#                if self.timeBounds:
#                    fileName = path + "\\" + test.testName + "_Channel_" + str(test.Channels[i]) + "_TestTime_" + test.StartTimes[i] + "-" + test.EndTimes[i] + "_IV&Aux__Wb_" + str(wbIndex) + FileType
#
#                if self.cycleBounds:
#                    fileName = path + "\\" + test.testName + "_Channel_" + str(test.Channels[i]) + "_Cycle_" + test.StartCycles[i] + "-" + test.EndCycles[i] + "_IV&Aux__Wb_" + str(wbIndex) + FileType
#
#                if self.exportType == 1:
#                    tbl.to_csv(fileName, index=False)
#                elif self.exportType == 0:
#                    tbl.to_excel(fileName, index=False)
#
#                wbIndex += 1





 

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
