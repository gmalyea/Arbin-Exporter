import pandas as pd
import pyodbc
from DBUtil import DBUtil
import xml.etree.ElementTree as ET
import openpyxl

#Includes ExportData.dll  
import clr
#clr.AddReference("ExportData")
#from ExportData import SQLExportUnitPro8

def getLocalConfigSettings():
    """Gets Local Database Settings from the DB config file in Arbin Software folder"""

    #path = "C:\ArbinSoftware\MITS_PRO\ArbinSys.DBCF"

    #tree = ET.parse(path)
    #root = tree.getroot()
    settings = {"Server":"sqlserver", "Username": "sa", "Password":"Garret2020"}

#    for item in root:
#        if item.tag == "LocalPort":
#            settings["Server"] = item.text
#        if item.tag == "LocalLogin":
#            settings["Username"] = item.text
#        if item.tag == "LocalPwd":
#            settings["Password"] = item.text


    return settings 

while(True):

    DoPythonExport = True #True executes all queries in Python, False will use the ExportData.dll
    ExportType = 1 # 1 For CSV 0 for Excel

    ServerAddress = ""
    Database = ""
    Username = ""
    Password = ""
    while(True):
        try:
            print("This tool can export test data from your Arbin Result Data Tables")

            ServerAddress = input("Press enter to connect to the local server, otherwise, enter the IP address of the server you want to connect to\n")
            if ServerAddress == "":
                settings = getLocalConfigSettings()
                ServerAddress = "(local)\\" + settings["Server"]
                Username = settings["Username"]
                Password = settings["Password"]
            else:
                Username = input("Enter your Username: ")
                Password = input("Enter your Password: ")

            DBLocal = DBUtil(ServerAddress, Username, Password, ExportType)

            #Initialize the master database list
            DBLocal.setMasterDatabaseList()
            DBLocal.printMasterDatabaseList();
            break
        except Exception as e:
            print("Error: ")
            print(e)
            print("\n\n")

    while(True):
        try:
            index = int(input("Select the index of the database associated with your cycler: "))

            DBLocal.database = DBLocal.masterDataBases.iloc[index - 1, 0]
            Database = DBLocal.database
            #Initialize the test list from the master database
            DBLocal.setTestList()
            DBLocal.printTestList();
            break
        except Exception as e:
            print("Error: ")
            print(e)
            print("\n\n")

    while(True):
        try:
            index = int(input("Select the index of the test you want to export: "))

            testName = DBLocal.testlist.iloc[index - 1, 5]
            testID = DBLocal.testlist.iloc[index - 1, 0]
            break
        except Exception as e:
            print("Error: ")
            print(e)
            print("\n\n")

    emptyTest = False
    while(True):
        try:
            print("\nCalculating export size...")
            #Load the test info from the selected test
            DBLocal.setTestItem(testName, testID)
            break
        except Exception as e:
            print("Error: ")
            print(e)
            print("\n\n")
            emptyTest = True
    
    if emptyTest:
        continue

    while(True):
        try:
            #Set additional export parameters on the test data
            print("\nTo export data within a range of time, enter \"-t\" followed by the range. Ex. \"-t 0-100\"")
            print("To export data within a cycle range, enter \"-c\" followed by the range. Ex. \"-c 1-2\"")

            range = input("Press enter to select all data\n");

            if("-t" in range):
                range = range.split(" ")[1].split("-")
                maxTime = range[1]
                minTime = range[0]
                DBLocal.testItem.setTimeRange(minTime, maxTime)
                DBLocal.timeBounds = True
            elif("-c" in range):
                range = range.split(" ")[1].split("-")
                maxCycle = range[1]
                minCycle = range[0]
                DBLocal.testItem.setCycleRange(minCycle, maxCycle)
                DBLocal.cycleBounds = True
            break
        except Exception as e:
            print("Error: ")
            print(e)
            print("\n\n")

    
    ##########PYTHON EXPORT###############
    if DoPythonExport:
        resultPath = ""

        if ExportType == 0:
            resultPath = "C:\\ArbinResultData\\"

        if ExportType == 1:
            resultPath = "C:\\ArbinResultDataCSV\\"

        try:
            print("\nExporting Data...")
            #Perform data export using pure python
            DBLocal.doExport(resultPath)
            print("Data exported to %s" % resultPath)
        except Exception as e:
            print("Error: ")
            print(e)
            print("\n\n")

        try:
            DoStats = input("\nWould you also like to export the statistics sheets? (Y/N): ")

            if(DoStats == "Y" or DoStats == "y"):
                print("\nExporting Statistics Data...\n")
                DBLocal.ExportStatsByCycle(resultPath)
        except Exception as e:
            print("Error: ")
            print(e)
            print("\n\n")
            continue

    ########################################

    ########### C# EXPORT###################
    else:
        try:
            Conn = ("Server=%s;Database=%s;User Id=%s;Password=%s;" % (ServerAddress,Database,Username,Password))
            print("\nExporting Data...")

            ExportUnit = SQLExportUnitPro8()
            ExportUnit.SetConnectionString(Database, Conn)

            del range 
            for i in range(len(DBLocal.testItem.Channels)):

                testID = str(DBLocal.testItem.testID)
                CycleStart = 0
                CycleEnd = 0

                if DBLocal.cycleBounds:
                    CycleStart = int(DBLocal.testItem.StartCycles[i])
                    CycleEnd = int(DBLocal.testItem.EndCycles[i])

                StepStart = 0;
                StepEnd = 0
                CyclePerFile = 0
                CANdata = False
                CANsheet = False

                AUXdata = False

                if DBLocal.testItem.IncludeAuxData:
                    AUXdata = True

                SMBdata = False
                SMBsheet = False
                StatisticCycle = False
                StatisticStep = False

                Channel = str(DBLocal.testItem.Channels[i])

                IncludeGraph = False

                TimePercentStart = 0
                TimePercentEnd = 0
                PointPercentStart = 0;
                PointPercentEnd = 0
                StartTime = 0
                EndTime = 0

                if DBLocal.timeBounds:
                    StartTime = int(DBLocal.testItem.StartTimes[i])
                    EndTime = int(DBLocal.testItem.EndTimes[i])
                
                resultPath = ""    
                if ExportType == 0:
                    resultPath = "C:\\ArbinResultData\\"
                elif ExportType ==1:
                    resultPath = "C:\\ArbinResultDataCSV\\"
                    
                ExportUnit.SetFilePath(str(resultPath))

                #Perform data export using Data Watcher's export function written in C# using the Pythonnet library
                ExportUnit.ExportDataPro8(testID,CycleStart,CycleEnd,StepStart,StepEnd,CyclePerFile,CANdata,CANsheet,AUXdata,SMBdata,SMBsheet,
                                          StatisticCycle,StatisticStep,Channel,IncludeGraph,TimePercentStart,TimePercentEnd,PointPercentStart,
                                          PointPercentEnd,StartTime,EndTime,ExportType)
                
                print("Data exported to %s" % resultPath)

        except Exception as e:
            print("Error: ")
            print(e)
            print("\n\n")
            continue

        

    ########################################
    print("\n*****************************")
    print("* Data succesfully exported *")
    print("*****************************")
    print("\n--------------------------------------------------------------------------\n")

