import pyodbc
import pandas as pd
from ArbinDatabase import ArbinDatabase
from ArbinTestItem import ArbinTestItem
from ArbinExport import ArbinExport

# ==============================================================================
# ASSUMPTIONS
# ------------------------------------------------------------------------------
# - Tests are only on one channel.
# - Data is only in one database.
#
#
#
# ==============================================================================
# Check for Max datapoints - do this on the fly during export?



#Power might be calculated
#Test Name: File Name
# Convert Times


SERVER = 'localhost'
USERNAME = 'sa'
PASSWORD = 'Garret2020'



arbinDatabase = ArbinDatabase( SERVER, USERNAME, PASSWORD )

tests = arbinDatabase.testList()

print( tests )
print( tests.iloc[0,0] )

arbinTest = ArbinTestItem( tests.iloc[0,0], arbinDatabase )

arbinExport = ArbinExport( arbinTest )
#arbinExport.exportGlobalInfoSheet()
#arbinExport.exportChannelData()
arbinExport.saveWorkbook( "Output/" )

#data = arbinDatabase.basicData( tests.iloc[0,0] )

#print( data )


print( "END" )



#Initialize the master database list
#DBLocal.setMasterDatabaseList()
#DBLocal.printMasterDatabaseList();

#index = 1
#DBLocal.database = DBLocal.masterDataBases.iloc[index - 1, 0]
#Database = DBLocal.database
#Initialize the test list from the master database
#DBLocal.setTestList()
#DBLocal.printTestList();


#index = int(input("Select the index of the test you want to export: "))

#testName = DBLocal.testlist.iloc[index - 1, 5]
#testID = DBLocal.testlist.iloc[index - 1, 0]


#print("\nCalculating export size...")
#Load the test info from the selected test
#DBLocal.setTestItem(testName, testID)



#Set additional export parameters on the test data
#print("\nTo export data within a range of time, enter \"-t\" followed by the range. Ex. \"-t 0-100\"")
#print("To export data within a cycle range, enter \"-c\" followed by the range. Ex. \"-c 1-2\"")
#
#range = input("Press enter to select all data\n");
#
#if("-t" in range):
#	range = range.split(" ")[1].split("-")
#	maxTime = range[1]
#	minTime = range[0]
#	DBLocal.testItem.setTimeRange(minTime, maxTime)
#	DBLocal.timeBounds = True
#elif("-c" in range):
#	range = range.split(" ")[1].split("-")
#	maxCycle = range[1]
#	minCycle = range[0]
#	DBLocal.testItem.setCycleRange(minCycle, maxCycle)
#	DBLocal.cycleBounds = True
#	
#	
#	
#
###########PYTHON EXPORT###############
#resultPath = ""
#
#if ExportType == 0:
#	resultPath = "ArbinResultData\\"
#
#if ExportType == 1:
#	resultPath = "ArbinResultDataCSV\\"
#
#
#print("\nExporting Data...")
##Perform data export using pure python
#DBLocal.doExport(resultPath)
#print("Data exported to %s" % resultPath)
#
#print("\nExporting Statistics Data...\n")
#DBLocal.ExportStatsByCycle(resultPath)
