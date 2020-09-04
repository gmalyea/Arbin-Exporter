import sys
import pyodbc
import pandas as pd
from arbin.ArbinDatabase import ArbinDatabase
from arbin.ArbinTestItem import ArbinTestItem
from arbin.ArbinExport import ArbinExport

# =============================================================================
# Exporter
# -----------------------------------------------------------------------------
# Usage
# -----------------------------------------------------------------------------
# To export all tests:
# >python export.py
#
# To export specific Test_IDs:
# >python export.py Test_ID1 Test_ID2 Test_ID3 etc...
#
# Issues
# -----------------------------------------------------------------------------
# - Tests are only on one channel?
# - Data is only in one database?
# - drop incomplete rows?
# - single channel aux data? - Test #46
# - Need to set properties or just use methods?
# - Clean up cell formatters
# - Statistics tab is "StatisticsByCycle"?
# - Channel and Statistics no "A" column?
# - Channel and Statistics DateTime doesn't scroll
# - Statistics need Power after Voltage
# - Statistics: Vmax, Coulombic, mAh/g, Aux
#
#
# QUESTIONS
# - Channel number in worksheet names?
# =============================================================================


# SQL Server Connection Constants
# -----------------------------------------------------------------------------
SERVER = 'localhost'
USERNAME = 'sa'
PASSWORD = 'Garret2020'


# Connect to the database
# -----------------------------------------------------------------------------
arbinDatabase = ArbinDatabase( SERVER, USERNAME, PASSWORD )


# Get Tests and Process
# -----------------------------------------------------------------------------
if sys.argv[1] != "":
    tests = pd.DataFrame(sys.argv[1:], columns = ['Test_ID']) 
else:
    tests = arbinDatabase.test_list()

for index, row in tests.iterrows():
    # Get Test ID
    testID = row['Test_ID']
    
    # Process Test
    print( "Processing Test ID: " + str(testID) )
    arbinTest = ArbinTestItem( testID, arbinDatabase )
    
    # Export
    arbinExport = ArbinExport( arbinTest )
    arbinExport.save_workbook( "Output/" )
    
    

print( "END" )
