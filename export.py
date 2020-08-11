import pyodbc
import pandas as pd
from ArbinDatabase import ArbinDatabase
from ArbinTestItem import ArbinTestItem
from ArbinExport import ArbinExport

# =============================================================================
# Export
# -----------------------------------------------------------------------------
# Issues
# -----------------------------------------------------------------------------
# - Tests are only on one channel?
# - Data is only in one database?
# - drop incomplete rows?
# - single channel aux data?
# - Check for Max datapoints - do this on the fly during export?
# - Power might be calculated
# - Test Name: File Name
# - Convert Times - order of date
#
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
tests = arbinDatabase.test_list()


arbinTest = ArbinTestItem( 43, arbinDatabase )
arbinExport = ArbinExport( arbinTest )
arbinExport.save_workbook( "Output/" )


#for index, row in tests.iterrows():
#    # Get Test ID
#    testID = row['Test_ID']
#    
#    # Process Test
#    print( "Processing Test ID: " + str(testID) )
#    arbinTest = ArbinTestItem( testID, arbinDatabase )
#    
#    # Export
#    arbinExport = ArbinExport( arbinTest )
#    arbinExport.save_workbook( "Output/" )
    


print( "END" )
