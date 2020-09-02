import pyodbc
import pandas as pd
from ArbinDatabase import ArbinDatabase
from ArbinTestItem import ArbinTestItem
from ArbinExport import ArbinExport

# =============================================================================
# List Tests
# -----------------------------------------------------------------------------
# Issues
# -----------------------------------------------------------------------------
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


# Get Tests and Print
# -----------------------------------------------------------------------------
tests = arbinDatabase.test_list()

print( "Test List" )
print( "-----------------------" )

for index, row in tests.iterrows():
    # Get Test ID
    testID = row['Test_ID']
    
    # Process Test
    print( str(testID) )


print( "END" )
