import sys
import argparse
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
# >python exporter.py
#
# To export specific Test_IDs:
# >python exporter.py Test_ID1 Test_ID2 Test_ID3 etc...
#
# -l    output list of tests
# -o    output folder
# -t    specify test
#
#
#
# Issues
# -----------------------------------------------------------------------------
# - single channel aux data? - Test #46
# - Timezone: is time off by 1 hour?
# - No command line test input doesn't work - need to figure out interface
#
# QUESTIONS
# - Channel number in worksheet names?
# - Resume Table
# - Test Name vs Schedule Name
# =============================================================================


# SQL Server Connection Constants
# -----------------------------------------------------------------------------
SERVER = 'localhost'
USERNAME = 'sa'
PASSWORD = 'Garret2020'


# Parse the Command-Line
# -----------------------------------------------------------------------------
parser = argparse.ArgumentParser(description='Exports Arbin data as Excel files.')
#parser.add_argument('tests', type=int, help='list of specific Test_IDs to process')
parser.add_argument('-l', '--list', action='store_true', help='display a list of all tests in the database')
#parser.add_argument('-o', '--output', dest='accumulate', action='store_const', help='specify an output folder')

args = parser.parse_args()








# Connect to the database
# -----------------------------------------------------------------------------
arbinDatabase = ArbinDatabase( SERVER, USERNAME, PASSWORD )


# Get Tests and Process
# -----------------------------------------------------------------------------
if args.list:
    list_df = arbinDatabase.tests_all_detailed()
    df = ArbinTestItem.convert_date_time( list_df, 'First_Start_DateTime', 's', 1 )
    print( df.sort_values('Test_ID').to_string(index=False) )
    exit()

if sys.argv[1] != "":
    #tests = pd.DataFrame(sys.argv[1:], columns = ['Test_ID'])
    tests = sys.argv[1:]
else:
    tests = arbinDatabase.list_tests()

for test_id in tests:
    # Get Test ID
    #testID = row['Test_ID']
    
    # Process Test
    print( "Processing Test ID: " + str(test_id) )
    arbinTest = ArbinTestItem( test_id, arbinDatabase )
    
    # Export
    arbinExport = ArbinExport( arbinTest )
    arbinExport.save_workbook( "../Exporter Output/" )
    
    

print( "END" )
