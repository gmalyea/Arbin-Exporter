import os
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
# - Timezone: is time off by 1 hour?
# - Need to figure out if new export needed based on age of data
# - Test #50 - missing last row of Aux data.  Bug?  Drop row?
# - Check if Test_ID inputted is valid
#
# QUESTIONS
# - Channel number in worksheet names?
# - Resume Table
# - If two test have same name, overwrite?
# =============================================================================


# SQL Server Connection Constants
# -----------------------------------------------------------------------------
SERVER = 'localhost'
USERNAME = 'sa'
PASSWORD = 'Garret2020'
OUTPUTPATH = "../Exporter Output/"


# Parse the Command-Line Arguments
# -----------------------------------------------------------------------------
# Using Python's included argparse
#  https://docs.python.org/3.3/library/argparse.html
#  https://docs.python.org/3/howto/argparse.html
# -----------------------------------------------------------------------------
parser = argparse.ArgumentParser(description='Exports Arbin data as Excel files.')
parser.add_argument('tests', nargs='*', type=int, help='list of specific Test_IDs to process')
parser.add_argument('-l', '--list', action='store_true', help='display a list of all tests in the database')
parser.add_argument('-o', '--output', action='store', help='specify an output folder')
args = parser.parse_args()


# Connect to the database
# -----------------------------------------------------------------------------
arbinDatabase = ArbinDatabase( SERVER, USERNAME, PASSWORD )


# Command-Line: List
# -----------------------------------------------------------------------------
if args.list:
    list_df = arbinDatabase.tests_all_detailed()
    df = ArbinTestItem.convert_date_time( list_df, 'First_Start_DateTime', 's', 1 )
    print( df.sort_values('Test_ID').to_string(index=False) )
    exit()


# Command-Line: Output
# -----------------------------------------------------------------------------
if args.output:
    OUTPUTPATH = args.output
    

# Get Tests and Process
# -----------------------------------------------------------------------------
if args.tests:
    tests = args.tests
else:
    tests = arbinDatabase.list_tests()


for test_id in tests:
    # Process Test
    print( "Processing Test ID: " + str(test_id) )
    arbinTest = ArbinTestItem( test_id, arbinDatabase )
    
    # Export
    arbinExport = ArbinExport( arbinTest )
    arbinExport.save_workbook( OUTPUTPATH )
    
    

print( "DONE" )
