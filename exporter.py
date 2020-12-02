import os
import sys
import time
import argparse
import pyodbc
import pandas as pd
from arbin.ArbinDatabase import ArbinDatabase
from arbin.ArbinTest import ArbinTest
from arbin.ArbinExport import ArbinExport

# =============================================================================
# Exporter
# -----------------------------------------------------------------------------
#
# Utility for exporting data from an Arbin Instruments battery test system 
# SQL Server to an excel file.
#
#
# Usage
# -----------------------------------------------------------------------------
# To get command-line help:
# >python exporter.py --help
#
# To export tests:
# >python exporter.py
#
# To export specific tests:
# >python exporter.py Test_ID1 Test_ID2 Test_ID3 etc...
#
#
# Dependencies
# -----------------------------------------------------------------------------
# > pip install pandas
# > pip install pyodbc
# > pip install openpyxl
#
#
# Issues
# -----------------------------------------------------------------------------
# - Two tests with the same name overwrite, should concatenate
# - Aux_Pressure/dt_1(PSI) not in database
#
# =============================================================================


# Constants
# -----------------------------------------------------------------------------
OUTPUTPATH = "../Arbin Exporter Output/"
TESTDAYSIGNORE = 30 # Days


# Constants: SQL Server
# -----------------------------------------------------------------------------
SERVER = 'localhost'
USERNAME = 'sa'
PASSWORD = ''


# Parse the Command-Line Arguments
# -----------------------------------------------------------------------------
# Using Python's included argparse
#  https://docs.python.org/3.3/library/argparse.html
#  https://docs.python.org/3/howto/argparse.html
# -----------------------------------------------------------------------------
parser = argparse.ArgumentParser( description="Exports Arbin data as Excel files." )
parser.add_argument( 'tests', nargs='*', type=int, help="list of specific Test_IDs to process" )
parser.add_argument( '-a', '--all', action='store_true', help="force output of all tests" )
parser.add_argument( '-l', '--list', action='store_true', help="display a list of all tests" )
parser.add_argument( '-o', '--output', action='store', help="specify an output folder" )
args = parser.parse_args()


# Connect to the database
# -----------------------------------------------------------------------------
arbinDatabase = ArbinDatabase( SERVER, USERNAME, PASSWORD )


# Show usage if no arguments
# -----------------------------------------------------------------------------
if not len(sys.argv) > 1:
    parser.print_usage()
    print()


# Command-Line: List
# -----------------------------------------------------------------------------
if args.list:
    list_df = arbinDatabase.test_list_display()
    df = ArbinTest.convert_date_time( list_df, 'First_Start_DateTime', 's', 1 )
    print( df.sort_values('Test_ID').to_string(index=False) )
    exit()


# Command-Line: Output
# -----------------------------------------------------------------------------
if args.output:
    OUTPUTPATH = args.output
    

# Get Tests to Process
# -----------------------------------------------------------------------------
all_tests = arbinDatabase.list_tests()
time_current = int(time.time())
time_ignore = TESTDAYSIGNORE * 24 * 60 * 60

tests = []
# Check if the inputted tests are valid
if args.tests:
    for test in args.tests:
        if test in all_tests:
            tests.append( test )
        else:
            print( "Invalid Test_ID: " + str(test) )

# Force all tests to be output
elif args.all:
    tests = all_tests
    
# Ignore tests older than TESTDAYSIGNORE
else:
    print( "Ignoring tests older than " + str(TESTDAYSIGNORE) + " days..." )
    for test in all_tests:
        time_last = arbinDatabase.last_datetime_for( test )
        if (time_current - time_last) < time_ignore:
            tests.append( test )


# Process
# -----------------------------------------------------------------------------
for test_id in tests:
    # Process Test
    print( "Processing Test ID: " + str(test_id) )
    arbinTest = ArbinTest( test_id, arbinDatabase )
    
    # Export
    arbinExport = ArbinExport( arbinTest )
    arbinExport.save_workbook( OUTPUTPATH )
    
    
print( "DONE" )
