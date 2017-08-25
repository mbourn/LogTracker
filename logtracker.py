#!/usr/bin/env python3.5
######################################################################################################################
# NAME
## logtracker
#
# SYNOPSYS
## logtracker.py
#
# DESCRIPTION
## This program regularly scans the logs stored in the HFT system of record.  It notes if each device has log file(s)
## for the currect day.  It stores the results in a sqlite3 database.  It also compares the result for that 
## device in the current scan with the result from the previous scan.  It then generates a CEF event stating whether
## the device has current logs of, if not, if this is the first scan for which the device does not have current logs.
#
# AUTHORS
## Matthew Kaufman
## Matthew Bourn
#
# SEE ALSO
#
# 
# LICENSE
## This software is licensed in accordance with the GPLv3 (http://www.gnu.org/licenses/quick-guide-gplv3.en.html)
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED
## INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
## IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
## WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR 
## THE USE OR OTHER DEALINGS IN THE SOFTWARE.
## (c) Harbor Freight Tools 2017
#
######################################################################################################################
# ASSUMPTIONS: 
## 1) This program assumes that the device name  immediately follows the path in logDirPath and that the date of the 
##    log file is a directory name immediately under the device name: /path/to/logs/DEVICE/DATE/LOG_FILE or /path/to/
##    logs/DEVICE/DATE/HOUR/LOG_FILE. Modify line 529
## 2) When populating a db, the program assumes that every device that it finds with a log for today has a logging 
##    frequency of 1.  This will usually be true, but will occationally cause a device to erroneously be flagged as 
##    "Not Logging".  This should be smoothed out automatically after a few weeks.
######################################################################################################################
# TODO - Add weekly check for reactivated devices
######################################################################################################################
##### Import List #####
import os 
import re         # For pattern matching
import sys
import glob       # For manipulating filenames
import signal     # Allows for graceful exit on CTRL+C
import getopt     # For capturing command line arguments
import dbinit     # Custom module, initialize the database
import datetime   # For timestamps
import sqlite3 as lite    # For database access
from math import ceil     # Get rid of decimals

######################################################################################################################
### General variables ###
# Modify these variables 
daysToInactive = 60
devicesDontAudit = ["string01","/string02"]
pathLogger = "/usr/bin/logger"
logDirPath = "/path/to/central/logging"
pathToOpLog = "/path/to/logtracker/execution/logs/directory"
pathToDB = "/path/to/logtracker.db"
opLogName = "logTracker.log"
reportFileName = "logTrackerReport_"+ str(datetime.datetime.now()).split(".")[0].replace(" ","_").replace(":",".")
#
# Don't modify these variables 
devicesNew = []
devicesNotLogging = []
dateToday = str(datetime.date.today())
hourNow = int(getattr(datetime.datetime.now(), 'hour'))
ptrnDateSubDir = '/[0-9]{4}-[0-9]{2}-[0-9]{2}'
ptrnDateRecalcFreq = '[0-9]{4}-[0-9]{2}-[0-9]{2}'
if hourNow == 0:
    hourPrev = 23
else:
    hourPrev = hourNow - 1

### Database variables ###
# Table 'devices' structure
# dev_name TEXT, first_see TEXT, last_seen TEXT, freq INT, crit_sys INT, inactive INT, inactive_date INT, dev_id INT PK
tbl_devs = 'devices'
col_dname = 'dev_name'
col_fseen = 'first_seen'
col_lseen = 'last_seen'
col_freq = 'freq'
col_crit = 'crit_sys'
col_inact = 'inactive'
col_idate = 'inactive_date'
col_nlog = 'not_log'
col_nldate = 'notlog_date'
col_devid = 'dev_id'

### Text blocks ###
# Help text
helpText = "\n***** logtracker.py, by Matthew Bourn and Matthew Kaufman *****\n"
helpText+= "USAGE: ./logtracker.py <option> <argument>\n"
helpText+= "OPTIONS:\n"
helpText+= "  <NONE>            Running the program with no options will scan all actively logging directories for fresh\n"
helpText+= "                    logs and produce a report containing a list of newly discovered devices and a list of \n"
helpText+= "                    devices that have not logged in more days than their set logging frequency\n"
helpText+= "  -h  --help        Print this message\n"
helpText+= "  -c  --critical=   Toggle the Critical System status of the specified devices.  Takes the path to a text file\n"
helpText+= "                    as an argument.  That file should have only a single, case-sensitive device name per\n"
helpText+= "                    line.  If the device has a status of 1, it will be switched to 0 and vice versa\n"
helpText+= "  -C  --criticals   Only check critical systems for fresh logs\n"
helpText+= "  -f  --frequency=  Manually set the logging frequency for the specified devices.  Take the path to a text file\n"
helpText+= "                    as an argument.  That file should have only a single, case-sensitive device name per\n"
helpText+= "                    line, followed by a whole number integer for the number of days between logs, separated\n"
helpText+= "                    by a comma.\n"
helpText+= "  -i  --inactive=   Toggle the Inactive status of the specified devices.  Takes the path to a text file\n"
helpText+= "                    as an argument.  That file should have only a single, case-sensitive device name per\n"
helpText+= "                    line.  If the device has a status of 1, it will be switched to 0 and vice versa\n"
helpText+= "  -p  --populate    Scans the directory tree specified in the variables and automatically populates the \n"
helpText+= "                    with the devices it finds.  If a device has no logs newer than "+ str(daysToInactive) +"days \n"
helpText+= "                    old it is set to Inactive.  Devices with current logs under 'today', its requency will \n"
helpText+= "                    be set to 1, otherwise the frequency will be calculated and set.  This will fail if a database\n"
helpText+= "                    already exists in the specified path\n"
helpText+= "  -r  --report      Generate a report containing the devices that are not logging, critical systems, or are inactive. \n"
######################################################################################################################
### Function definitions ###

# Capture CTRL+C and exit gracefully
def signal_handler(signal, fram):
    log("[!] CTRL+C pressed. Exiting")
    print("\n[!] CTRL+C pressed. Exiting")
    raise SystemExit

# Write a line into the operations log
def log(logLine):
    with open(pathToOpLog+"/"+opLogName, "a+") as logFile:
        logFile.write(logLine)

# Send a CEF string to ArcSight via the logger
# 0 = Device is not logging when it should be
# 1 = Device is logging
# 2 = Device does not have a fresh log, but is not yet overdue
# 3 = Device has just gone overdue and has been listed as "Not Logging" 
# 4 = Device has been set to inactive due to prolonged inactivity
# 5 = Device has resumed logging and had its "not logging" bit flipped
# 6 = Device is new and added to the database
# 100 = An error has occurred
def cefMsg(devName,num):
    os.system(pathLogger +" \"CEF:0|HFT Infosec|HFT-Infosec-Utils|1.0|0|Asset-Logging-Status|3|msg="+ devName +" "+ str(num) +" cs1Label='Device Name' cs1=" + devName + " cs2Label='Event Number' cs2="+ str(num) + "\"")

# Start ops log
def logStart():
    try:
        if os.path.isfile(pathToOpLog+"/"+opLogName):
            log("\n----- "+ ''.join(str(datetime.datetime.now()).partition('.')[0:1]) +" -----\n") 
            log("[-] Start operations logging\n")
        else:
            with open(pathToOpLog+"/"+opLogName, "w") as logFile:
                logFile.write("====== GENERATING NEW LOG FILE ======\n")
                logEntry = "\n----- "+ ''.join(str(datetime.datetime.now()).partition('.')[0:1]) +" -----\n"
                logFile.write(logEntry) 
                logFile.write("[-] Start operations logging\n")
    except:
        print("\n[!] Unable to initialize or write to the operations log\n[!] Quitting\n\n")
        cefMsg("DB Error",100)
        raise SystemExit

# Sanitize directory names
# Returns the passed string leaving only a-z, A-Z, 0-9, /, ., and -
def cleanDirName(devName):
    # Don't strip periods if dirName is an IP address
    return re.sub('[^a-zA-Z0-9-./]+','',devName)


# Check database for duplicate entries with duplicate device names
# Takes a sqlite3 database cursor as an argument
# Fail if one is found
# This is mostly for diagnostics and burn-in testing
def dupCheck(c):
    try:
        c.execute("SELECT {dn}, COUNT(*) c FROM {tn} GROUP BY {dn} HAVING c > 1".format(dn=col_dname, tn=tbl_devs))
        r = c.fetchall()
    except lite.Error as e:
        log("[!] Query to test database for duplicates failed\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
        cefMsg("Query Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit
    if r:
        log("[!] Duplicates device names found\n")
        for i in r:
            log("[!][!] Device: "+ i[0] +" has "+ str(i[1]) +" entries\n")
        cefMsg("Duplicates Found",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit


# Get list of tuples of devices marked as Crtical Systems and non-Critical Systems
# Takes a sqlite3 database cursor
# Returns a list of 2 lists: list of tuples of critical systems and a list of tuples of non-critical systems
def getCritSystemList(dbc):
    # Get list of tuples of critial system devices and non-critical systems from database, append each to a list, return the list of lists.
    Y=1
    N=0
    devLists = []
    try:
        dbc.execute("SELECT * FROM {tn} WHERE {cs}={yes} AND {ia}={no}".format(tn=tbl_devs, cs=col_crit, ia=col_inact, yes=Y, no=N))
        devLists.append(dbc.fetchall())
        dbc.execute("SELECT * FROM {tn} WHERE ({cs}={no}) OR ({cs}={yes} AND {ia}={yes})".format(tn=tbl_devs, cs=col_crit, ia=col_inact, no=N, yes=Y))
        devLists.append(dbc.fetchall())
    except lite.Error as e:
        log("[!] Failed to get list of inactive devices\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
        cefMsg("Query Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit
    log("[+] Got critical systems list. It has "+ str(len(devLists[0])) +" entries in it\n")
    return devLists


# Query database, get the stats for all devices that are activley logging
# Takes a sqlite3 database cursor as an arguments
# Returns 2 lists of lists: list of tuples of actively logging devices and a list of tuples of inactive devices
def getActiveDeviceList(dbc):
    # Get list of tuples of active and inactive devices from database
    N=0
    Y=1
    devLists = []
    try:
        dbc.execute("SELECT * FROM {tn} WHERE {ia}={no}".format(tn=tbl_devs, ia=col_inact, no=N))
        devLists.append(dbc.fetchall())
        dbc.execute("SELECT * FROM {tn} WHERE {ia}={yes}".format(tn=tbl_devs, ia=col_inact, yes=Y))
        devLists.append(dbc.fetchall())
    except lite.Error as e:
        log("[!] Failed to get list of active devices\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
        cefMsg("Query Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit
    log("[+] Got active device list. It has "+ str(len(devLists[0])) +" entries in it\n")
    return devLists


# Compares today's date against date of last seen log, compares result against logging frequency for that device
# Takes a sqlite3 tuple as an argument
# Returns True if device has NOT created a new log file for a number of days greater than that device's logging frequency and an integer
# representing the number of days since the last new log was created, else False and the number of days
def checkLogFrequency(devStats):
    # Create datetime object from last_seen date devStats[2], compare with today's date, check against average logging frequency devStats[3]
    try:
        lseen = datetime.date(int(devStats[2].split('-')[0]), int(devStats[2].split('-')[1]), int(devStats[2].split('-')[2]))
        today = datetime.date.today()
        logFreq = datetime.timedelta(days=devStats[3])
    except:
        log("[!] Logging frequency comparison failed\n[!] Error: "+ str(sys.exc_info()[1]) +"\n[!] Exiting\n\n")
        cefMsg("checkLogFrequency Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit
        
    if today - lseen > logFreq:
        return [True, (today - lseen).days]
    else:
        return [False, (today - lseen).days]

# Collect device names with various statuses and feed them to the reportPrint() function:
# Takes a sqlite3 cursor object and two lists of strings as arguments
def reportMake():
    log("[-] Report data collection beginning\n")
    Y = 1
    devInact = []
    devCrit = []
    devNotLog = []

    # Connect to the database
    dbconn = dbMakeConnection(pathToDB)

    # Get a cursor for the database
    dbc =  dbMakeCursor(dbconn)

    # Get a list of all devices in the database
    try:
        dbc.execute("SELECT * FROM {dev} WHERE {cs} = {yes} OR {ia} = {yes} OR {nl} = {yes}".format(dev=tbl_devs, cs=col_crit, ia=col_inact, nl=col_nlog, yes=Y))
        allDevs = dbc.fetchall()
    except lite.Error as e:
        log("[!] Failed to get list of inactive devices\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
        cefMsg("Query Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit
    log("[+] Got list of critical, inactive, and not logging systems\n")

    # Step through list of devs and categorize each device
    # dev_name TEXT, first_seen TEXT, last_seen TEXT, freq INT, crit_sys INT, inactive INT, inactive_date TEXT, not_log INT, notlog_date TEXT, dev_id INTEGER PRIMARY KEY AUTOINCREMENT
    for dev in allDevs:
        if dev[4]:
            d = []
            d.append(dev[0])
            if dev[5]:
                d.append("INACTIVE")
            else:
                d.append("ACTIVE")
            if dev[7]:
                d.append("NOT LOGGING")
            else:
                d.append("LOGGING")
            devCrit.append(d)
        elif not dev[5] and dev[7]:
            devNotLog.append(dev[0])
        else:
            devInact.append(dev[0])
    log("[+][+] There are "+ str(len(devCrit)) +" Critical devices\n")
    log("[+][+] There are "+ str(len(devNotLog)) +" active devices that are not logging\n")
    log("[+][+] There are "+ str(len(devInact)) +" inactive devices\n")

    # Print the report
    reportPrint(devCrit, devNotLog, devInact)
    
# Print the report to a text file. It takes 5 lists of strings as arguments
# Creates the text document in the logging directory
def reportPrint(devCrit, devNotLog, devInact):
    # Create the text of the report
    log("[-] Printing report\n")
    body = "\n\n-----==== "+ reportFileName +" ====-----\n\n"
    body+= "--------------------------------------------------------\n"

    # Print not logging devices
    if devNotLog:
        body+= "[BEGIN NOT LOGGING]\n"
        body+= "Total: "+ str(len(devNotLog)) +"\n"
        for dev in devNotLog:
            body+= dev +"\n"
        body+= "[END NOT LOGGING]\n"
    else:
        body+= "--------------------------------------------------------\n"
        body+= "[THERE ARE NO DEVICES THAT ARE NOT LOGGING]\n"

    # Print critical systems
    if devCrit:
        body+= "--------------------------------------------------------\n"
        body+= "[BEGIN CRITICAL SYSTEMS]\n"
        body+= "Total: "+ str(len(devCrit)) +"\n"
        for dev in devCrit:
            body+= dev +"\n"
        body+= "[END CRITICAL SYSTEMS]\n"
    else:
        body+= "--------------------------------------------------------\n"
        body+= "[THERE ARE NO CRITICAL SYSTEMS]\n"
    
    # Print inactive devices
    if devInact:
        body+= "--------------------------------------------------------\n"
        body+= "[BEGIN INACTIVE]\n"
        body+= "Total: "+ str(len(devInact)) +"\n"
        for dev in devInact:
            body+= dev +"\n"
        body+= "[END INACTIVE]\n"
    else:
        body+= "--------------------------------------------------------\n"
        body+= "[THERE ARE NO INACTIVE DEVICES]\n"
    body+= "--------------------------------------------------------\n"
    
    # Create file and write the prepared text to the document
    try:
        with open(pathToOpLog+"/"+reportFileName, "w") as logFile:
            logFile.write(body)
    except:
        log("[!] Failed to write the report\n[!] Error: "+ str(sys.exc_info()[1]) +"\n[!] Exiting\n\n")
        cefMsg("Report Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit

# Find the string that matches the date pattern.  If found, everything before the string becomes the device name.
# Takes a string as an argument
# Returns a list of the devName and the discovered date
def getDevNameFromPath(p):
    # Search for a formatted date substring.  If found, return the prior substring as the devname and also the date
    # Convert the date into a datetime object
    res = re.search(ptrnDateSubDir,p)
    if res:
        date = datetime.date(int(p[res.start()+1:res.end()].split("-")[0]),int(p[res.start()+1:res.end()].split("-")[1]),int(p[res.start()+1:res.end()].split("-")[2]))
        return [p[:res.start()],date]
    # If neither of those conditions is met, we don't want whatever this path is for
    return [p,False]

# Confirm databse location, establish and return database connection
# Takes string of directory path as an argument
# Returns a sqlite3 database connection object
def dbMakeConnection(pathToDB):
    # Establish database connection
    log("[-] Looking for database\n")
    if os.path.isfile(pathToDB):
        log("[-] Database found, creating database connection\n")
        # Create the database connection
        try:
            dbconn = lite.connect(pathToDB)
        except lite.Error as e:
            log("[!] Failed to connect to the database\n[!] Error: " + str(e) + "\n[!] Exiting\n\n")
            cefMsg("DB Error",100)
            print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
            raise SystemExit
        log("[+] Database connection created\n")
    # If the database is not found, create a new one
    else:
        log("[!] No database found\n[!] Please run the program with the -p option to create and populate a database\n[!] Exiting\n\n")
        cefMsg("DB Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit
    return dbconn

# Create database cursor 
# Takes a sqlite3 database connection as an argument
# Returns a sqlite3 database cursor
def dbMakeCursor(dbconn):
    try:
        dbc = dbconn.cursor()
    except lite.Error as e:
        log("[!] Failed to create database cursor\n[!] Error: "+ str(e) +"[!] Exiting\n\n")
        cefMsg("DB Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit
    log("[+] Database cursor created\n")
    return dbc


# Populate a new database.  This function is highly dependant on your local directory structure
# Takes a sqlite3 connection, cursor, and string of a directory path as arguments
def dbPopulate(c, conn, path):
    dateToday = str(datetime.date.today())
    twoMonths = datetime.timedelta(days=daysToInactive)
    dbEntries = []
    dictDevDate = {}
    delFromDict = []
    
    # Make sure the database is empty before continuing
    try:
        pathToDB
        c.execute("SELECT * from {tn}".format(tn=tbl_devs))
        r = c.fetchone()
    except lite.Error as e:
        log("[!] SELECT query to see if the database is already populated failed\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
        cefMsg("Query Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit
    if r:
        log("[!] Database is not emtpy\n[!] Do not try to populate already populated databases\n[!] Exiting\n\n")
        cefMsg("DB Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit

    # Walk through the directory tree recursively 
    log("[-] Walking the directory tree looking for log files and devices\n")
    for p in glob.glob(path+'/**/*', recursive=True):

        # If the path ends in a file
        if os.path.isfile(p):

            # Find a string that matches the date pattern.  If found, everything before the date
            # string becomes the device name.  Returns list of name and the date
            #print(p)
            devName = getDevNameFromPath(p)
            if not devName[1]:
                continue
            else:  
                if devName[0] in dictDevDate:
                    dictDevDate[devName[0]].append(devName[1])
                else:
                    dictDevDate.setdefault(devName[0], [])
                    dictDevDate[devName[0]].append(devName[1])

    # Purge devices on the skip list
    for dev, date in dictDevDate.items():
        for name in devicesDontAudit:
            if name in dev:
                delFromDict.append(dev)
    for dev in delFromDict:
        if dev in dictDevDate:
            del dictDevDate[dev]
    
    # Step through the dictionary, determine the date of the most recent collected log, determine if the device 
    # is actively logging, determine a frequencey if there are relatively recent logs, add the appropriate
    # values string for the insert query
    log("[-] Calculating logging frequency and activity status for "+ str(len(dictDevDate)) +" devices\n")
    sumInactiveDevs = 0
    skipAnomDevices = []
    for dev, dates in dictDevDate.items():
        freq = 1
        # Test to make sure the list of dates was populated
        if not dates:
            skipAnomDevices.append(dev)
            continue

        # If the device hasn't logged in over 2 months, insert the device as inactive
        lastLogDate = max(dates)
        firstLogDate = min(dates)
        if datetime.date.today() - lastLogDate > twoMonths:
            entry =  (dev, str(firstLogDate), str(lastLogDate), '1','0','1', dateToday,'1',dateToday)
            dbEntries.append(entry)
            sumInactiveDevs += 1

        # If there is only one log entry, add it and set the frequency to half the number of days between now and the last log entry
        elif len(set(dates)) == 1:
            delta = datetime.date.today() - lastLogDate
            freq = ceil(delta.days/2)
            if freq == 0:
                freq = 1
            entry = (dev, str(firstLogDate), str(lastLogDate), freq, '0', '0', 'None','0','None') 
            dbEntries.append(entry)

        # Else determine the logging frequencey, set the frequency, and create the insert string
        else:
            i = 1
            sumOfDateDifferences = datetime.timedelta(days=0)
            dateSet = sorted(set(dates))

            #print(dev)
            #for i in dateSet:
            #    print(i)

            # Get sum of days between logs
            while i < len(dateSet):
                sumOfDateDifferences += dateSet[i] - dateSet[i-1]
                i += 1

            # Calculate the average logging period
            try:
                avgDelta = sumOfDateDifferences / (len(dateSet) - 1)
            except:
                log("[!] Error calculating average logging period for "+ dev +"\n[!] Error: "+ str(sys.exc_info()[1]) +"\n[!] Exiting\n\n")
                cefMsg("Math Error",100)
                print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
                raise SystemExit
            if avgDelta.days == 0:
                freq = '1'
            elif avgDelta.days < 0:
                log("[!] The calulated average logging frequency is negative: "+ str(avgDelta.days) +". Please inspect "+ dev +"\n[-] Continuing\n\n")
                #print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
                #raise SystemExit
                continue
            else:
                freq = avgDelta.days

            # Create the insert string and add it to the list
            entry = (dev, str(firstLogDate), str(lastLogDate), freq,'0','0','None','0','None')
            dbEntries.append(entry)

    # Bulk insert all devices into the database
    log("[+] Found "+ str(sumInactiveDevs) +" inactive devices.  Use the inactive device report option for more information\n")
    log("[-] Starting bulk insert of "+ str(len(dbEntries)) +" devices into the database\n")
    try:
        c.executemany("INSERT INTO {tn} ({dn}, {fs}, {ls}, {fq}, {cs}, {ia}, {iad}, {nl}, {nld}) VALUES (?,?,?,?,?,?,?,?,?)"\
        .format(tn=tbl_devs, dn=col_dname, fs=col_fseen, ls=col_lseen, fq=col_freq, cs=col_crit, ia=col_inact, iad=col_idate, nl=col_nlog, nld=col_nldate), (dbEntries))
        conn.commit()
        conn.close()
    except lite.Error as e:
        log("[!] Bulk insert of devices into fresh database failed\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
        cefMsg("Query Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit
    log("[+] Bulk insert completed successfully\n[-] All database population tasks completed successfully\n[-] Quitting.  Good bye.\n\n")
            

# Toggle the critical system or inactive setting or set the logging frequency on a device(s)
# Also recalculates the logging frequencies for the specified devices
# Takes the path to a text file as an argument, formatted as a string
# The file must contain a single, case sensitive device name per line and nothing else
# For logging frequency, each line should contain the device name and the frequency integer separated by a comma, e.g.: dev-1,20\n.
def toggleStatus(filePath,pathToDB, option):
    # Test filePath to make sure it leads to a file
    try:
        os.path.isfile(filePath)
    except:
        log("[!] Failed to find the file containing the list of devices\n[!] Error: "+ str(sys.exc_info()[1]) +"\n[!] Exiting\n\n")
        cefMsg("File Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit

    # Make the database connection and cursor
    dbconn = dbMakeConnection(pathToDB)
    dbc = dbMakeCursor(dbconn)

    # Get list of all devices and their stats
    try:
        dbc.execute("SELECT * FROM {tn}".format(tn=tbl_devs))
        devsAll = dbc.fetchall()
    except lite.Error as e:
        log("[!] Failed to get list of devices from database\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
        cefMsg("Query Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit
    log("[+] Got all devices in database\n")

    # Read device names into a list from the file, remove whitespace, and sanitize if not setting the logging frequencies
    if option != 3:
        devs = [cleanDirName(line.strip()) for line in open(filePath)]
        if not devs:
            log("[!] The file provided was empty\n[!] No work to be done, exiting\n\n")
            cefMsg("File Error",100)
            print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
            raise SystemExit

    # Get list of all device names
    devNamesAll = []
    for d in devsAll:
        devNamesAll.append(d[0])

    # Find any devices in the provided list that are not in the database and notify the user if not setting the loging frequencies
    if option != 3:
        devNamesMissing = list(set(devs) - set(devNamesAll))
        if devNamesMissing:
            log("[!] The following devices that you put on the list were not found in the database:\n")
            for name in devNamesMissing:
                log("[!][!]   "+ name +"\n")
            log("[!]  Please check your spelling and try again\n")
            print("[!] Some devices that you put on the list were not found in the database\n[!] Check the log for more info\n\n")
    
    # Parse the options
    dbEntries = []
    # Process for critical systems
    if option == 1:
        # Start log
        log("----- "+ str(datetime.date.today()) +" -----\n")
        log("[-] Starting process of toggling the \"Critital System\" setting for the specified devices\n")

        # Find specified devices in the database, create a list for values to be fed into the bulk update query that will toggle its critical system status
        for d in devsAll:
            if d[0] in devs:
                if d[4]:
                    entry = [0,d[9]]
                    dbEntries.append(entry)
                else:
                    entry = [1,d[9]]
                    dbEntries.append(entry)

        # Perform a bulk update query
        try:
            dbc.executemany("UPDATE {tn} SET {cs} = ? WHERE {did} = ?".format(tn=tbl_devs, cs=col_crit, did=col_devid), dbEntries)
            dbconn.commit()
        except lite.Error as e:
            log("[!] Bulk database update failed\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
            cefMsg("Query Error",100)
            print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
            raise SystemExit


    # Process for inactive
    elif option == 2:
        dateToday = str(datetime.date.today())
        # Start log
        log("----- "+ dateToday +" -----\n")
        log("[-] Starting process of toggling the \"Inactive\" setting for the specified devices\n")

        # Find specified devices in the database, create a list for values to be fed into the bulk update query that will toggle its inactive status
        for d in devsAll:
            if d[0] in devs:
                if d[5]:
                    entry = [0,dateToday,d[9]]
                    dbEntries.append(entry)
                else:
                    entry = [1,dateToday,d[9]]
                    dbEntries.append(entry)

        # Perform a bulk update query
        try:
            dbc.executemany("UPDATE {tn} SET {inc} = ?, {ind} = ? WHERE {did} = ?".format(tn=tbl_devs, inc=col_inact, ind=col_idate, did=col_devid), dbEntries)
            dbconn.commit()
        except lite.Error as e:
            log("[!] Bulk database update failed\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
            cefMsg("Query Error",100)
            print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
            raise SystemExit

    # Manually set the logging frequency for the specified device(s)
    elif option == 3:
        devs = []
        dnames = []
        # Start log
        log("----- "+ str(datetime.date.today()) +" -----\n")
        log("[-] Starting process of manually setting the loggin frequency for the specified devices\n")
        
        # Parse the list of devices and desired logging frequencies
        devsUnclean = [line.strip() for line in open(filePath)]
        if not devsUnclean:
            log("[!] The file provided was empty\n[!] No work to be done, exiting\n\n")
            cefMsg("File Error",100)
            print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
            raise SystemExit
        for du in devsUnclean:
            d = [cleanDirName(du.split(',')[0]), int(du.split(',')[1])]
            devs.append(d)
            dnames.append(d[0])
        
        # Check for unknown devices on the list of devices logging to the system and notify user if any are found
        devNamesMissing = list(set(dnames) - set(devNamesAll))
        if devNamesMissing:
            log("[!] The following devices were found to be logging but were not in the database:\n")
            for n in devNamesMissing:
                log("[!][!]   "+ n +"\n")
    
        # Create entries for the bulk database update
        for d in devsAll:
            if d[0] in dnames:
                entry = [devs[dnames.index(d[0])][1],d[9]]
                dbEntries.append(entry)

        # Bulk update of database
        try:
            dbc.executemany("UPDATE {tn} SET {fq} = ? WHERE {did} = ?".format(tn=tbl_devs, fq=col_freq, did=col_devid), dbEntries)
            dbconn.commit()
        except lite.Error as e:
            log("[!] Bulk database update failed\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
            cefMsg("Query Error",100)
            print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
            raise SystemExit

        
    # Or something bad happened
    else:
        log("[!] You shouldn't see this error, the programmer messed up\n[!]Exiting\n\n")
        cefMsg("Unknown Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit

    # Close the database connection
    dbconn.close()


# Calculate the logging frequency of a range of dates
# Takes a list of datetime date objects as an argument
# Returns an integer
# TODO Create a more nuanced algorithm
def calcFreq(dates):
    i = 1
    sumOfDateDifferences = datetime.timedelta(days=0)
    dateSet = sorted(set(dates))
    
    # Get sum of days between logs
    while i < len(dateSet):
        sumOfDateDifferences += dateSet[i-1] - dateSet[i]
        i += 1

    # Calculate the average logging period
    try:
        avgDelta = sumOfDateDifferences / (len(dateSet) - 1)
    except:
        return 0
    if avgDelta.days == 0:
        return 1
    else:
        return abs(avgDelta.days)

# The script's basic functionality: step through directory tree, check for fresh logs, check for devices for which
# the not logging frequency has been exceeded, check for devices that have resumed logging and reset their frequency, 
# check for newly inactive devices, check for previously unknown devices and enter them into the database.
# Takes two booleans as arguments
def runAudit(critsOnly, report):
    # Confirm databse location, establish database connection
    dbconn = dbMakeConnection(pathToDB)

    # Create databse cursor using the database connection just created
    dbc = dbMakeCursor(dbconn)

    # Make sure there are no duplicate device name entries in the database
    dupCheck(dbc)

    # Get list of actively logging devices
    if critsOnly:
        # If onlyCrits is set, only get critical systems
        devLists = getCritSystemList(dbc)
    else:
        # Get list of all actively logging and inactive devices.  Add inactive devices to DontAudit list
        devLists = getActiveDeviceList(dbc)

    dateToday = str(datetime.date.today())
    pastInactive = datetime.timedelta(days=daysToInactive)
    dirDepth = logDirPath.count("/") + 1
    dbEntries = []
    dbUpdates = []
    devAnom = []
    devKnown = []
    devUnknown = []
    dictDevDate = {}
    newDevs = [] #
    delFromDict = [] #

    log("[-] Checking active devices for fresh logs\n[-][-] "+ str(len(devLists[0])) +" active devices\n")
    for dev in devLists[0]:
        # If there is a log from today, send CEF 1.  If device was not logging before, send CEF 5,
        # recalc the frequency, and update the database entry
        if dateToday in os.listdir(dev[0]):
            cefMsg(dev[0], 1) 
            # If the device was "not logging" reset it
            if dev[7]:
                # Get a list of all subdirectories, filter date formatting, change to datetime object
                dates = []
                for d in os.listdir(dev[0]):
                    if re.match(ptrnDateRecalcFreq, d):
                        dates.append(datetime.date(int(d.split("-")[0]),int(d.split("-")[1]),int(d.split("-")[2])))
                freq = calcFreq(dates) 
                if freq == 0:
                    log("[!] Unable to calculate the logging frequency for "+ dev[0] +"\n[!] Exiting\n\n")
                    cefMsg("Math Error",100)
                    print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
                    raise SystemExit
                entry = (dev[0], dev[1], dateToday, freq, dev[4], dev[5], dev[6], 0, dateToday, dev[9])
                cefMsg(dev[0], 5)
            else:
                entry = (dev[0], dev[1], dateToday, dev[3], dev[4], dev[5], dev[6], dev[7], dev[8], dev[9])
            dbUpdates.append(entry)
        else:
            daysNotLog = datetime.date.today() - datetime.date(int(dev[2].split("-")[0]),int(dev[2].split("-")[1]),int(dev[2].split("-")[2]))
            # If the device has not logged recently, but is not overdue, send CEF 2 and move on
            if daysNotLog <= datetime.timedelta(days=dev[3]):
                cefMsg(dev[0], 2)

            # If the device has not logged in longer than the predefined limit, send CEF 4 and set it as inactive 
            elif daysNotLog > pastInactive:
                cefMsg(dev[0],4)
                entry = (dev[0], dev[1], dev[2], dev[3], dev[4], 1, dateToday, dev[7], dev[8], dev[9])
                dbUpdates.append(entry)

            # If the device is past its logging frequency send CEF 0
            else:
                cefMsg(dev[0], 0)

                # If the device isn't already set to "not logging", set it and send CEF 3
                if not dev[7]:
                    cefMsg(dev[0],3)
                    entry = (dev[0], dev[1], dev[2], dev[3], dev[4], dev[5], dev[6], 1, dateToday, dev[9])
                    dbUpdates.append(entry)

        # Separate standard paths from anomalous paths
        if dev[0].count("/") == dirDepth:
            devKnown.append(dev[0])
        else:
            devAnom.append(dev[0])

    # Sort the inactive devices
    log("[-] Sorting inactive devices\n[-][-] "+ str(len(devLists[1])) +" inactive devices\n")
    for dev in devLists[1]:
        if dev[0].count("/") == dirDepth:
            devKnown.append(dev[0])
        else:
            devAnom.append(dev[0])

    # Get logging directory listings
    devAll = os.listdir(logDirPath)
    log("[+] Got directory listing for "+ logDirPath +": "+ str(len(devAll)) +" device directories\n")

    # Prepend logging path to dev name
    devAll = [logDirPath +"/"+ i for i in devAll]

    log("[-] Removing known standard devices, unmonitored devices, and device directories with no subdirectories\n")
    # Remove known standard devices from device list
    devUnknown = list(set(devAll) - set(devKnown))
    #print(str(len(devUnknown)))

    # Remove unmonitored devices (View clients, etc)
    removeThese = [i for i in devUnknown for j in devicesDontAudit if j in i]
    #print(str(len(set(removeThese))))
    devUnknown = list(set(devUnknown) - set(removeThese))

    #print(str(len(devUnknown)))
    # Identify, enter into db, and remove listed devices with no log files
    devEmpty = [i for i in devUnknown if not os.listdir(i)]
    #print(str(len(devEmpty)))
    for dev in devEmpty:
        entry = (dev, dateToday, dateToday, 1, 0, 1, dateToday, 1, dateToday)    
        dbEntries.append(entry)
        cefMsg(dev, 3)
        cefMsg(dev, 4)
        cefMsg(dev, 6)
    log("[+] Added "+ str(len(dbEntries)) +" device directories with no subdirectories to the database\n")

		# Remove the device with no subdirectories from the list of unknown devices
    devUnknown = list(set(devUnknown) - set(devEmpty))

    # Remove anomalous parent dirs from devUnknown
    # Get a list of parent pathes with devices in subdirectories
    parentPaths = list(set([i for i in devUnknown for j in devAnom if i in j]))
    pathsKnown = list(set([i for i in devAnom for j in parentPaths if j in i]))
    #for i in parentPaths:
    #  print(i)
    log("[-] Beginning to process "+ str(len(parentPaths)) +" anomalous logging directories\n")
    for path in parentPaths:
        tree = []
        pathWithFile = []

        # Get tree of subdirectories
        for r,d,f in os.walk(path, topdown=True):
            tree.append([r,d,f])

        # Loop through subdirectories, ID, and process found devices
        for subDir in tree[1:]:
            # If this path ends in files add the path to the list
            known = [i for i in pathsKnown if i in subDir[0]]
            if not known:
                if subDir[2]:
                    #print(subDir[0])
                    pathWithFile.append(subDir[0])
                # Else if the path has subdirectories and their names are date formatted, 
                # add the path/device name to the dictionary, but no dates
                elif subDir[1] and (subDir[1][0] == 'today' or subDir[1][0] == 'yesterday' or re.match('[0-9]{4}-[0-9]{2}-[0-9]{2}', subDir[1][0])):
                    dictDevDate.setdefault(subDir[0], [])
    
        # Sort through the paths that end in files
        for p in pathWithFile:
            devName = getDevNameFromPath(p)
            if devName[0] in dictDevDate:
                dictDevDate[devName[0]].append(devName[1])
            else:
                dictDevDate.setdefault(devName[0], [])
                dictDevDate[devName[0]].append(devName[1])    
    
    # Remove the known parent paths for known anomalous devices
    devUnknown = list(set(devUnknown) - set(parentPaths))            
    #print(str(len(devUnknown)))

    # Process the remaining unknown devices
    log("[-] Processing "+ str(len(devUnknown)) +" unknown devices\n")
    for d in devUnknown:
        pathWithFile = []
  
        for r,d,f in os.walk(d, topdown=True):
            if f:
                pathWithFile.append(r)
            elif d and (d[0] == 'today' or d[0] == 'yesterday' or re.match('[0-9]{4}-[0-9]{2}-[0-9]{2}', d[0])):
                dictDevDate.setdefault(r, [])

        # Sort through the paths that end in files
        for p in pathWithFile:
            devName = getDevNameFromPath(p)
            if not devName[1]:
                continue
            elif devName[0] in dictDevDate:
                dictDevDate[devName[0]].append(devName[1])
            else:
                dictDevDate.setdefault(devName[0], [])
                dictDevDate[devName[0]].append(devName[1])
    
		# If there are any unknown devices were discovered and entered into the dictionary, process them
    if dictDevDate:
        # TODO find a more elegant way of preventing this path from being entered into the dictionary
        if '/var/log/HOSTS' in dictDevDate:
            del dictDevDate['/var/log/HOSTS']
        log("[-] Adding "+ str(len(dictDevDate)) +" newly discovered devices to the database\n")
        for dev, dates in dictDevDate.items():
            if dates:
                entry = (dev, str(min(dates)), str(max(dates)), 1, 0, 0, None, 0, None)
                dbEntries.append(entry)
                cefMsg(dev, 6)
            else:
                entry = (dev, None, None, 1, 0, 1, dateToday, 1, dateToday)
                dbEntries.append(entry)
                cefMsg(dev, 3)
                cefMsg(dev, 4)
                cefMsg(dev, 6)


    # Update the device database entries
    log("[-] Performing bulk update of "+ str(len(dbUpdates)) +" known devices\n")
    try:
        dbc.executemany("UPDATE {tn} SET {dn} =?, {fs} =?, {ls}=?, {fq}=?, {cs}=?, {ia}=?, {iad}=?, {nl}=?, {nld}=? WHERE {did}=?"\
        .format(tn=tbl_devs, dn=col_dname, fs=col_fseen, ls=col_lseen, fq=col_freq, cs=col_crit, ia=col_inact, iad=col_idate, nl=col_nlog, nld=col_nldate, did=col_devid),\
        dbUpdates)
    except lite.Error as e:
        log("[!] Bulk update of known devices during routine audit failed\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
        cefMsg("Query Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit
    log("[+] Known devices status successfully updated in the database\n")

    # Insert newly discovered devices into the database
    if dbEntries: 
        log("[-] Performing bulk insert of "+ str(len(dbEntries)) +" newly found devices\n")
        try:
            dbc.executemany("INSERT INTO {tn} ({dn}, {fs}, {ls}, {fq}, {cs}, {ia}, {iad}, {nl}, {nld}) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"\
            .format(tn=tbl_devs, dn=col_dname, fs=col_fseen, ls=col_lseen, fq=col_freq, cs=col_crit, ia=col_inact, iad=col_idate, nl=col_nlog, nld=col_nldate),\
            dbEntries)
        except lite.Error as e:
            log("[!] Bulk insert of new devices during routine audit failed\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
            cefMsg("Query Error",100)
            print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
            raise SystemExit
        log("[+] New devices successfully inserted into the database\n")

    # Commit changes and close the database connection
    log("[-] Commiting changes to the database\n")
    try:
        dbconn.commit()
        dbconn.close()
    except lite.Error as e:
        log("[!] Database commit() or close() during routine audit failed\n[!] Error: "+ str(e) +"\n[!] Exiting\n\n")
        cefMsg("Query Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit

    # If the report flag set, print the report
    if report:
        log("[-] Generating report\n")
        reportMake()

    log("[+] Changes successfully committed to the database\n[+] All auditing tasks completed successfully\n[-] Quitting.  Good bye.\n\n")


######################################################################################################################
### MAIN ###
def main(argv):
    critsOnly = False
    report= False
    # Capture CTRL+C and exit gracefully
    signal.signal(signal.SIGINT, signal_handler)

    # Confirm ops log location and writability
    logStart()

    # Get any commandline arguments and handle them
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hpCrf:i:c:", ["help","populate","onlyCrits","--report", "frequency=","inactive=","critical="])
    except:
        log("[!] Failed to capture commandline arguments\n[!] Error: "+ str(sys.exc_info()[1]) +"\n[!] Exiting\n\n")
        cefMsg("CLI argument Error",100)
        print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
        raise SystemExit

    if len(sys.argv) >= 2:
        for opt, arg in opts:
            # We all need help sometimes
            if opt in ("-h", "--help"):
                print(helpText)
                raise SystemExit

            # Recalculate the logging frequency for a specified device(s)
            elif opt in ("-f", "--frequency"):
                if len(sys.argv) > 3:
                    log("[!] Too many arguments for the frequency command\n[!] Please provide the path to a single text file containing the relevant devices\n")
                    log("[!] Exiting\n\n")
                    cefMsg("CLI argument Error",100)
                    print("[!] Commandline syntax error.  Check the log for more details or try '-h'\n\n")
                    raise SystemExit

                toggleStatus(arg,pathToDB,3)
                raise SystemExit

            # Toggle the "critical system" status of a device(s)
            elif opt in ("-c", "--critical"):
                if len(sys.argv) > 3:
                    log("[!] Too many arguments for the frequency command\n[!] Please provide the path to a single text file containing the relevant devices\n")
                    log("[!] Exiting\n\n")
                    cefMsg("CLI argument Error",100)
                    print("[!] Commandline syntax error.  Check the log for more details or try '-h'\n\n")
                    raise SystemExit

                toggleStatus(arg,pathToDB,1)
                raise SystemExit

            # Toggle the "inactive" status of a device(s)
            elif opt in ("-i", "--inactive"):
                if len(sys.argv) > 3:
                    log("[!] Too many arguments for the frequency command\n[!] Please provide the path to a single text file containing the relevant devices\n")
                    log("[!] Exiting\n\n")
                    cefMsg("CLI argument Error",100)
                    print("[!] Commandline syntax error.  Check the log for more details or try '-h'\n\n")
                    raise SystemExit

                toggleStatus(arg,pathToDB,2)
                raise SystemExit

            # Populate a fresh database
            elif opt in ("-p", "--populate"):
                if len(sys.argv) > 2:
                    log("[!] Too many arguments for the populate command\n[!] Check your syntax and try again\n[!] Exiting\n\n")
                    cefMsg("CLI argument Error",100)
                    print("[!] Commandline syntax error.  Check the log for more details or try '-h'\n\n")
                    raise SystemExit

                # Make sure the database is empty before continuing
                if os.path.isfile(pathToDB):
                    log("[!] Discovered existing database\n[!] Do not try to populate already populated databases\n[!] Exiting\n\n")
                    cefMsg("DB Error",100)
                    print("\n[!] The program has experienced a fatal error\n[!] Please check the log for details\n[!] Quitting\n\n")
                    raise SystemExit

                # Initialize the database
                dbinit.initDB()

                # Confirm new database presence, connect, and create the cursor
                dbconn = dbMakeConnection(pathToDB)
                dbc = dbMakeCursor(dbconn)

                # Populate 
                dbPopulate(dbc, dbconn, logDirPath)
                raise SystemExit

            # Only check critical systems for fresh logs
            elif opt in ("-C", "--onlyCrits"):
                if len(sys.argv) > 2:
                    log("[!] Too many arguments for onlyCrits command\n[!] Check your syntax and try again\n[!] Exiting\n\n")
                    cefMsg("CLI argument Error",100)
                    print("[!] Commandline syntax error.  Check the log for more details or try '-h'\n\n")
                    raise SystemExit
                else:
                    critsOnly = True

            # Print a full report
            elif opt in ("-r", "--report"):
                if len(sys.argv) > 2:
                    log("[!] Too many arguments for onlyCrits command\n[!] Check your syntax and try again\n[!] Exiting\n\n")
                    cefMsg("CLI argument Error",100)
                    print("[!] Commandline syntax error.  Check the log for more details or try '-h'\n\n")
                    raise SystemExit
                else:
                    report = True

    # Audit the logging structure
    runAudit(critsOnly, report)


######################################################################################################################
if __name__ == "__main__":
    main(sys.argv[1:])


