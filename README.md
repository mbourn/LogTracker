# LogTracker
Receive CEF messages when devices stop sending logs to central logging


# Compatibility
Tested and functional in:
 - python3.4
 - python3.5
 - python3.6
 
# Directions
1. Place both files in the same directory
2. Supply the required information in the proper variable located at the top of each file
3. ./logtracker.py -p # This will recursively walk your central logging directory,
                      # find individual devices, determine logging frequencies, populate db
4. ./logtracker.py    # This will finalize the db population
5. Create a cronjob to run logtracker.py, with no arguments, on a regular basis
6. Run ./logtracker.py -h to see all options
