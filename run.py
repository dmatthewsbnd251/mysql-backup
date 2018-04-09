#!/usr/bin/python

# Ensure user modules are available no matter
# the working directory of the caller
import os
import sys

from optparse import OptionParser

def main():

    parser = OptionParser(usage="usage: %prog [options] filename",
                          version="%prog 1.0")
    parser.add_option("-s", "--settings-file",
                      action="store",
                      dest="settings_file",
                      default=False,
                      help="The settings file to execute.")

    (options, args) = parser.parse_args()

    if not options.settings_file:
        print "Settings file argument is required.  Run with -h to see more information."
        sys.exit(-1)

    config_file = os.path.abspath(options.settings_file)

    # Always run local to the run.py so user modules import properly.
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    from mysql_backup.mysql_backup import MysqlBackup

    mysql_backup_obj = MysqlBackup(config_file)
    mysql_backup_obj.execute()

if __name__ == '__main__':
    main()
