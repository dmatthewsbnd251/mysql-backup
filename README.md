## Requirements

Please see the requirements.txt for the python module dependencies. Also install mysql-devel. If you use
do not change the compress command from pbzip2 to bzip2 (or whatever your compression tool of choice may be), then
also install the pbzip2 package.

## Use case

You have a mysql server/slave setup and backup from the slave.  You only want to back up databases when one changes.
You would also like to save to a short term, cheap storage location more aggressively and a longer term path that
goes to tape.  You would like that tape backup to backup from an lvm snapshot to ensure consistency.  You want lots
of options regarding how many copies and how frequently backups are made and copied to each location.  And parallelism is
nice too.  You want that.  This script takes care of ensuring a snapshot exists.  The backup platform must take care
of removing the snapshot when the backup completes.

## How to use

Configure the settings.ini to your liking.  Then run ./run.py.  If you would like to have different settings
for different databases, make a copy of the settings.ini, ensuring to configure exclude_only and include_only
values appropriately, and modify run.py accordingly.

# Notes
Tested and run on CentOS 7 using the default python installed.  I see no reason it would not work on any Python 2.7
installation though.

