import MySQLdb
import MySQLdb.cursors
from time import sleep
import datetime
from .mysql_backup_file import *
from .mysql_backup_instance import MysqlBackupInstance
from .mysql_db_instance import MysqlDbInstance
import ConfigParser
from lv_snapshot.lv_snapshot import LvSnapshot
import os, time
from joblib import Parallel, delayed
from multiprocessing import cpu_count
from run_cache.run_cache_manager import RunningCacheManager
import logging
import uuid
import psutil


def fork_db(db_instance_obj):
    """Helper function to allow forking"""
    db_instance_obj.execute()


class MysqlBackup:
    """Given a set of path and limiting
    parameters, will dump mysql databases,
    compress, rotate and otherwise manage
    the backup process. Must be done using
    a slave server."""

    mysql_username = None
    mysql_password = None
    mysql_dump_options = None
    mysql_host = None
    compression_enabled = None
    compress_command = None
    decompress_command = None
    compressed_file_extension = None
    max_parallel = None
    cleanup_delay_days = None
    incremental_path = None
    incremental_min_backup_frequency_seconds = None
    incremental_max_lifespan_seconds = None
    incremental_max_copies = None
    long_term_backup_path = None
    long_term_backup_min_frequency_seconds = None
    long_term_backup_max_copies = None
    long_term_max_lifespan_seconds = None
    limits_exclude_databases = None
    limits_include_only_databases = None
    verbose = True
    backup_logger = None

    def __init__(self, settings_file):

        # Read configuration file options
        Config = ConfigParser.SafeConfigParser(allow_no_value=True)
        Config.read(settings_file)

        # Also opening with RawConfig Parser to deal with % characters in
        # passwords.
        raw_config = ConfigParser.RawConfigParser(allow_no_value=True)
        raw_config.read(settings_file)

        MysqlBackup.mysql_username = Config.get("MySQL", "username")
        MysqlBackup.mysql_password = raw_config.get("MySQL", "password")
        MysqlBackup.mysql_dump_options = Config.get("MySQL", "dump_options")
        MysqlBackup.mysql_host = Config.get("MySQL", "host")

        MysqlBackup.compression_enabled = Config.getboolean("Backup", "compression_enabled")
        MysqlBackup.compress_command = Config.get("Backup", "compress_command")
        MysqlBackup.decompress_command = Config.get("Backup", "decompress_command")
        MysqlBackup.compressed_file_extension = Config.get("Backup", "compressed_file_extension")

        MysqlBackup.max_parallel = self.int_or_none(Config.get("Backup", "max_parallel"))

        MysqlBackup.cleanup_delay_days = self.int_or_none(Config.get("Backup", "cleanup_delay_days"))

        MysqlBackup.incremental_path = Config.get("Backup", "incremental_path")
        MysqlBackup.incremental_min_backup_frequency_seconds = \
            self.int_or_none(Config.get("Backup", "incremental_min_backup_frequency_seconds"))
        MysqlBackup.incremental_max_lifespan_seconds = self.int_or_none(Config.get("Backup",
                                                                                   "incremental_max_lifespan_seconds"))
        MysqlBackup.incremental_max_copies = self.int_or_none(Config.get("Backup", "incremental_max_copies"))
        MysqlBackup.long_term_backup_path = Config.get("Backup", "long_term_backup_path")
        MysqlBackup.long_term_backup_min_frequency_seconds = \
            self.int_or_none(Config.get("Backup", "long_term_backup_min_frequency_seconds"))
        MysqlBackup.long_term_max_lifespan_seconds = self.int_or_none(Config.get("Backup",
                                                                                 "long_term_max_lifespan_seconds"))
        MysqlBackup.long_term_backup_max_copies = self.int_or_none(Config.get("Backup", "long_term_backup_max_copies"))

        # Set up logging
        # This can be changed without consequences.
        # Every other object uses getLogger(settings_file).
        # As long as that continues to be true, the logger
        # can be made to do whatever.

        logfile = Config.get("Logging", "logfile")
        loglevel = Config.get("Logging", "loglevel")
        MysqlBackup.settings_file = settings_file

        rand_backup_id = str(uuid.uuid4().get_hex().upper()[0:6])
        MysqlBackup.backup_logger = logging.getLogger("Backup ID:" + rand_backup_id)
        hdlr = logging.FileHandler(logfile)
        formatter = logging.Formatter('[%(name)s][TIME:%(asctime)s][OBJ:%(object)s][LOGLEVEL:%(levelname)s]'
                                      '[METHOD:%(funcName)s][LINE:%(lineno)d]'
                                      '[MSG:%(message)s]')

        hdlr.setFormatter(formatter)
        MysqlBackup.backup_logger.addHandler(hdlr)
        MysqlBackup.backup_logger.setLevel(loglevel)
        # Done setting up logging


        MysqlBackup.limits_exclude_databases = None
        if Config.get("Limits", "exclude_databases"):
            MysqlBackup.limits_exclude_databases = [x.strip() for x in
                                                    Config.get("Limits", "exclude_databases").split(',')]

        MysqlBackup.limits_include_only_databases = None
        if Config.get("Limits", "include_only_databases"):
            MysqlBackup.limits_include_only_databases = [x.strip() for x in
                                             Config.get("Limits", "include_only_databases").split(',')]

        # database connection
        self.db_connection = None
        self.cursor = None
        self.cur_database = None

        # instance locking
        self.running_cache_file = Config.get("Backup", "running_cache_file")
        self.cache_lock_wait = self.int_or_none(Config.get("Backup", "cache_lock_wait"))
        self.cache_successful_run_purge_days = self.int_or_none(Config.get("Backup", "cache_successful_run_purge_days"))

        # snapshot settings
        self.snapshot_name = Config.get("Snapshot", "name")
        self.snapshot_vg = Config.get("Snapshot", "vg")
        self.snapshot_lv = Config.get("Snapshot", "lv")
        self.snapshot_size_gb = self.int_or_none(Config.get("Snapshot", "size_gb"))

        # all of the db backup instances
        self.mysql_db_backup_instances = None

        # running time
        self.starting_time = None
        self.run_cache_manager = RunningCacheManager(settings_file, self.cache_lock_wait, self.running_cache_file,
                                                     self.cache_successful_run_purge_days)

    def __str__(self):
        return "mysql_backup.py"

    def execute(self):
        """Attempt to run new backups, rotate, and age according to configuration.  This is the only method
        That needs called after initialization."""
        # Prep
        self.log_running_time(logtype='begin')

        if self.run_cache_manager.have_already_run_while_others_are_still_running():
            MysqlBackup.backup_logger.info("A backup using this settings file has already run while another is still running. "
                             "Because no changes would be expected while that is true, there is no reason to proceed.",
                             extra={'object': self})
        else:

            # Let other instances know this backup has started
            self.run_cache_manager.add_current_backup_to_running_cache()

            # A little pre-cleanup possible here
            self.mysql_db_backup_instances = self.get_db_backup_instances_from_files()

            # More prep
            self.slave_should_be_running(False)

            # Do work
            self.clean_non_backup_files()
            self.process_databases()

            # Cleanup
            if self.run_cache_manager.get_current_running_count() == 1:
                MysqlBackup.backup_logger.info("This is the only backup running.  Starting the mysql slave back up and refreshing the"
                                 " snapshot if possible.", extra={'object': self})
                # this backup is still in the run queue.  Unless this
                # is the only backup, the slave should not be started
                # nore should the snapshot be refreshed.
                self.slave_should_be_running(True)
                self.ensure_snapshot_exists_and_refresh_if_possible()
            else:
                MysqlBackup.backup_logger.info("This does not appear to be the only running backup. To be safe the slave will not be "
                                 "started nor will the snapshot be refreshed at this time.", extra={'object': self})

            self.run_cache_manager.update_last_successful_runtime()
            self.run_cache_manager.remove_current_backup_from_running_cache()

        self.log_running_time(logtype='end')

    def process_databases(self):
        """(void)
        request only databases that should process
        per configuration and database objects
        marked as invalid for cleanup purposes."""

        self.set_valid_database_flags()

        dbs_to_process_per_configuration = self.get_databases_to_attempt_backups()

        db_object_processing_queue = list()

        for db in self.get_databases():

            dbobj = self.get_db_instance_by_name(db)

            if db in dbs_to_process_per_configuration or (dbobj is not None and not dbobj.is_valid()):
                MysqlBackup.backup_logger.debug("Executing %s per configuration or because marked as an invalid database instance"
                                  % db, extra={'object': self})

                if dbobj is None:
                    MysqlBackup.backup_logger.debug("No existing backups found for %s. Initializing before execution." % db,
                                      extra={'object': self})
                    dbobj = MysqlDbInstance(db_name=db, valid=True)

                    MysqlBackup.backup_logger.info("Adding to the processing queue %s.." % dbobj, extra={'object': self})

                db_object_processing_queue.append(dbobj)

            else:
                MysqlBackup.backup_logger.debug("Not executing %s per configuration" % db, extra={'object': self})

        proc_count = cpu_count()

        if mysql_backup.MysqlBackup.max_parallel not in (None, 0):
            proc_count = mysql_backup.MysqlBackup.max_parallel

            MysqlBackup.backup_logger.info("Start multiprocessing backups.  Max parallel set to %s"
                         % str(mysql_backup.MysqlBackup.max_parallel), extra={'object': self})

        Parallel(n_jobs=proc_count)(map(delayed(fork_db), db_object_processing_queue))

    def set_valid_database_flags(self):
        """(void)
        If a database object exists as an actual database, it is considered valid"""
        db_names = self.get_databases()
        for dbobj in self.mysql_db_backup_instances:
            if dbobj.db_name in db_names:
                MysqlBackup.backup_logger.debug("Setting %s as a valid database" % dbobj.db_name,
                                                extra={'object': self})
                dbobj.set_valid(valid=True)
            else:
                MysqlBackup.backup_logger.debug("Setting %s as a invalid database" % dbobj.db_name,
                                                extra={'object': self})
                dbobj.set_valid(valid=False)

    def get_db_backup_instances_from_files(self):
        """Scan through the incremental path and attempt
        to initialize each file as a mysql backup file, passing
        each to the MysqlBackupFileFactory and throwing and
        catching any exceptions.

        return: list of db instance objects"""
        db_instance_file_objects = list()
        for myfile in self.get_files_in_incremental_path():
            try:
                db_obj = MysqlBackupFileFactory.get_file_object(myfile)
                db_instance_file_objects.append(db_obj)
            except AssertionError as e:
                MysqlBackup.backup_logger.debug("%s is not a valid mysql backup file" % myfile, extra={'object': self})
                MysqlBackup.backup_logger.debug("Excpetion was %s" % e, extra={'object': self})
            else:
                MysqlBackup.backup_logger.debug("%s is a valid mysql backup file" % myfile, extra={'object': self})

        db_backup_instance_dict = dict()
        for fo in db_instance_file_objects:
            if fo.db_name not in db_backup_instance_dict:
                # initialize a dictionary holding dates and later a db instance object
                db_backup_instance_dict[fo.db_name] = {
                    'date_strings':dict(),
                    'db backup instance': None
                }
            if fo.date_string not in db_backup_instance_dict[fo.db_name]['date_strings']:
                # initialize the date dictionary with a holder for a instances files
                # and later backup instances
                db_backup_instance_dict[fo.db_name]['date_strings'][fo.date_string]={
                    'db backup file objects': [fo, ],
                    'backup instance': None
                }
            else:
                db_backup_instance_dict[fo.db_name]['date_strings'][fo.date_string]['db backup file objects'].append(fo)

        # loop and build the backup instances
        for db in db_backup_instance_dict:
            failed_instances = set()
            for date_string in db_backup_instance_dict[db]['date_strings']:
                try:
                    db_backup_instance_dict[db]['date_strings'][date_string]['backup instance'] = \
                        MysqlBackupInstance(db_name=db, date_string=date_string, bkup_file_objs=
                                            tuple(db_backup_instance_dict[db]['date_strings'][date_string]
                                                  ['db backup file objects']))

                    MysqlBackup.backup_logger.debug("%s initialized" %
                                      db_backup_instance_dict[db]['date_strings'][date_string]['backup instance'],
                                      extra={'object': self})

                except RuntimeError as e:
                    MysqlBackup.backup_logger.warning("Caught failure when initializing backup instance. Cleaning up "
                                                      "db: %s and backup instance date: %s" % (db, date_string),
                                                      extra={'object': self})
                    failed_instances.add(date_string)
                else:
                    MysqlBackup.backup_logger.debug("%s %s is a valid backup instance" % (db, date_string),
                                                    extra={'object': self})

            # Cleanup failed initializations
            for failed_instance in failed_instances:
                del db_backup_instance_dict[db]['date_strings'][failed_instance]

        # Cleanup initialized database entries that only had failed initializations
        empty_databases = set()
        for d in db_backup_instance_dict:
            if len(db_backup_instance_dict[d]['date_strings']) == 0:
                empty_databases.add(d)
                MysqlBackup.backup_logger.debug("%s only had failed or impartial instance, removing from the backup "
                                                "candidates to initialize." % (d,), extra={'object': self})
        for d in empty_databases:
            del db_backup_instance_dict[d]

        # build the db instances
        for db in db_backup_instance_dict:
            backup_instances = list()
            for date_string in db_backup_instance_dict[db]['date_strings']:
                backup_instances.append(db_backup_instance_dict[db]['date_strings'][date_string]['backup instance'])
            db_backup_instance_dict[db]['db backup instance'] = \
                MysqlDbInstance(db_name=db, mysql_backup_instances=tuple(backup_instances))

        # and finally, throw most of the hard work away.
        # all this method really does is return a list of
        # of db instances, so do that now
        db_backup_instances = list()
        for db in db_backup_instance_dict:
            db_backup_instances.append(db_backup_instance_dict[db]['db backup instance'])

        return db_backup_instances

    def clean_non_backup_files(self):
        """(void)
        If a file exists in a backup managed path
        and does not look like a backup file, it has no
        business being there, but wait cleanup_delay_days
        before removing it."""
        non_backup_files = set(self.get_all_files()) - set(self.get_all_db_files())
        for myfile in non_backup_files:
            if not MysqlBackup.is_file_open(myfile):
                MysqlBackup.backup_logger.info("%s does not appear to be a backup file"
                                                % myfile, extra={'object': self})

                file_age_days = int((time.time() - os.stat(myfile).st_mtime) / 86400.0)
                if file_age_days > MysqlBackup.cleanup_delay_days:
                    MysqlBackup.backup_logger.info("%s is older, %d days, than cleanup_delay_days, %d, removing."
                                                   % (myfile, file_age_days, MysqlBackup.cleanup_delay_days),
                                                   extra={'object': self})
                    os.remove(myfile)
                else:
                    MysqlBackup.backup_logger.info("%s is not older, %d days, than cleanup_delay_days, %d, not "
                                                   "removing." % (myfile, file_age_days,
                                                                  MysqlBackup.cleanup_delay_days),
                                                   extra={'object': self})
            else:
                MysqlBackup.backup_logger.debug("%s is open.  Not removing it." % myfile, extra={'object': self})

    def get_all_db_files(self):
        filelist = list()
        for dbinst in self.mysql_db_backup_instances:
            filelist += dbinst.get_all_files()
        return filelist

    def get_all_files(self):
        return self.get_files_in_incremental_path() + self.get_files_in_long_term_path()

    def get_files_in_incremental_path(self):
        incremental_files = [mysql_backup.MysqlBackup.incremental_path.rstrip('/') + '/' + f for f in os.listdir(mysql_backup.MysqlBackup.incremental_path)
                             if os.path.isfile(os.path.join(mysql_backup.MysqlBackup.incremental_path, f))]
        return incremental_files

    def get_files_in_long_term_path(self):
        incremental_files = [mysql_backup.MysqlBackup.long_term_backup_path.rstrip('/') + '/' + f for f in os.listdir(mysql_backup.MysqlBackup.long_term_backup_path)
                             if os.path.isfile(os.path.join(mysql_backup.MysqlBackup.long_term_backup_path, f))]
        return incremental_files

    def get_db_instance_by_name(self, name):
        for db in self.mysql_db_backup_instances:
            if db.db_name == name:
                return db
        return None

    @staticmethod
    def get_file_age(age_format, file_name):
        """format: [days|seconds]"""
        cur_ts = int(time.time())
        epoc_mod_time = int(os.stat(file_name).st_mtime)

        age_in_secs = cur_ts - epoc_mod_time

        if age_format == 'seconds':
            return age_in_secs
        elif age_format == 'days':
            return int(age_in_secs/86400)
        else:
            ValueError("get_file_age requires the string 'days' or 'seconds' as the argument to age_format.")

    @staticmethod
    def human_readable_date_from_tt(tt):
        """purpose: singular place for date stuff to be consistent
        input: time tuple (time.localtime() as an example)
        output: date string that looks nicer for files."""
        return time.strftime("%Y%m%d-%H%M%S", tt)

    @staticmethod
    def ts_from_human_readable_date(date_time_string):
        """input: date string in the format produced by human_readable_date_from_ts
        output: seconds since the unix epoch as an integer"""

        frmt = "%Y%m%d-%H%M%S"
        dt_obj = datetime.datetime.strptime(date_time_string, '%Y%m%d-%H%M%S')
        dtst = dt_obj.strftime(frmt)  # convert datetime object to string
        time_struct = time.strptime(dtst, frmt)  # convert time (yyyy-mm-dd hh:mm:ss) to time tuple
        return int(time.mktime(time_struct))

    @staticmethod
    def is_file_open(file_name):

        open_files = set()
        for p in psutil.process_iter():
            process = psutil.Process(p.pid)
            for f in process.open_files():
                open_files.add(f.path)

        return file_name in open_files

    def connect_if_not_connected(self, database):
        """
        If a database connection to the requested database does not exist it will be created.
        """
        if self.cur_database != database:

            if self.db_connection is not None:
                self.db_connection.close()

            if self.cursor is not None:
                self.cursor.close()

            self.db_connection = MySQLdb.connect(MysqlBackup.mysql_host, MysqlBackup.mysql_username,
                                                 MysqlBackup.mysql_password, database,
                                                 cursorclass=MySQLdb.cursors.DictCursor)
            self.cursor = self.db_connection.cursor()
            self.cur_database = database

    def get_databases(self):
        '''
        return a list of databases names
        '''
        dblist = list()
        self.connect_if_not_connected('mysql')
        self.cursor.execute("SHOW DATABASES;")
        result = self.cursor.fetchall()
        for row in result:
            dblist.append(row['Database'])
        return dblist

    def get_databases_to_attempt_backups(self):
        """
        return: list of databases

        Not all databases should be attempted.  Based on
        a number of criteria only certain ones should be attempted.
        However, this still does not dictate whether or not a
        backup would be saved.  Only when a backup completes and
        matches an md5 are we really sure it should be saved or not."""

        db_config_filtered = list()
        for db in self.get_databases():
            if MysqlBackup.limits_include_only_databases:
                if db in MysqlBackup.limits_include_only_databases:
                    db_config_filtered.append(db)
            elif MysqlBackup.limits_exclude_databases:
                if db not in MysqlBackup.limits_exclude_databases:
                    db_config_filtered.append(db)
            else:
                db_config_filtered.append(db)

        if MysqlBackup.verbose:
            MysqlBackup.backup_logger.info("Based on the exclude_databases and include_only_databases directives, the potential "
                             "database backup candidates so far are as follows:\n%s" % (','.join(db_config_filtered)),
                             extra={'object': self})

        return db_config_filtered

    def is_slave_running(self):
        """True = slave is running
        False = slave is not running"""
        self.connect_if_not_connected("mysql")
        self.cursor.execute("SHOW SLAVE STATUS;")
        result = self.cursor.fetchall()
        if result[0]["Slave_IO_Running"] == "Yes" and result[0]["Slave_SQL_Running"] == "Yes":
            return True
        else:
            return False

    def slave_should_be_running(self, running_state):
        """
        param: running_state (bool)
        True = set the slave to running if not already running
        False = set the slave to stopped if not already stopped
        """

        # wait for the slave to catch up before failing
        seconds_between_tries = 5
        retries = 20
        i = 0

        if running_state and not self.is_slave_running():
            self.connect_if_not_connected("mysql")
            MysqlBackup.backup_logger.info("Starting mysql slave.", extra={'object': self})
            self.cursor.execute("START SLAVE;")

            while i < retries and not self.is_slave_running():
                i += 1
                sleep(seconds_between_tries)

            if not self.is_slave_running():
                msg = "MySQL Slave Failed to start"
                MysqlBackup.backup_logger.error(msg, extra={'object': self})
                raise SystemError(msg)

        elif not running_state and self.is_slave_running():

            self.connect_if_not_connected("mysql")
            MysqlBackup.backup_logger.info("Stopping slave.", extra={'object': self})
            self.cursor.execute("STOP SLAVE;")

            while i < retries and self.is_slave_running():
                i += 1
                sleep(seconds_between_tries)

            if self.is_slave_running():
                msg = "MySQL Slave Failed to stop"
                MysqlBackup.backup_logger.error(msg, extra={'object': self})
                raise SystemError(msg)

    def get_database_file_objects(self):
        """Iterate over the backup file directories
        and attempt to return backup file objects.
        These can be later used when instantiating
        a database instance object.
        returns a generator but will also destroy
        failed attempts.

        Failed attempts look like bad combinations, lets
        say for example a sql file that doesn't have an md5.

        However, backup files are only removed if they meet
        the age criteria."""

        for path in (MysqlBackup.incremental_path, MysqlBackup.long_term_backup_path):
            file_full_paths = [os.path.join(path, fn) for fn in next(os.walk(path))[2]]
            for file_full_path in file_full_paths:
                try:
                    db_file_obj = MysqlBackupFileFactory.get_file_object(file_full_path)
                    yield db_file_obj
                except AssertionError as e:

                    MysqlBackup.backup_logger.debug("%s failed to initialized as a backup file. Only backup files "
                                                    "should exist in backup directories. Based on preservation time, "
                                                    "considering deletion."
                                                    % (file_full_path,), extra={'object': self})

                    MysqlBackup.backup_logger.debug("Assertion was %s" % e)

                    if path == MysqlBackup.incremental_path and \
                            MysqlBackup.get_file_age(file_name=file_full_path, age_format='days') > \
                            MysqlBackup.cleanup_delay_days:
                            MysqlBackup.backup_logger.debug("Criteria met.  Deleting.", extra={'object': self})

                            os.remove(file_full_path)

                    elif path == MysqlBackup.long_term_backup_path and \
                            MysqlBackup.get_file_age(file_name=file_full_path, age_format='days') > \
                            MysqlBackup.cleanup_delay_days:

                            MysqlBackup.backup_logger.debug("Criteria met.  Deleting.")

                            os.remove(file_full_path)
                    else:
                        MysqlBackup.backup_logger.debug("Criteria not met.  Leaving along for now.", extra={'object': self})

    def int_or_none(self, config_value):
        """For some odd reason allow_no_value does not
        work the way it should for getint when using a
        ConfigParser object.  This function let's us use
        get and cast as an int when not None"""
        if config_value is None:
            return None
        else:
            return int(config_value)

    def ensure_snapshot_exists_and_refresh_if_possible(self):
        snapshot_obj = LvSnapshot(vg=self.snapshot_vg, lv=self.snapshot_lv, snapshot_name=self.snapshot_name,
                                  size_gb=self.snapshot_size_gb)
        snapshot_obj.safe_refresh_snapshot()

    def log_running_time(self, logtype):
        """Input: logtype=[begin|end]
        If verbosity is enabled, print start, end, and running times"""

        if logtype == 'begin':
            self.starting_time = datetime.datetime.now()
            MysqlBackup.backup_logger.info("Backup start time: %s" % (self.starting_time,), extra={'object': self})

        elif logtype == 'end':
            if self.starting_time is None:
                msg = "log_running_time requested and end time but start was never run first."
                MysqlBackup.backup_logger.error(msg, extra={'object': self})
                raise ValueError(msg)
            end = datetime.datetime.now()
            MysqlBackup.backup_logger.info("Backup end time: %s" % (end,), extra={'object': self})
            tdelta = end - self.starting_time
            MysqlBackup.backup_logger.info("Backup runtime: %s" % (tdelta,), extra={'object': self})

        else:
            msg = "logtype must be either begin or end"
            MysqlBackup.backup_logger.error(msg, extra={'object': self})
            raise ValueError(msg)




