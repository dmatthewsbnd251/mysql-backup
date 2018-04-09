from lockfile import LockFile, NotLocked
import os
import shelve
import mysql_backup.mysql_backup
import psutil
from time import time


class RunningCacheManager:

    backup_logger = None

    def __init__(self, settings_file, cache_lock_wait, running_cache_file, cache_successful_run_purge_days):

        RunningCacheManager.backup_logger = mysql_backup.mysql_backup.MysqlBackup.backup_logger
        self.settings_file = os.path.abspath(settings_file)
        self.running_cache_file = running_cache_file
        """
        self.running_cache_file = {
            successful_run_times = {
                self.settings_file:timestamp,
                .....
            },
            running_backups = {
                self.settings_file:pid,
                ...
            }
        }
        """

        self.cache_lock_wait = cache_lock_wait
        self.cache_successful_run_purge_days = cache_successful_run_purge_days
        self.LockFileObj = LockFile(self.running_cache_file)
        self.cache_shelve_handle = None
        self.sanitize_cache()

    def __str__(self):
        return self.running_cache_file + " cache manager"

    def sanitize_cache(self):
        """
        void()
        The running cache could have dead, crashed backup
        attempts.  This method will look for any pids in the
        running cache that are no longer running and clean
        them from the running cache.

        This step will also remove from the successful backup
        tracking based on self.cache_successful_run_purge_days

        In the off chance that the file can not
        be read by the shelve module it will be re-iniatlized
        here.
        """

        RunningCacheManager.backup_logger.debug("Sanitizing the running cache", extra={'object': self})

        self.should_have_cache_locked(locked=True)
        self.shelve_open(state=True)

        # Verify basic structure of the shelve or reset it
        reset_shelve = False
        for category in ('successful_run_times', 'running_backups'):
            if category not in self.cache_shelve_handle:
                reset_shelve = True
            else:
                if not isinstance(self.cache_shelve_handle[category], dict):
                    reset_shelve = True

        if reset_shelve:
            self.cache_shelve_handle['successful_run_times'] = dict()
            self.cache_shelve_handle['running_backups'] = dict()

        # Make sure all of the pids in the running cache are
        # actually still running
        dead_instances = set()
        for sf in self.cache_shelve_handle['running_backups']:
            pid = self.cache_shelve_handle['running_backups'][sf]
            try:
                # a failed kill 0 means it isn't there
                os.kill(pid, 0)
            except OSError:
                RunningCacheManager.backup_logger.debug("Found an orphaned pid.  Removing it from the running cache.", extra={'object': self})
                dead_instances.add(sf)
            else:
                # the pid is there, but is it actually a python script
                p = psutil.Process(pid)
                if 'python' not in p.cmdline()[0]:
                    RunningCacheManager.backup_logger.debug("Found a running pid but it does not have python in the path. "
                                      "Removing it from the running cache.",
                                      extra={'object': self})
                    dead_instances.add(sf)

        for sf in dead_instances:
            del self.cache_shelve_handle['running_backups'][sf]

        # Finally, remove any pids from the successful cache that
        # exceed how long those should exist.

        now = int(time())
        expired_instances = set()

        for sf in self.cache_shelve_handle['successful_run_times']:
            success_time = self.cache_shelve_handle['successful_run_times'][sf]
            age_in_days = int((now - success_time)/86400)
            if age_in_days > self.cache_successful_run_purge_days:
                expired_instances.add(sf)

        for sf in expired_instances:
            del self.cache_shelve_handle['successful_run_times'][sf]

        self.shelve_open(state=False)
        self.should_have_cache_locked(locked=False)

    def add_current_backup_to_running_cache(self):
        """
        void()
        Attempt to add the current running process to the
        running cache file.
        """
        RunningCacheManager.backup_logger.debug("Adding current pid to running cache", extra={'object': self})
        self.should_have_cache_locked(locked=True)
        self.shelve_open(state=True)
        self.cache_shelve_handle['running_backups'][self.settings_file] = os.getpid()
        self.shelve_open(state=False)
        self.should_have_cache_locked(locked=False)

    def remove_current_backup_from_running_cache(self):
        """
        void()
        Attempt to remove the current running process from
        the running cache file.
        """
        RunningCacheManager.backup_logger.debug("Removing current pid from running cache", extra={'object': self})
        self.should_have_cache_locked(locked=True)
        self.shelve_open(state=True)

        try:
            del self.cache_shelve_handle['running_backups'][self.settings_file]
        except KeyError as e:
            RunningCacheManager.backup_logger.debug("Strange but this instance was already removed from the run "
                                                    "cache manager.", extra={'object': self})

        self.shelve_open(state=False)
        self.should_have_cache_locked(locked=False)

    """
    def __str__(self):

        self.should_have_cache_locked(locked=True)
        self.shelve_open(state=True)

        return_str = str(self.cache_shelve_handle)

        self.shelve_open(state=False)
        self.should_have_cache_locked(locked=False)

        return return_str
    """

    def get_current_running_count(self):
        """
        return: int
        Current number of running backups.
        """
        self.should_have_cache_locked(locked=True)
        self.shelve_open(state=True)

        running_count = len(self.cache_shelve_handle['running_backups'])

        self.shelve_open(state=False)
        self.should_have_cache_locked(locked=False)

        return running_count

    def should_have_cache_locked(self, locked):
        """
        input: bool, True = running cache should be locked
                     False = running cache should not be locked

        """
        if not isinstance(locked, bool):
            msg = "Locked must be a boolean value."
            RunningCacheManager.backup_logger.error(msg, extra={'object': self})
            raise ValueError(msg)

        if locked:
            RunningCacheManager.backup_logger.debug("Exclusively locking the running cache file.", extra={'object': self})
            self.LockFileObj.acquire(timeout=self.cache_lock_wait)
        else:
            RunningCacheManager.backup_logger.debug("Unlocking the running cache file.", extra={'object': self})
            try:
                self.LockFileObj.release()
            except NotLocked:
                # We never care if this is tried and we currently
                # do not have the lock.  It only matters that we
                # reach desired state successfully.
                pass

        # By now an exception would be thrown if the lock was
        # not in an expected state

    def shelve_open(self, state):
        """state (bool)
        sets the shelve open state of cache_shelve_handle
        True = shelve should be open
        False = shelve should not be open"""

        if not isinstance(state, bool):
            msg = "State must be a boolean value."
            RunningCacheManager.backup_logger.error(msg)
            raise ValueError(msg)

        # shelve does not care if handle is already open or closed
        # subsequent attempts will not raise an error if already
        # at the target state.
        if state:
            RunningCacheManager.backup_logger.debug("Opening the cache", extra={'object': self})
            self.cache_shelve_handle = shelve.open(self.running_cache_file, writeback=True)
        else:
            RunningCacheManager.backup_logger.debug("Closing the cache", extra={'object': self})
            self.cache_shelve_handle.close()

    def update_last_successful_runtime(self):
        """
        void()
        Inserts or updates the tracking of the last successful runtime of this
        backup.
        """

        RunningCacheManager.backup_logger.debug("Updating the stored successful runtime of this backup.",
                                                extra={'object': self})

        self.should_have_cache_locked(locked=True)
        self.shelve_open(state=True)

        p = psutil.Process(os.getpid())
        self.cache_shelve_handle['successful_run_times'][self.settings_file] = int(p.create_time())

        self.shelve_open(state=False)
        self.should_have_cache_locked(locked=False)

    def have_already_run_while_others_are_still_running(self):
        """It does not make sense to run a backup again
        when the slave has not been started since the last run.
        Look at the pids of the other running backups in the queue
        and determine if the last successful is younger than they
        are.  If so, return True and it would make sense to
        otherwise not run a backup."""

        self.should_have_cache_locked(locked=True)
        self.shelve_open(state=True)

        already_ran = False

        last_successful_ts = None
        if self.settings_file in self.cache_shelve_handle['successful_run_times']:
            last_successful_ts = self.cache_shelve_handle['successful_run_times'][self.settings_file]

        if last_successful_ts is not None:
            for sf in self.cache_shelve_handle['running_backups']:
                p = psutil.Process(self.cache_shelve_handle['running_backups'][sf])
                start_time = int(p.create_time())
                if start_time < last_successful_ts:
                    already_ran = True
                    break

        self.shelve_open(state=False)
        self.should_have_cache_locked(locked=False)
        return already_ran
