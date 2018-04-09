# A MySQL DB Instance Object is responsible for managing
# MysqlBackupInstance objects. Primary areas of concern here
# are meeting configured criteria in the settings by adding
# and removing instances.

import mysql_backup
from mysql_backup_instance import MysqlBackupInstance
from operator import methodcaller


class MysqlDbInstance:

    backup_logger = None

    def __init__(self, db_name, mysql_backup_instances=(), valid=None):

        MysqlDbInstance.backup_logger = mysql_backup.mysql_backup.MysqlBackup.backup_logger

        self.mysql_backup_instances = list(mysql_backup_instances)
        self.db_name = db_name

        # Is the database valid (ie. The underlying database exists)
        self.valid = valid

    def __str__(self):
        return self.db_name

    def execute(self):
        """After calling set_valid, running execute will do the right thing."""
        if not isinstance(self.valid, bool):
            msg = "Before executing the database must be marked valid or invalid."
            MysqlBackupInstance.backup_logger.error(msg, extra={'object': self})
            raise AssertionError(msg)

        if self.is_valid():
            # What we should be doing when a database exists
            self.add_new_instance_if_criteria_is_met()
            self.set_correct_state()
        else:
            # What we should be doing when the database no longer exists but some files were left hanging around.
            if mysql_backup.MysqlBackup.cleanup_delay_days is not None:
                age_in_days = self.get_age_secs() * 86400
                if age_in_days > mysql_backup.MysqlBackup.cleanup_delay_days:
                    msg = "This databse is not valid and exceeds the configured amount of time to preserve. Removing."
                    MysqlBackupInstance.backup_logger.debug(msg, extra={'object': self})
                    self.self_destruct()
                else:
                    msg = "This databse is not valid but does not exceed the configured amount of time to preserve. " \
                          "not removing the leftover backup files for now."
                    MysqlBackupInstance.backup_logger.debug(msg, extra={'object': self})

    def is_valid(self):
        return self.valid

    def set_valid(self, valid):
        """Does this database actually exist?"""
        if not isinstance(valid, bool):
            msg = "valid parameter must be of type boolean."
            MysqlBackupInstance.backup_logger.error(msg, extra={'object': self})
            raise ValueError(msg)
        self.valid = valid

    def get_youngest_instance(self):
        """Returns the youngest backup instance or None"""
        my_youngest_instance = None
        for mbi in self.mysql_backup_instances:
            if my_youngest_instance is None or (mbi.get_age_secs() < my_youngest_instance.get_age_secs()):
                my_youngest_instance = mbi
        return my_youngest_instance

    def get_youngest_long_term_backup(self):
        my_youngest_long_term_backup = None
        for mbi in self.mysql_backup_instances:
            if mbi.is_a_long_term_version() and (my_youngest_long_term_backup is None or (mbi.get_age_secs() < my_youngest_long_term_backup.get_age_secs()) ):
                my_youngest_long_term_backup = mbi
        return my_youngest_long_term_backup

    def get_oldest_instance(self):
        """Returns the oldest backup instance or None"""
        oldest_instance = None
        for mbi in self.mysql_backup_instances:
            if oldest_instance is None or mbi.get_age_secs() < oldest_instance:
                oldest_instance = mbi
        return oldest_instance

    def get_instance_count(self):
        """Return: int, how many instances are there."""
        return len(self.mysql_backup_instances)

    def get_instances_from_oldest_to_youngest(self):
        """Return a list of the instances sorted by
        age, oldest to youngest"""
        return sorted(self.mysql_backup_instances, key=methodcaller('get_age_secs'), reverse=True)

    def get_instances_from_youngest_to_oldest(self):
        """Return a list of instances sorted by
        age, youngest to oldest"""
        return sorted(self.mysql_backup_instances, key=methodcaller('get_age_secs'), reverse=False)

    def set_correct_state(self):
        self.set_correct_short_term_state()
        self.set_correct_long_term_state()

    def get_current_long_term_count(self):
        lt_count = 0
        for instance in self.mysql_backup_instances:
            if instance.is_a_long_term_version():
                lt_count += 1
        return lt_count

    def get_most_recent_lt_age(self):
        for instance in self.get_instances_from_youngest_to_oldest():
            if instance.is_a_long_term_version():
                return instance.get_age_secs()
        return None

    def set_correct_long_term_state(self):
        """This does not likely need called directly.  Use set_correct_state.
        This will check criteria and set the correct long term backup status of a
        database."""

        if self.get_youngest_instance() is None:
            # No backups available, just return.
            return None

        # Are long term backups even allowed
        if mysql_backup.MysqlBackup.long_term_backup_max_copies is None or \
           mysql_backup.MysqlBackup.long_term_backup_max_copies != 0:

            # Okay to shift the stack?
            make_youngest_new_lt = False
            # First off, is the current youngest already the long term?
            if not self.get_youngest_instance().is_a_long_term_version():
                # Has enough time passed between the last long term and this one?
                if self.get_most_recent_lt_age() is None or \
                   (mysql_backup.MysqlBackup.long_term_backup_min_frequency_seconds <
                   (self.get_most_recent_lt_age() - self.get_youngest_instance().get_age_secs())):
                    make_youngest_new_lt = True
                    MysqlDbInstance.backup_logger.info("%s: Enough time has passed between the last long term copy and "
                                                       "this one." % self, extra={'object': self})
                else:
                    make_youngest_new_lt = False
                    MysqlDbInstance.backup_logger.info("Not enough time has passed between the last long term copy and "
                                                       "this one. Per configuration, the latest version should not be "
                                                       "made a long term version.", extra={'object': self})
            else:
                make_youngest_new_lt = True
                MysqlDbInstance.backup_logger.info("%s: The newest backup is already the long term backup version."
                                                   % self, extra={'object': self})

            if make_youngest_new_lt:
                self.get_youngest_instance().set_as_long_term_version(lt_state=make_youngest_new_lt)
                MysqlDbInstance.backup_logger.info("%s: Created or ensured this instance was a long term backup copy."
                                                   % self, extra={'object': self})
            else:
                MysqlDbInstance.backup_logger.info("%s: Not creating a new long term backup copy" % self,
                                                   extra={'object': self})

            # Now to enforcing all requirements
            lt_instance_count = 0
            last_instance_age = None
            # Loop and look for a reason not to keep the lt_backup
            for instance in self.get_instances_from_youngest_to_oldest():
                if instance.is_a_long_term_version():
                    keep_instance = True

                    # Too many
                    if mysql_backup.MysqlBackup.long_term_backup_max_copies is not None and lt_instance_count >= \
                            mysql_backup.MysqlBackup.long_term_backup_max_copies:

                        MysqlDbInstance.backup_logger.info("%s: %s exceeds the max number of long term backups per "
                                                           "configuration." % (self, instance), extra={'object': self})
                        keep_instance = False

                    # Not enough time between
                    if last_instance_age is not None and \
                       mysql_backup.MysqlBackup.long_term_backup_min_frequency_seconds is not None:

                        age_between_instances = instance.get_age_secs() - last_instance_age
                        if age_between_instances < mysql_backup.MysqlBackup.long_term_backup_min_frequency_seconds:
                            keep_instance = False

                            MysqlDbInstance.backup_logger.info("%s: %s is to close in age from the last long term copy."
                                                               % (self, instance), extra={'object': self})

                    # Too old
                    if mysql_backup.MysqlBackup.long_term_max_lifespan_seconds is not None and \
                       instance.get_age_secs() > mysql_backup.MysqlBackup.long_term_max_lifespan_seconds:

                        keep_instance = False

                        MysqlDbInstance.backup_logger.info("%s: %s is too old" % (self, instance),
                                                           extra={'object': self})

                    if keep_instance:
                        MysqlDbInstance.backup_logger.info("%s: %s meets the criteria to preserve.  Keeping this "
                                                           "backup." % (self, instance), extra={'object': self})
                        instance.set_as_long_term_version(lt_state=True)
                        lt_instance_count += 1
                    else:
                        MysqlDbInstance.backup_logger.info("%s: %s does not meet the criteria to preserve.  Removing "
                                                           "this backup." % (self,instance), extra={'object': self})
                        instance.set_as_long_term_version(lt_state=False)

        else:
            MysqlDbInstance.backup_logger.info("%s: Max backup copies is set to 0.  All should be removed."
                                               % (self, ), extra={'object': self})
            for instance in self.get_instances_from_youngest_to_oldest():
                instance.set_as_long_term_version(lt_state=False)

    def set_correct_short_term_state(self):
        """This does not likely need called directly.  Use set_correct_state.
        This will check criteria and set the correct short term backup status of a
        database."""

        if self.get_youngest_instance() is None:
            # No backups exist, just return
            MysqlDbInstance.backup_logger.info("%s: No short term backups exist.  Nothing to do when setting the "
                                               "correct short term state."
                                               % (self,), extra={'object': self})
            return None

        st_counter = 0
        for instance in self.get_instances_from_youngest_to_oldest():
            previous_instances_age = None
            destroy_this = False
            # look for a reason to destroy this

            # More copies than allowed?
            if mysql_backup.MysqlBackup.incremental_max_copies is not None and \
                            st_counter >= mysql_backup.MysqlBackup.incremental_max_copies:
                MysqlDbInstance.backup_logger.info("%s: %s will be removed.  It would be more instances than allowed by"
                                                   " configuration." % (self, instance,), extra={'object': self})
                destroy_this = True

            # Too old?
            if mysql_backup.MysqlBackup.incremental_max_lifespan_seconds is not None and \
                            instance.get_age_secs() > mysql_backup.MysqlBackup.incremental_max_lifespan_seconds:
                MysqlDbInstance.backup_logger.info("%s: %s will be removed.  It is older than allowed by configuration."
                                                   % (self, instance,), extra={'object': self})
                destroy_this = True

            # Too soon?
            if None not in (previous_instances_age, mysql_backup.MysqlBackup.incremental_min_backup_frequency_seconds) \
                    and (instance.get_age_secs() - previous_instances_age) < \
                    mysql_backup.MysqlBackup.incremental_min_backup_frequency_seconds:
                MysqlDbInstance.backup_logger.info("%s: %s will be removed.  It is too close in age than the previous "
                                                   "backup by configuration."% (self, instance,),
                                                   extra={'object': self})
                destroy_this = True

            if not destroy_this:
                MysqlDbInstance.backup_logger.info("%s: %s will be preserved.  Configuration criteria has been met." %
                                                   (self, instance,), extra={'object': self})
                st_counter += 1
                previous_instances_age = instance.get_age_secs()
            else:
                MysqlDbInstance.backup_logger.info("%s: Deleting %s." % (self, instance,), extra={'object': self})
                self.delete_instance(instance)

    def delete_instance(self, instance):
        """Request the instance file delete associated files.
        Remove the instance from the managed instances list"""
        self.mysql_backup_instances = [bkinst for bkinst in self.mysql_backup_instances if bkinst is not instance]
        instance.self_destruct()

    def is_criteria_for_an_attempt_met(self):

        if self.get_youngest_instance() is None:
            return True

        if mysql_backup.MysqlBackup.incremental_min_backup_frequency_seconds is not None:
            if self.get_youngest_instance().get_age_secs() > \
                    mysql_backup.MysqlBackup.incremental_min_backup_frequency_seconds:
                return True
            else:
                MysqlDbInstance.backup_logger.info("%s: Minimum backup frequency requirement for incrementals was not "
                                                   "met." % (self, ), extra={'object': self})
        else:
            return True

        return False

    def initialize_a_new_instance(self):
        """Return: New mysql_backup instance
        that has not yet set_proper_instance state.
        This is useful to be able to inspect the
        checksum before requesting compression."""
        MysqlDbInstance.backup_logger.debug("%s: Requesting initialization of a new backup instance."
                                            % (self,), extra={'object': self})
        return MysqlBackupInstance(self.db_name)

    def add_new_instance_if_criteria_is_met(self):
        if self.is_criteria_for_an_attempt_met():
            newinst = self.initialize_a_new_instance()
            youngest_instance = self.get_youngest_instance()
            if youngest_instance is not None:
                if youngest_instance != newinst:
                    MysqlDbInstance.backup_logger.info("%s: Most recent incremental has a different checksum. "
                                                       "Preserving this instance." % (self, ), extra={'object': self})
                    newinst.set_proper_instance_state()
                    self.mysql_backup_instances.append(newinst)
                else:
                    MysqlDbInstance.backup_logger.info("%s: The previous backup and this one have matching checksums. "
                                                       "No reason to keep this backup.  Destroying it." % (self, ),
                                                       extra={'object': self})
                    self.delete_instance(newinst)
            else:
                MysqlDbInstance.backup_logger.info("%s: No previous backups exists.  Assuming this should be preserved."
                                                   % (self, ), extra={'object': self})
                newinst.set_proper_instance_state()
                self.mysql_backup_instances.append(newinst)

    def get_all_files(self):
        all_files = set()
        for instance in self.mysql_backup_instances:
            for file in instance.get_all_files():
                all_files.add(file)
        return list(all_files)

    def get_age_secs(self):
        """Return: int or None
        For a database instance, age is defined
        by the age of the youngest backup."""
        if self.get_youngest_instance() is not None:
            return self.get_youngest_instance().get_age_secs()
        else:
            return None

    def self_destruct(self):
        MysqlDbInstance.backup_logger.info("%s: Self destruct requested." % (self,), extra={'object': self})
        instances = self.mysql_backup_instances[:]
        for instance in instances:
            self.delete_instance(instance)
