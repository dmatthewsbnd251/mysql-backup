# A MySQL Backup Instance Object
# is responsible for managing the
# mysql backup files associated with it.
# It also functions a place to abstract
# the creation of new backups.

import mysql_backup
import time


class MysqlBackupInstance:

    backup_logger = None

    def __init__(self, db_name, date_string=None, bkup_file_objs=()):
        """There are two methods to initialize.
        1: Pass only db_name = trigger a new backup to be created and become an instance.
        (Still verify the crap out of the new instance)
        2: Pass a tuple of backup file objects to bkup_file_objs = initialize an instance
        of an existing backup, making every effort to make sure things are valid or
        self destructing (removing all files) with a RuntimeError"""

        MysqlBackupInstance.backup_logger = mysql_backup.mysql_backup.MysqlBackup.backup_logger

        self.db_name = db_name
        self.date_string = date_string

        # These are things managed within the instance itself
        self.bkup_file_objs = list(bkup_file_objs)

        # The following two values will always be initialized after the
        # call to set_proper_instance_state
        self.checksum = None
        self.incremental_backup_file_obj = None

        # A convenient way to be sure proper instance has been attempted
        # at least once, which should almost always be sufficient
        self.set_proper_instance_state_called_at_least_once = False

        if bkup_file_objs and date_string is not None:
            if self.any_files_being_written():
                msg = "Files are being written.  Can not instantiate %s" % self
                # MysqlBackupInstance.backup_logger(msg, extra={'object': self})
                raise RuntimeError(msg)
            else:
                self.set_proper_instance_state()

        elif not bkup_file_objs and date_string is None:
            # Create a new backup
            results = mysql_backup.MysqlBackupFileFactory.create_file_object(self.db_name)
            self.bkup_file_objs = results.values()
            self.date_string = results.values()[0].date_string
            validated_instance_file_objects = self.clean_bad_files_return_good_file_objects_or_fail()
            self.checksum = validated_instance_file_objects.get("checksumfileobj").get_checksum()

        else:
            msg = "Improper combination of arguments."
            # MysqlBackupInstance.backup_logger(msg, extra={'object': self})
            raise ValueError(msg)

    def __eq__(self, other):
        """If the checksums of two instances are equal
        the backups are equal."""
        if not isinstance(other, MysqlBackupInstance):
            AssertionError("Invalid comparison attempted.")
        return self.checksum == other.checksum

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return self.db_name + " " + self.date_string

    # Get stuff

    def get_age_secs(self):
        # current time stamp
        now = int(time.time())

        #time stamp based on the backup files naming
        backup_time = mysql_backup.MysqlBackup.ts_from_human_readable_date(self.date_string)

        age_in_secs = now - backup_time

        return age_in_secs

    def get_all_files(self):
        """Return a list of all files (full paths) associated
        with this backup instance"""
        return_list = list()
        for obj in self.bkup_file_objs:
            return_list.append(obj.file_name_full_path)

        if self.is_a_long_term_version():
            return_list.append(self.incremental_backup_file_obj.get_long_term_backup_full_name())
        return return_list

    def is_a_long_term_version(self):
        """Does this backup instance exist in the long
        term backup path"""
        return self.incremental_backup_file_obj.is_a_long_term_version()

    # Validate or (do stuff (with a RuntimeError) and die trying)

    def set_proper_instance_state(self):
        """void (but throws RunTiimeException on error) either here or in a called method.
        Backups could die mid run or compression.
        In any case this method will make an effort to
        resolve situations that should not exist and
        ensure backup instances exist in a proper state.
        If this is not possible, CLEAN EVERYTHING UP, SELF DESTRUCT,
        and throw a RunTimeError"""

        validated_instance_file_objects = self.clean_bad_files_return_good_file_objects_or_fail()

        self.checksum = validated_instance_file_objects.get("checksumfileobj").get_checksum()
        self.incremental_backup_file_obj = validated_instance_file_objects.get("bkupfileobj")
        self.set_compression_state()

        self.set_proper_instance_state_called_at_least_once = True

    def self_destruct(self):
        """Delete all files associated with this instance"""
        for bkfobj in self.bkup_file_objs:
                bkfobj.self_destruct()

    def clean_bad_files_return_good_file_objects_or_fail(self):
        """Part of the initialization phase of an existing backup
        and should be called when a new backup will be kept
        ie. the checksums are different than the last one.

        If this does not fail, it will
        return a dict of

        {
            'checksumfileobj':CheckSumFile,
            'bkupfileobj':ActualBackupFileObj (UncompressedFile or CompressedFile)
        }"""

        has_checksum_file = False
        checksum_file_has_content = False
        has_uncompressed_file = False
        has_compresssed_file = False
        # less obvious but noting these things are also being factored
        # self.should_be_long_term_version
        # mysql_backup.MysqlBackup.compression_enabled

        for bkup_file_obj in self.bkup_file_objs:
            if isinstance(bkup_file_obj, mysql_backup.CheckSumFile):
                has_checksum_file = True
                result = bkup_file_obj.get_checksum()
                if isinstance(result, str):
                    checksum_file_has_content = True
            elif isinstance(bkup_file_obj, mysql_backup.UncompressedFile):
                has_uncompressed_file = True
            elif isinstance(bkup_file_obj, mysql_backup.CompressedFile):
                has_compresssed_file = True

        # Missing a checksum object, game over.  Backup not to be trusted
        # Missing a checksum object, game over.  Backup not to be trusted
        if not has_checksum_file:
            self.self_destruct()
            msg = "Checksum file missing.  This backup is invalid."
            MysqlBackupInstance.backup_logger.error(msg, extra={'object': self})
            raise RuntimeError(msg)

        if not checksum_file_has_content:
            self.self_destruct()
            msg = "Checksum file exists but had no content.  Checksum file is not valid."
            MysqlBackupInstance.backup_logger.error(msg, extra={'object': self})
            raise RuntimeError(msg)


        # If both files exist, this is strange.  The compressed one is
        # not to be trusted but let's assume it's a failure during the
        # compression of step
        if has_compresssed_file and has_uncompressed_file:
            for bkup_file_obj in self.bkup_file_objs:
                if isinstance(bkup_file_obj, mysql_backup.CompressedFile):
                    bkup_file_obj.self_destruct()
                    if bkup_file_obj.exists():
                        msg = "Tried to delete the backup file but failed."
                        MysqlBackupInstance.backup_logger.error(msg, extra={'object': self})
                        raise RuntimeError(msg)
                    has_uncompressed_file = False

        # If neither a compressed or uncompressed version exists
        # this is a bad backup and should not be trusted
        if True not in (has_uncompressed_file, has_compresssed_file):

            self.self_destruct()
            msg = "No backups actually exist.  Self destructing this instance."
            MysqlBackupInstance.backup_logger.error(msg, extra={'object': self})
            raise RuntimeError(msg)

        # At this point the expectation is that there is one backup file
        # and one checksum file.  To be really, really sure, let's double check
        # and while we are at it, set up the return values
        bkpfileobj = None
        checksumfileobj = None
        bkup_file_obj_count = 0

        for bkup_file_obj in self.bkup_file_objs:
            if isinstance(bkup_file_obj, (mysql_backup.UncompressedFile, mysql_backup.CompressedFile)):
                bkup_file_obj_count += 1
                bkpfileobj = bkup_file_obj
            elif isinstance(bkup_file_obj, mysql_backup.CheckSumFile):
                checksumfileobj = bkup_file_obj

        if bkup_file_obj_count != 1 or checksumfileobj is None:
            self.self_destruct()
            msg = "An error occur when validating the the backup file state."
            MysqlBackupInstance.backup_logger.error(msg, extra={'object': self})
            raise RuntimeError(msg)

        returndict = {
            'checksumfileobj': checksumfileobj,
            'bkupfileobj': bkpfileobj,
        }
        return returndict

    def any_files_being_written(self):
        for bkup_file_obj in self.bkup_file_objs:
            if mysql_backup.MysqlBackup.is_file_open(bkup_file_obj.file_name_full_path):
                return True
        return False

    def set_compression_state(self):
        """return: void or fail
        If backups should or should not be compressed,
        do the right thing and make it so. Should something
        change here, the self.incremental_backup_file_obj
        will be updated to become the modified object and
        the old one cleaned up.

        This would typically only be called after
        clean_bad_files_return_good_file_objects_or_fail
        and after self.incremental_backup_file_obj has
        been initialized"""

        if self.incremental_backup_file_obj is None:
            self.self_destruct()
            msg = "This should never be called without the incremental_backup_file_obj initialized." \
                  "This is a weird error that is never to be expected. Did you run " \
                  "clean_bad_files_return_good_file_objects_or_fail and initialize " \
                  "incremental_backup_file_obj?"
            MysqlBackupInstance.backup_logger.error(msg, extra={'object': self})
            raise RuntimeError(msg)

        else:

            # When compression should exist, make it so
            if mysql_backup.MysqlBackup.compression_enabled and isinstance(self.incremental_backup_file_obj,
                                                                           mysql_backup.UncompressedFile):
                cmpf = mysql_backup.MysqlBackupFileFactory.create_file_object(self.db_name,
                                                                              ucpf=self.incremental_backup_file_obj)
                # Add the compressed file object as managed by this instance
                self.bkup_file_objs.append(cmpf)

                # at this point the uncompressed file should have been removed.  Let's double check or fail.
                # before removing it from the backup file objects here and pointing to the new file

                if self.incremental_backup_file_obj.exists():
                    msg = "Attempted to delete the compressed file but failed."
                    MysqlBackupInstance.backup_logger.error(msg, extra={'object': self})
                    raise RuntimeError(msg)

                # it is now safe to drop the uncompressed file object as managed by this instance.
                # and set the new incremental_backup_file_obj to the compressed file object
                self.bkup_file_objs = [bkobj for bkobj in self.bkup_file_objs
                                       if not isinstance(bkobj, mysql_backup.UncompressedFile)]
                self.incremental_backup_file_obj = cmpf

            # When compression should not exist, make it so
            elif not mysql_backup.MysqlBackup.compression_enabled and isinstance(self.incremental_backup_file_obj,
                                                                                 mysql_backup.CompressedFile):
                ucmf = self.incremental_backup_file_obj.decompress()

                # Add the decompressed file object as managed by this instance
                self.bkup_file_objs.append(ucmf)

                # at this point the decompressed file should have been removed.  Let's double check or fail.
                # before removing it from the backup file objects here and pointing to the new file

                if self.incremental_backup_file_obj.exists():
                    msg = "Attempted to delete the decompressed file but failed."
                    MysqlBackupInstance.backup_logger.error(msg, extra={'object': self})
                    raise RuntimeError(msg)

                # it is now safe to drop the uncompressed file object as managed by this instance.
                # and set the new incremental_backup_file_obj to the compressed file object
                self.bkup_file_objs = [bkobj for bkobj in self.bkup_file_objs
                                       if not isinstance(bkobj, mysql_backup.CompressedFile)]
                self.incremental_backup_file_obj = ucmf

    def set_as_long_term_version(self, lt_state):
        """Input: lt_state (bool)
        Result: This instace will either become the long
        term backup or remove itself as being the long term backup"""

        lt_cur_state = self.is_a_long_term_version()
        if lt_cur_state != lt_state:
            if lt_state:
                self.incremental_backup_file_obj.copy_to_long_term_backup()
            else:
                self.incremental_backup_file_obj.remove_long_term_version()
