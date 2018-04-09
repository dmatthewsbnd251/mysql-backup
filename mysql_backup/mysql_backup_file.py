# Factory
# A simple factory method to determine if a file
# appears to be associated with a MySQL backup.
# If it is, will become the correct type of backup file.

import os
import mysql_backup
import re
import time
import subprocess
from abc import abstractmethod
from shutil import copyfile
# import traceback
# import psutil


class MysqlBackupFileFactory(object):

    backup_logger = None

    def __init__(self, file_name_full_path, path, file_name, file_name_no_ext, file_ext, db_name, date_string):

        MysqlBackupFileFactory.backup_logger = mysql_backup.mysql_backup.MysqlBackup.backup_logger

        self.file_name_full_path = file_name_full_path
        self.path = path
        self.file_name = file_name
        self.file_name_no_ext = file_name_no_ext
        self.file_ext = file_ext
        self.db_name = db_name
        self.date_string = date_string

        # database connection
        # letting the file objects have their own database connection
        # will allow for parallel backups
        self.db_connection = None
        self.cursor = None
        self.cur_database = None

    def __str__(self):
        return self.file_name_full_path

    def get_age_secs(self):
        mysql_backup.MysqlBackup.get_file_age(age_format='seconds', file_name=self.file_name_full_path)

    def self_destruct(self):
        """Delete the backup file associated with this object."""
        if os.path.isfile(self.file_name_full_path):
            MysqlBackupFileFactory.backup_logger.debug("File exists and removal requested.  Removing.",
                                                       extra={'object': self})
            os.remove(self.file_name_full_path)
        else:
            MysqlBackupFileFactory.backup_logger.debug("File does not exist and removal requested.  Whatever.",
                                                       extra={'object': self})

        MysqlBackupFileFactory.backup_logger.debug("Requesting removal of the long term copy.", extra={'object': self})
        self.remove_long_term_version()

    def exists(self):
        """Does the file described by this file object exist
        Return: bool"""
        MysqlBackupFileFactory.backup_logger.debug("Determining if file exists.", extra={'object': self})
        return os.path.isfile(self.file_name_full_path)

    def get_long_term_backup_full_name(self):
        return mysql_backup.MysqlBackup.long_term_backup_path.rstrip('/') + '/' + self.file_name

    def is_a_long_term_version(self):
        """This file is in long term version path"""
        return os.path.isfile(self.get_long_term_backup_full_name())

    def copy_to_long_term_backup(self):
        src = self.file_name_full_path
        dst = self.get_long_term_backup_full_name()
        MysqlBackupFileFactory.backup_logger.info("copying %s to %s" % (src, dst), extra={'object': self})
        copyfile(src, dst)

    def remove_long_term_version(self):
        if self.is_a_long_term_version():
            MysqlBackupFileFactory.backup_logger.debug("Long term version exists.  Removing.", extra={'object': self})
            os.remove(self.get_long_term_backup_full_name())
        else:
            MysqlBackupFileFactory.backup_logger.debug("Long term version does not exist.  Nothing to do.",
                                                       extra={'object': self})


    @abstractmethod
    def birth(self, **kwargs):
        """This is expected to be overridden per each returned backup file type"""
        MysqlBackupFileFactory.backup_logger.error("birth called when not implemented.  Raising an exception.",
                                                   extra={'object': self})
        raise NotImplemented("Not implemented on this object class.")

    @staticmethod
    def get_file_object(file_name_full_path):
        """Static factory method to return the proper file type object"""

        path = os.path.dirname(file_name_full_path)
        file_name = os.path.basename(file_name_full_path)

        if path != mysql_backup.MysqlBackup.incremental_path:
            msg = "File not in a backup path"
            MysqlBackupFileFactory.backup_logger.error(msg, extra={'object': file_name_full_path})
            raise AssertionError(msg)

        if '.' not in file_name or file_name.count('__') != 1:
            msg = "File does not appear to be valid"
            MysqlBackupFileFactory.backup_logger.error(msg, extra={'object': file_name_full_path})
            raise AssertionError(msg)

        if file_name.endswith(mysql_backup.MysqlBackup.compressed_file_extension):
            file_name_no_ext = '.'.join(file_name.split('.')[0:-2])
        else:
            file_name_no_ext = '.'.join(file_name.split('.')[0:-1])
        file_ext = file_name.split('.')[-1]
        db_name = file_name_no_ext.split('__')[0]
        date_string = file_name_no_ext.split('__')[1]

        if file_ext not in ('md5', 'sql',mysql_backup.MysqlBackup.compressed_file_extension):
            msg = "File extension does not appear to be valid.  Extenion was %s" % file_ext
            MysqlBackupFileFactory.backup_logger.error(msg, extra={'object': file_name_full_path})
            raise AssertionError(msg)

        if not re.match('^\d{8}-\d{6}$', date_string):
            msg = "Date does not adhere to the expected format within the file.  Date looked lik %s" % date_string
            MysqlBackupFileFactory.backup_logger.error(msg, extra={'object': file_name_full_path})
            raise AssertionError(msg)

        if file_ext == 'sql':
            return UncompressedFile(file_name_full_path=file_name_full_path, path=path, file_name=file_name,
                                    file_name_no_ext=file_name_no_ext, file_ext=file_ext, db_name=db_name,
                                    date_string=date_string)
        elif file_ext == mysql_backup.MysqlBackup.compressed_file_extension:
            return CompressedFile(file_name_full_path=file_name_full_path, path=path, file_name=file_name,
                                  file_name_no_ext=file_name_no_ext, file_ext=file_ext, db_name=db_name,
                                  date_string=date_string)
        elif file_ext == 'md5':
            return CheckSumFile(file_name_full_path=file_name_full_path, path=path, file_name=file_name,
                                file_name_no_ext=file_name_no_ext, file_ext=file_ext, db_name=db_name,
                                date_string=date_string)

    @staticmethod
    def create_file_object(db_name, **kwargs):
        """
        db_name is required.  Additionally,

        1: Passing nothing runs a backup and returns a dictionary with an uncompressed file object
           and a checksum file object
           intput: db_name
           return:
           dict {
                'checksum file object' = CheckSumFile,
                'uncompressed file object' = UncompressedFile
           }
        2: Passing a UncompressedFile object will return a CompressedFile Object
            inpute: db_name, UncompressedFile

            return: CompressedFile

        """
        if 'ucpf' in kwargs:
            # initialize an instance of an uncompressed backup object
            ucpf = kwargs['ucpf']
            date_string = ucpf.date_string
            file_ext = mysql_backup.MysqlBackup.compressed_file_extension
            file_name_no_ext = ucpf.file_name_no_ext + '.sql'
            file_name = file_name_no_ext + '.' + file_ext
            file_name_full_path = mysql_backup.MysqlBackup.incremental_path.rstrip('/') + '/' + file_name

            cmpf = CompressedFile(file_name_full_path=file_name_full_path,
                                  path=mysql_backup.MysqlBackup.incremental_path,
                                  file_name=file_name,
                                  file_name_no_ext=file_name_no_ext,
                                  file_ext=file_ext,
                                  db_name=db_name,
                                  date_string=date_string)

            MysqlBackupFileFactory.backup_logger.debug("Requesting conversion of an uncompressed file object to"
                                                       " become a compressed file object.", extra={'object': db_name})
            cmpf.birth(ucpf=kwargs.get('ucpf'))
            return cmpf

        else:
            # initialize an instance of an uncompressed backup object
            date_string = mysql_backup.MysqlBackup.human_readable_date_from_tt(time.localtime())
            file_ext = 'sql'
            file_name_no_ext = db_name + '__' + date_string
            file_name = file_name_no_ext + '.' + file_ext
            file_name_full_path = mysql_backup.MysqlBackup.incremental_path.rstrip('/') + '/' + file_name

            ucpf = UncompressedFile(file_name_full_path=file_name_full_path,
                                    path=mysql_backup.MysqlBackup.incremental_path,
                                    file_name=file_name,
                                    file_name_no_ext=file_name_no_ext,
                                    file_ext=file_ext,
                                    db_name=db_name,
                                    date_string=date_string)

            MysqlBackupFileFactory.backup_logger.debug("Requesting creation of an uncompressed file object.",
                                                       extra={'object': db_name})
            ucpf.birth()

            # initialize an instance of a CheckSumFile object

            file_ext = 'md5'
            file_name_full_path = '.'.join(file_name_full_path.split('.')[0:-1]) + '.' + file_ext
            chksmf = CheckSumFile(file_name_full_path=file_name_full_path,
                                  path=mysql_backup.MysqlBackup.incremental_path,
                                  file_name=file_name_no_ext + '.' + file_ext,
                                  file_name_no_ext=file_name_no_ext,
                                  file_ext=file_ext,
                                  db_name=db_name,
                                  date_string=date_string)

            MysqlBackupFileFactory.backup_logger.debug("Requesting creation of a checksum file object.",
                                                       extra={'object': db_name})
            chksmf.birth(ucpf=ucpf)

            return {
                'checksum file object': chksmf,
                'uncompressed file object': ucpf,
            }

    @staticmethod
    def get_checksum_from_file(file_name):
        return subprocess.check_output(["/bin/md5sum", file_name, ], close_fds=True).split()[0]


class CheckSumFile(MysqlBackupFileFactory):

    def birth(self, **kwargs):
        """Required (key word arg): ucpf (type=UncompressedFile)"""
        if 'ucpf' not in kwargs:
            msg = "ucpf (UncompressedFile object is required when creating a CheckSumFile object)"
            MysqlBackupFileFactory.backup_logger.error(msg, extra={'object': self})
            raise ValueError(msg)

        ucpf = kwargs.get('ucpf')
        self.write_checksum(ucpf)

    def get_checksum(self, ucpf=None):
        """Pass a UncompressFile object when the file itself needs a checksum calculated,
        otherwise read the file and return"""
        if ucpf is not None and not isinstance(ucpf, UncompressedFile):
            msg = "ucpf must be an uncompressedfile object"
            MysqlBackupFileFactory.backup_logger.error(msg, extra={'object': self})
            raise ValueError(msg)
        elif ucpf is not None:
            return MysqlBackupFileFactory.get_checksum_from_file(ucpf.file_name_full_path)
        else:
            with open(self.file_name_full_path, 'r') as checksum_file_pointer:
                checksum_str = checksum_file_pointer.readline()
                checksum_file_pointer.close()
                return checksum_str

    def write_checksum(self,ucpf):
        if mysql_backup.MysqlBackup.verbose:
            MysqlBackupFileFactory.backup_logger.debug("Getting checksum from file at %s" % ucpf.file_name_full_path,
                                                       extra={'object': self})
        checksum = self.get_checksum(ucpf)
        with open(self.file_name_full_path, 'w') as checksum_file_pointer:
            if mysql_backup.MysqlBackup.verbose:
                MysqlBackupFileFactory.backup_logger.info("writing new checksum file at %s" % self.file_name_full_path,
                                                          extra={'object': self})
            checksum_file_pointer.write(checksum)
        checksum_file_pointer.close()


class UncompressedFile(MysqlBackupFileFactory):

    def birth(self):
        """void
        creates a mysql backup"""
        os.environ['MYSQL_PWD'] = mysql_backup.MysqlBackup.mysql_password
        # command = '/usr/bin/mysqldump -u ' + mysql_backup.MysqlBackup.mysql_username + ' ' + self.db_name + ' ' + \
        #          mysql_backup.MysqlBackup.mysql_dump_options + ' --result-file ' + self.file_name_full_path

        command = ['/usr/bin/mysqldump', '-u', mysql_backup.MysqlBackup.mysql_username, self.db_name] + \
                  mysql_backup.MysqlBackup.mysql_dump_options.split() + ['--result-file', self.file_name_full_path]

        MysqlBackupFileFactory.backup_logger.info("running %s" % (' '.join(command),), extra={'object': self})

        process = subprocess.Popen(command, stdout=subprocess.PIPE, close_fds=True)
        process.wait()
        if process.returncode != 0:
            msg = "Something went wrong while trying to backup %s" % self.db_name
            MysqlBackupFileFactory.backup_logger.error(msg, extra={'object': self})
            raise RuntimeError(msg)
        else:
            MysqlBackupFileFactory.backup_logger.debug("command completed successfully.", extra={'object': self})


class CompressedFile(MysqlBackupFileFactory):

    def birth(self, ucpf):
        """void
        create an compressed file"""
        self.compress_ucpf(ucpf=ucpf)

    def compress_ucpf(self, ucpf):
        """Compresses a CompressedFile object
        and requested an UncompressedFile to self destruct"""
        cmd = mysql_backup.MysqlBackup.compress_command.split()
        cmd.append(ucpf.file_name_full_path)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        MysqlBackupFileFactory.backup_logger.info("compressing %s" % (ucpf.file_name_full_path,),
                                                  extra={'object': self})
        process.wait()
        if process.returncode != 0:
            msg = "Something went wrong while trying to compress %s" % ucpf.file_name_full_path
            MysqlBackupFileFactory.backup_logger.error(msg, extra={'object': self})
            raise RuntimeError(msg)
        else:
            MysqlBackupFileFactory.backup_logger.debug("command completed successfully.", extra={'object': self})

        ucpf.self_destruct()

    def decompress(self):
        """Decompress self, self destruct
        return: UncompressedFile object"""
        cmd = mysql_backup.MysqlBackup.decompress_command.split()
        cmd.append(self.file_name_full_path)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        MysqlBackupFileFactory.backup_logger.info("decompressing %s" % (self.file_name_full_path,),
                                                  extra={'object': self})
        process.wait()
        if process.returncode:
            msg = "Something went wrong while trying to decompress %s" % self.file_name_full_path
            MysqlBackupFileFactory.backup_logger.error(msg, extra={'object': self})
            raise RuntimeError(msg)
        else:
            MysqlBackupFileFactory.backup_logger.debug("command completed successfully.", extra={'object': self})

        ucpf = UncompressedFile(
            file_name_full_path='.'.join(self.file_name_full_path.split('.')[0:-1]),
            path=self.path,
            file_name=self.file_name_no_ext + '.sql',
            file_name_no_ext=self.file_name_no_ext,
            file_ext='sql',
            db_name=self.db_name,
            date_string=self.date_string
        )
        MysqlBackupFileFactory.backup_logger.debug("Removing the compressed file object.", extra={'object': self})
        self.self_destruct()
        return ucpf
