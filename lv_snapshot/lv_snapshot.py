from os.path import islink
import subprocess
import mysql_backup.mysql_backup


class LvSnapshot:
    """Class to manage lv snapshots"""

    backup_logger = None

    def __init__(self, vg, lv, snapshot_name, size_gb):
        """provide vg,lv, snapshot_name, and size_mb (snapshot allocation size) to this constructor"""

        self.vg = vg
        self.lv = lv
        self.snapshot_name = snapshot_name
        self.size_gb = size_gb
        LvSnapshot.backup_logger = mysql_backup.mysql_backup.MysqlBackup.backup_logger

    def __str__(self):
        return "/" + self.vg + "/" + self.lv + "/" + self.snapshot_name + " snapshot instance"

    def get_snapshot_status(self, is_mounted=False):
        """return: Bool
        Whether or not a snapshot already exists.  If is_mounted is True
        will return True only when the snapshot exists and is mounted."""
        if not is_mounted:
            return islink('/dev/'+self.vg+'/'+self.snapshot_name)
        elif islink('/dev/'+self.vg+'/'+self.snapshot_name):
            cmd = ['/sbin/lvdisplay', '-c', '/dev/'+self.vg+'/'+self.snapshot_name,]
            output = int(subprocess.check_output(cmd, close_fds=True).split(':')[5])
            if output:
                return True
            else:
                return False
        else:
            return False

    def ensure_snapshot_exists(self):
        """return: void
        Ensures a snapshot exists or raises an IOError"""

        if not self.get_snapshot_status():

            cmd = ['/sbin/lvcreate', '--snapshot', '-L', str(self.size_gb)+'G', '--name', self.snapshot_name,
                   '/dev/'+self.vg+'/'+self.lv]

            LvSnapshot.backup_logger.info("Snapshot does not exist.  Attempting the following command:\n " + " ".join(cmd),
                         extra={'object': self})

            lv_snap = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            lv_snap.wait()

            if not self.get_snapshot_status():
                raise IOError("Failed to create snapshot named %s at /dev/%s/%s of size %d" %
                              (self.snapshot_name, self.vg, self.lv, self.size_gb))

    def delete_snapshot(self):
        """Remove a snapshot.  Will raise IOError on failure."""

        if self.get_snapshot_status(is_mounted=True):
            raise IOError("Failed to delete snapshot.  Snapshot is currently mounted.")

        if self.get_snapshot_status():

            cmd = ['/sbin/lvremove', '-f', '/dev/'+self.vg+'/'+self.snapshot_name]

            LvSnapshot.backup_logger.info("Removing snapshot with the following command: \n" + ' '.join(cmd), extra={'object': self})

            lv_snap = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            lv_snap.wait()

            if self.get_snapshot_status():
                msg = "Failed to delete the snapshot."
                LvSnapshot.backup_logger.error(msg, extra={'object': self})
                raise IOError(msg)

        else:
            msg = "Failed to delete snapshot.  Snapshot does not exist."
            LvSnapshot.backup_logger.error(msg, extra={'object': self})
            raise IOError(msg)

    def safe_refresh_snapshot(self):
        """If the snapshot is not mounted, delete it and recreate it"""

        LvSnapshot.backup_logger.info("Begin refreshing snapshot at /dev/%s/%s" % (self.vg, self.snapshot_name),
                                      extra={'object': self})

        if not self.get_snapshot_status(is_mounted=True):

            LvSnapshot.backup_logger.debug("Snapshot is not mounted.", extra={'object': self})

            if self.get_snapshot_status():
                LvSnapshot.backup_logger.debug("Snapshot exists.  Deleting it.", extra={'object': self})
                self.delete_snapshot()
            else:
                LvSnapshot.backup_logger.debug("Snapshot did not exist.  Nothing to delete.", extra={'object': self})

                LvSnapshot.backup_logger.info("Snapshot now removed if it was there.  Ensuring one exists.", extra={'object': self})

            self.ensure_snapshot_exists()

        else:
            LvSnapshot.backup_logger.info("Snapshot was mounted, not refreshing it.", extra={'object': self})

        if not self.get_snapshot_status():
            msg = "Snapshot still does not exist.  Something went bad."
            LvSnapshot.backup_logger.error(msg, extra={'object': self})
            raise IOError(msg)
        else:
            msg = "Snapshot verified to exist."
            LvSnapshot.backup_logger.info(msg, extra={'object': self})
