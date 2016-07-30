### File Location: /EdgeCast/logs/bin$ cat ftpdelivery.py
### From: Big Data Team

#!/usr/bin/python

"""Deliver log files via FTP.

Iterate through customers and find those with FTP delivery enabled.

Pull their FTP server settings and upload the files to them. If the files aren't
in the EC storage logs folder then they are out of luck.
"""

import os
import sys
import datetime
import getopt
import simplejson
import ftplib
import socket
import string
import smtplib
import random
import time
import StringIO

from pymongo import MongoClient
from pymongo.read_preferences import ReadPreference
from pymongo.errors import AutoReconnect, ConnectionFailure

import paramiko

import logging
logging.basicConfig()

sys.path.append('/EdgeCast/base')
import srvinfo
DEFAULT_FROM_EMAIL = srvinfo.getSrvinfo('default-from-email')
SMTPSERVER =  srvinfo.getSrvinfo('smtp-server')
MEDIATYPES = simplejson.loads(srvinfo.getSrvinfo('logs-mediatypes'))
DELIVERYPOP = simplejson.loads(srvinfo.getSrvinfo('logs-delivery-pops'))

db = None


class FtpClientException(Exception):
    pass


class FtpClient(object):
    """FTP client to wrap sftp and S/FTP connections."""

    def __init__(self, hostname, username, password, dir_path=None, use_sftp=False,
                 private_key=None
):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.dir_path = dir_path
        self.use_sftp = use_sftp
        self.private_key = private_key
        self.conn = None

    def _get_ssh_key(self):
        if self.private_key is not None:
            key_obj = StringIO.StringIO(self.private_key)
            if 'RSA' in self.private_key:
                if self.password is not None:
                    return paramiko.RSAKey(file_obj=key_obj,
                                           password=self.password)
                else:
                    return paramiko.RSAKey(file_obj=key_obj)
            elif 'DSS' in self.private_key:
                    if self.password is not None:
                        return paramiko.DSSKey(file_obj=key_obj,
                                               password=self.password)
                    else:
                        return paramiko.DSAKey(file_obj=key_obj)
        return None


    def _connect(self):
        if self.use_sftp:
            # need to keep the sshclient so it doesn't get gc'd
            self.conn = paramiko.SSHClient()
            # need to use sshclient to set host policy
            self.conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            pkey = self._get_ssh_key()
            if pkey is not None:
                self.conn.connect(hostname=self.hostname,
                                  username=self.username,
                                  pkey=pkey,
                                  timeout=60)
            else:
                self.conn.connect(username=self.username,
                                  hostname=self.hostname,
                                  password=self.password,
                                  timeout=60)
            self.sftp = self.conn.open_sftp()
        else:
            self.ftp = ftplib.FTP(self.hostname,
                              self.username,
                              self.password,
                              timeout=60)
            self.conn = self.ftp

    def _change_directory(self):
        if not self.dir_path or self.dir_path == ' ':
            return

        if not hasattr(self, 'ftp') and not hasattr(self,'sftp'):
            raise FtpClientException('No connection object.')

        if self.use_sftp:
            self.sftp.chdir(self.dir_path)
        else:
            self.ftp.cwd(self.dir_path)

    @staticmethod
    def get_client(hostname, username, password, dir_path, use_sftp,
                   private_key=None):
        message = None
        client = FtpClient(hostname, username, password, dir_path, use_sftp, private_key)

        try:
            client._connect()
        except (paramiko.ssh_exception.AuthenticationException,
                paramiko.ssh_exception.BadAuthenticationType,
                ftplib.error_perm):
            message = "FTP log delivery was unable to deliver log files due to a failed login. Please check the login credentials in the Media Control Center."
        except paramiko.ssh_exception.SSHException, msg:
            message = "sFTP log delivery was unable to deliver log files due to failures in SSH2 protocol negotiation or logic errors (%s). \
Please check the FTP server %s. If you continue to have problems please contact our support team." % (msg, hostname)
        except ftplib.error_temp, msg:
            message = "FTP log delivery was unable to deliver log files due to a temporary problem with the FTP server (%s). \
Please check the FTP server %s. If you continue to have problems please contact our support team." % (msg, hostname)
        except ftplib.all_errors, msg:
            message = "FTP log delivery was unable to deliver log files due to a problem with the FTP server (%s). \
Please check the FTP server %s. If you continue to have problems please contact our support team." % (msg, hostname)
        except EOFError:
            message = "FTP log delivery was unable to deliver log files due to a problem with the FTP server. \
The connection response returned malformed data by the FTP server (%s). If you continue to have problems please contact our support team." % hostname

        if message is not None:
            raise FtpClientException(message)

        if client.conn is not None:
            try:
                client._change_directory()
            except ftplib.error_perm:
                message = "FTP Log delivery was unable to deliver log files due to a permissions problem with the FTP server. Please check the FTP server (%s) and path (%s) permissions." % (hostname, dir_path)
            except (EOFError,IOError):
                message = "FTP Log delivery was unable to deliver log files due a non-existant directory on the FTP server. Please check the FTP server (%s) and path (%s)." % (hostname, dir_path)
            if message is not None:
                raise FtpClientException(message)
        return client

    def send_package(self, options, filename, package_name):
        message = None
        if self.use_sftp:
            try:
                self.sftp.put(filename, package_name)
            except Exception, msg:
                message = "The delivery of the log file %s failed due to a FTP error (%s). \
If you continue to have problems please contact our support team." % (package_name, msg)
        else:
            try:
                fh = open(filename, 'r')
            except Exception, e:
                logger(options, "Could not open file - %s (%s)" % (filename, e))
                return None

            try:
                self.ftp.storbinary('STOR %s' % package_name, fh)
                fh.close()
                return None
            except socket.error:
                message = "The delivery of the log file %s failed due to a connection timeout. \
If you continue to have problems please contact our support team." % package_name
                fh.close()
            except ftplib.all_errors, msg:
                fh.close()
                if 'Transfer complete' in str(msg):
                    return None
                message =  "The delivery of the log file %s failed due to a FTP error (%s). \
If you continue to have problems please contact our support team." % (package_name, msg)
            except:
                logger(options, "Failed to deliver log file %s" % filename)
                return None
            if message is not None:
                raise FtpClientException(message)
            return None


def logger(options, msg):
    options['logfile'].write("%s - %s\n" % (datetime.datetime.now(), msg))


def get_mongodb_connection(options):

    global db

    for i in xrange(0, 10):
        try:
            db = MongoClient([srvinfo.getSrvinfo('ltraq-%s' % MEDIATYPES[str(options['mediatype'])])])
            return True
        except ConnectionFailure, err:
            logger(options,"Cannot connect to Mongodb replica set [%s]: %s, re-trying in 30s" % (srvinfo.getSrvinfo('ltraq-%s' % MEDIATYPES[str(options['mediatype'])]), err))
            time.sleep(30)

    return False


def main(options):

    global db
    if not get_mongodb_connection(options):
        return False

    # Go through and mark the customers that have logs to be delivered
    for i in xrange(0,10):
        try:
            customers = db.ltraq.customer_log_delivery.find({'ftp': True})
            break
        except AutoReconnect:
            logger(options, "AutoReconnect while getting customers list")
            time.sleep(30)
        except ConnectionFailure:
            logger(options, "Mongodb connection failed")
            if not get_mongodb_connection(options):
                return None

    customer_with_rawlogs = shuffle_customers(customers)

    for customer in customer_with_rawlogs:

        if options['customer_id'] != 'any':
            if options['customer_id'] != customer['customer_id']:
                continue

        if not customer['unixpath']:
            continue

        if options['regional']:
        # Skip customer whoes origin is on different region
            if customer['unixpath'] not in DELIVERYPOP:
                if srvinfo.getSrvinfo('pop','None') not in DELIVERYPOP['default']:
                    continue
            elif srvinfo.getSrvinfo('pop','None') not in DELIVERYPOP[customer['unixpath']]:
                continue

        # Lock 1 ftp delivery session per customer
        cid = db.ltraq.ftpdelivery.find_one({'mt': options['mediatype'], 'ci': customer['customer_id']})
        if cid:
            if cid['lm'] > datetime.datetime.now() - datetime.timedelta(hours=24):
                logger(options, "Customer %X logs is being uploaded via ftp" % customer['customer_id'])
                continue
            else:
                logger(options, "Clean up lock for customer %X ftp delivery" % customer['customer_id'])
                db.ltraq.ftpdelivery.remove({'_id': cid['_id']})
                continue

        _cid = db.ltraq.ftpdelivery.insert({'mt': options['mediatype'], 'ci': customer['customer_id'], 'lm': datetime.datetime.now(), 'node': socket.gethostname()})
        if not _cid:
            continue

        has_packages = True
        while has_packages:
            has_packages = get_customer_package(options, customer)

        # Cleanup ftp delivery lock
        for i in xrange(0,10):
            try:
                db.ltraq.ftpdelivery.remove({'_id': _cid})
                break
            except AutoReconnect:
                logger(options, "AutoReconnect while removing ftp lock")
                time.sleep(30)


def shuffle_customers(customers):
    shuffled = []

    for customer in customers:
        shuffled.append(customer)

    random.shuffle(shuffled)
    return shuffled


def get_customer_package(options, customer):
    global db
    # Look back 7 days for files to deliver via FTP
    back_date = datetime.datetime.now() - (datetime.timedelta(days=8))
    query = {'mt': options['mediatype'], 'ci': customer['customer_id'], 'st': 10, 'ad': {'$gte': int('%d%02d%02d' % (back_date.year, back_date.month, back_date.day))}, 'seq': {'$exists': False} }
    package = db.ltraq.packages.find_and_modify(query=query, update={'$inc': {'st':1}}, upsert=False, new=False)
    if package:
        ret = deliver_package(options, customer, package)
        if ret == 2:
            # Temporary failure. FTP settings doesn't have any data for this customer try again later.
            return False
        return True
    return False


def deliver_package(options, customer, package):
    global db

    ftp_settings = db.ltraq.customer_ftp_settings.find_one({'customer_id': customer['customer_id']})
    if not ftp_settings:
        return 2

    package_dir = os.path.join(customer['unixpath'],
                               '{0:04X}'.format(customer['customer_id']),
                               'logs')

    if not os.path.isdir(package_dir):
        logger(options, "Package path doesn't exist %s" % package_dir)
        return False

    conn, e_message = ftp_connect(ftp_settings)

    if conn:
        e_message = send_package(options, conn, os.path.join(package_dir, package['nm']), package['nm'])

    if e_message:
        notify_customer(options, ftp_settings['email'], 'FTP Log Delivery FAILED: %s' % package['nm'], e_message, ftp_settings['from_email'])
    else:
        logger(options, "Package delivered: %s/%s" % (package_dir, package['nm']))

    return True


def send_package(options, conn, filename, package_name):
    if not os.path.exists(filename):
       logger(options, "File not found - %s" % filename)

    try:
        conn.send_package(options, filename, package_name)
    except FtpClientException, e:
        return e.message
    return None


def ftp_connect(ftp_settings):
    try:
        client = FtpClient.get_client(hostname=ftp_settings['hostname'],
                                      username=ftp_settings['username'],
                                      password=ftp_settings['password'],
                                      dir_path=ftp_settings['dir_path'],
                                      use_sftp=ftp_settings.get('sftp', False),
                                      private_key=ftp_settings.get('private_key', None))

        return client, None
    except FtpClientException, e:
        logger(options, str(e))
        return None, str(e)


def notify_customer(options, email, subject, body, fromemail):
    if email:
        if not fromemail:
            fromemail = DEFAULT_FROM_EMAIL
        msg = string.join((
            "From: Log Notification Alert <%s>" % fromemail,
            "To: %s" % email,
            "Subject: %s" % subject,
            "",
            body
            ), "\r\n")

        logger(options, "Emailing: %s" % msg)

        try:
            s = smtplib.SMTP(SMTPSERVER)
            s.sendmail(fromemail, email, msg)
            s.quit()
        except (socket.error, smtplib.SMTPConnectError, smtplib.SMTPException), err:
            logger(options, "Failed to send notification to %s: %s" % (email, err))


def usage():
    print "package.py -m <mediatype_id> -l <logfile location>"
    sys.exit()


def get_options():
    options = {
        'accessdate': 'any',
        'customer_id': 'any',
        'mediatype': None,
        'regional': True,
        'logfile': sys.stdout }

    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:l:m:a:h:r")
    except getopt.GetoptError:
        sys.stdout = sys.stderr
        usage()

    for o, v in opts:
        if o == '-l':
            fh = open(v, 'a')
            options['logfile'] = fh
        elif o == '-a':
            options['accessdate'] = int(v)
        elif o == '-c':
            options['customer_id'] = int(v)
        elif o == '-h':
            options['customer_id'] = int(v, 16)
        elif o == '-m':
            options['mediatype'] = int(v)
        elif o == '-r':
            options['regional'] = False

    for option in options:
        if not options[option]:
            usage()

    return options


if __name__ == '__main__':
    options = get_options()

    logger(options, "Starting FTP Delivery")
    main(options)
    logger(options, "Ending FTP Delivery")

    # Ensure close connection pool/health check
    if db:
        db.close()
