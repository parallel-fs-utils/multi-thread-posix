#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# parallel-untar.py - unpack tarball subdirectories in parallel
#
# copyright (c) 2015   Ben England, Red Hat, under Apache license
# see http://www.apache.org/licenses/LICENSE-2.0 for license details

import os
import errno
import tarfile

# we use multiprocessing module to create separate sub-processes and avoid
# the constraints of the python GIL

import multiprocessing
import sys
import time

debug = (os.getenv('DEBUG') is not None)

NOTOK = 1  # process failure exit status


def usage(msg):
    print('ERROR: ' + msg)
    print('usage: parallel-untar.py your-file.tar [ max-threads ]')
    sys.exit(NOTOK)

fmt_dangling_link = \
    'ERROR: %s is a link pointing to an absolute pathname that does not exist'
fmt_link2nonexistent = \
    '%s is a link pointing to a relative non-existent file'

# parse command line inputs

thread_count = 4
start_time = time.time()

if len(sys.argv) > 2:
    try:
        thread_count = int(sys.argv[2])
    except ValueError as e:
        usage('could not parse thread count %s' % sys.argv[2])
elif len(sys.argv) < 2:
    usage('must supply .tar file')
fn = sys.argv[1]
if fn == '--help' or fn == '-h':
    usage('so you need help, we all knew that ;-)')
print('untarring file %s with up to %d parallel threads' % (fn, thread_count))
if not fn.endswith('.tar'):
    usage('parallel-untar.py does not yet support compressed tar files' +
          'uncompress first to .tar file then run it on that')
if not os.path.exists(fn):
    usage('does not exist: %s' % fn)


# this class partitions directories in tar file amongst worker threads
# in a static way
# (thread k handles all directories with index d mod thread_count == k )
# so that no preprocessing is needed

class untarThread(multiprocessing.Process):

    def __init__(
            self, parent_conn_in, child_conn_in,
            index_in, thread_count_in, archive_path_in):

        # init base class

        multiprocessing.Process.__init__(self)

        # save thread inputs for run()

        self.parent_conn = parent_conn_in
        self.child_conn = child_conn_in
        self.index = index_in
        self.thread_count = thread_count_in
        self.archive_path = archive_path_in

        # counters for reporting

        self.file_count = 0
        self.dir_count = 0
        self.dir_create_collisions = 0

    def __str__(self):
        return 'untarThread %d %s %s' % (self.index, self.archive_path)

    def run(self):
        my_dirs = {}
        link_queue = []
        archive = tarfile.open(name=self.archive_path)
        archive.errorlevel = 2  # want to know if errors
        count = self.thread_count - 1
        for m in archive:  # for each thing in the tarfile
            if m.isdir():  # if a directory
                stripped_name = m.name.strip(os.sep)  # remove any trailing '/'
                count += 1
                if count >= thread_count:
                    count = 0
                if count == self.index:
                    if debug:
                        print('thread %d recording on count %d dir %s' %
                              (self.index, count, stripped_name))
                    # value doesn't matter, my_dirs is just a set
                    my_dirs[stripped_name] = self.index
                    try:
                        archive.extract(m)
                    except OSError as e:
                        # race condition if > 1 thread
                        # creating a common parent directory,
                        # just back off different amounts
                        # so one of them succeeds.

                        if e.errno == errno.EEXIST:
                            time.sleep(0.1 * self.index)
                            self.dir_create_collisions += 1
                            archive.extract(m)
                        else:
                            raise e
                    if debug:
                        print('%d got dir %s' % (self.index, m.name))
                    self.dir_count += 1
            else:
                # if not a directory
                dirname = os.path.dirname(m.name)
                # ASSUMPTION: directory object is always read from tarfile
                # before its contents
                if dirname in my_dirs:
                    if m.islnk() or m.issym():
                        print('link %s -> %s' % (m.name, m.linkname))
                        if not os.path.exists(m.linkname):
                            if m.linkname.startswith(os.sep):
                                if debug:
                                    print(fmt_dangling_link % m.linkname)
                            else:
                                # BUT DO IT ANYWAY, that's what tar xf does!
                                # FIXME: how do we know if link target is a
                                # file within the untarred directory tree?
                                # Only postpone link creation for these.

                                if debug:
                                    print(fmt_link2nonexistent % m.linkname)
                                link_queue.append(m)
                                continue
                    try:
                        archive.extract(m)  # not a link or dir at this point
                    except OSError as e:
                        if not (e.errno == errno.EEXIST and m.issym()):
                            raise e
                    if debug:
                        print('%d got file %s' % (self.index, m.name))
                    self.file_count += 1

        # we postpone links to non-existent files in case other threads
        # need to create target files
        # these links are created after
        # all other subprocesses have finished directories and files
        # to ensure that this succeeds.

        self.child_conn.send('y')
        # block until all subprocesses finished above loop
        self.child_conn.recv()

        # now it should be safe to create softlinks that point within this tree

        for m in link_queue:
            try:
                archive.extract(m)
            except OSError as e:
                if not (e.errno == errno.EEXIST and m.issym()):
                    raise e
            if debug:
                print('%d got file %s' % (self.index, m.name))
            self.file_count += 1
        archive.close()
        self.child_conn.send((self.file_count, self.dir_count,
                             self.dir_create_collisions))


# create & start worker threads, wait for them to finish

worker_pool = []
for n in range(0, thread_count):
    (parent_conn, child_conn) = multiprocessing.Pipe()
    t = untarThread(parent_conn, child_conn, n, thread_count, fn)
    worker_pool.append(t)
    t.daemon = True
    t.start()
if debug:
    print('thread pool: ' + str(worker_pool))

# implement barrier for softlink creation within the tree

for t in worker_pool:
    assert t.parent_conn.recv() == 'y'
for t in worker_pool:
    t.parent_conn.send('y')
elapsed_time = time.time() - start_time
print('reached softlink barrier at %7.2f sec' % elapsed_time)

total_files = 0
total_dirs = 0
for t in worker_pool:
    (w_file_count, w_dir_count, w_dir_create_collisions) = \
        t.parent_conn.recv()
    t.join()
    print('thread %d file-count %d dir-count %d create-collisions %d' %
          (t.index, w_file_count, w_dir_count, w_dir_create_collisions))
    total_files += w_file_count
    total_dirs += w_dir_count

elapsed_time = time.time() - start_time
print('all threads completed at %7.2f sec' % elapsed_time)

fps = total_files / elapsed_time
print('files per sec = %9.2f' % fps)

dps = total_dirs / elapsed_time
print('directories per sec = %8.2f' % dps)
