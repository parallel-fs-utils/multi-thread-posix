#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# copyright (c) 2015   Ben England, Red Hat, under Apache License
# see http://www.apache.org/licenses/LICENSE-2.0 for license details
#
# this script does equivalent of "rm -rf " command but in parallel on
# subdirectories

import os
import errno
import multiprocessing
import sys
import time

debug = (os.getenv('DEBUG') is not None)

NOTOK = 1   # process exit status meaning failure


def usage(msg):
    print('ERROR: ' + msg)
    print('usage: parallel-untar.py your-file.tar [ max-threads ]')
    sys.exit(NOTOK)


# python generator to recursively walk directory tree
# looking for all subdirectories,
# returning child directories before their parents
# this allows us to construct list of directories to delete
# in parallel with threads that delete them.

def find_subdirs(d):
    entries = os.listdir(d)
    for e in entries:
        entry_path = os.path.join(d, e)
        if not os.path.islink(entry_path) and os.path.isdir(entry_path):
            for subd in find_subdirs(entry_path):
                yield subd
    yield d


# parse command line inputs and display them

thread_count = 4
topdir = sys.argv[1]
start_time = time.time()

if len(sys.argv) > 2:
    try:
        thread_count = int(sys.argv[2])
    except ValueError as e:
        usage('could not parse thread count %s' % sys.argv[2])
elif len(sys.argv) < 2:
    usage('must supply top-level directory to delete')
print('deleting directory tree %s with up to %d parallel threads' %
      (topdir, thread_count))
if not os.path.isdir(topdir):
    print('parallel-rm-rf.py does not work on anything other than a directory')
    sys.exit(1)


# we use the multiprocessing module to create subprocesses so that the
# python GIL (Global Interpreter Lock) cannot
# get in the way of parallel processing
# this class uses a pipe to receive pathnames of directories to delete
# the parent thread is responsible for partitioning directories amongst
# subprocesses
# each thread sends back a tuple at the end that contains counters
# so parent can print out how the work was divided

class rmThread(multiprocessing.Process):

    def __init__(self, parent_conn_in, child_conn_in, index_in):
        self.index = index_in
        self.parent_conn = parent_conn_in
        self.child_conn = child_conn_in
        self.file_count = 0
        self.dir_count = 0
        self.dir_remove_collisions = 0
        self.dir_remove_nonempty = 0
        multiprocessing.Process.__init__(self)

    def __str__(self):
        return 'rmThread index=%d thread_count=%d directories=%d' % (
            self.index, self.thread_count, len(self.dirlist))

    def run(self):
        while [True]:
            d = self.child_conn.recv()
            if d == os.sep:
                break
            if debug:
                print('thread %d dir %s' % (self.index, d))
            try:
                dir_contents = os.listdir(d)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    self.dir_remove_collisions += 1
                    # not a problem, someone else might have removed
                    continue
                raise e

            # delete contents of directory
            # rather than have competing threads lock directories,
            # we rely on the filesystem to handle cases
            # where two threads attempt to delete at same time
            # one of the threads will get ENOENT in this case,
            # but that's ok, doesn't matter

            for dentry in dir_contents:
                de_path = os.path.join(d, dentry)
                if (not os.path.islink(de_path)) and os.path.isdir(de_path):
                    continue
                if debug:
                    print('%d deleting entry %s' % (self.index, de_path))
                try:
                    os.unlink(de_path)
                    self.file_count += 1
                except OSError as e:
                    if e.errno == errno.ENOENT:
                        self.dir_remove_collisions += 1
                        continue
                    raise e

            # delete directory and non-empty parent directories up to topdir
            # we can't delete d if it contains a subdirectory
            # (that hasn't been deleted yet)
            # that's ok, we'll get ENOTEMPTY and stop
            # other threads could be doing this same thing
            # (e.g. thread that deleted child of d)
            # again, rely on filesystem to deal with this,
            # one thread gets an ENOENT exception
            # that's ok, just stop

            while len(d) >= len(topdir):
                try:
                    os.rmdir(d)
                    self.dir_count += 1
                    if debug:
                        print('thread %d deleted directory %s' %
                              (self.index, d))
                except OSError as e:
                    if e.errno == errno.ENOTEMPTY:
                        self.dir_remove_nonempty += 1  # ok, will delete later
                        break
                    if e.errno == errno.ENOENT:
                        self.dir_remove_collisions += 1  # other thread did it
                        break
                    raise e
                d = os.path.dirname(d)

        self.child_conn.send((self.file_count, self.dir_count,
                             self.dir_remove_collisions,
                             self.dir_remove_nonempty))
        if debug:
            print('child exiting: ' + str(worker_pool))


# MAIN PROGRAM -- create & start worker threads, wait for them to finish

worker_pool = []
for n in range(0, thread_count):
    (parent_conn, child_conn) = multiprocessing.Pipe()
    t = rmThread(parent_conn, child_conn, n)
    worker_pool.append(t)
    t.daemon = True
    t.start()
if debug:
    print('thread pool: ' + str(worker_pool))

# round-robin schedule child threads to process directories
# FIXME: we could do something much more intelligent later on
# like scheduling based on total file count assigned to each thread

index = 0
for d in find_subdirs(topdir):
    worker_pool[index].parent_conn.send(d)
    index += 1
    if index >= thread_count:
        index = 0

elapsed_time = time.time() - start_time
print('constructed directory list and awaiting thread completions ' +
      'after %9.2f sec' % elapsed_time)

total_dirs = 0
total_files = 0
for worker in worker_pool:
    worker.parent_conn.send(os.sep)  # tell child that we're done
    (w_file_count, w_dir_count, w_dir_remove_collisions,
     w_dir_remove_nonempty) = worker.parent_conn.recv()
    worker.join()  # wait for child to exit
    print(('after %7.2f sec thread %d removed %d files and %d dirs ' +
           'with %d collisions and %d non-empty dirs') % (
          time.time() - start_time,
          worker.index,
          w_file_count,
          w_dir_count,
          w_dir_remove_collisions,
          w_dir_remove_nonempty))
    total_dirs += w_dir_count
    total_files += w_file_count

elapsed_time = time.time() - start_time
print('elapsed time = %7.2f sec' % elapsed_time)
fps = total_files / elapsed_time
print('files per second = %8.2f' % fps)
dps = total_dirs / elapsed_time
print('directories per second = %8.2f' % dps)
