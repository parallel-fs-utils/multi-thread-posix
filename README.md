This set of utilities shows how we can speed up traditional 
POSIX filesystem workloads by using the power of parallel processing, 
particularly in distributed filesystems where a single-threaded workload 
often cannot fully exploit the available hardware.  
To improve common utilities, 
such as tar xf, cp -r, rsync -r, rm -rf, and many more,  
we can exploit the fact that the user does not care 
what order the files are processed, 
as long as they are all processed when the command completes.  
Furthermore, filesystems don't care whether you access files 
in parallel or in series, 
subject to directory existence constraints.  Utilities are:

- parallel-rm-rf.py - delete directory tree by parallel subdirectory removal
- parallel-untar.py - unpack a tar archive in parallel

# parallel-rm-rf.py

Subdirectories are assembled recursively starting from leaf directories so that 
we can usually delete the directory after we have deleted its contents.    
In cases where this is not possible (i.e. thread 1 deleting directory x 
while thread 2 has not emptied directory x/y), 
we postpone directory deletion and 
let the threads processing the subdirectories 
climb up the tree and attempt deletion of parent directories.  
This climbing process terminates when we reach the top directory or 
when we reach a non-empty parent directory.

The first implementation suffered from 
the non-parallelized collection of a subdirectory list.  
It has been improved by making the subdirectory list collection 
concurrent with the removal process, 
by continuously feeding directories from the outer edge of the tree 
(leaf nodes) to deletion threads, 
then their parents, and so on.  
This is done using a recursive python generator, like this:

```
    def find_subdirs(d):
        entries = os.listdir(d)
        for e in entries:
            entry_path = os.path.join(d, e)
            if (not os.path.islink(entry_path)) and os.path.isdir(entry_path):
                for subd in find_subdirs(entry_path):
                    yield subd
        yield d
```

# parallel-untar.py

This utility leverages python *tar* module to extract from a tar file.   Unfortunately we have to read the tar file multiple times to get multiple threads unpacking it, but in cases where there is a small average file size (say < 1 MB), this is not a problem, because overhead of small-file creation far exceeds cost of re-reading the tar archive.

# background

This section is just background info on the problem which this utility and similar software is solving.

Linux utilities such as tar were written up to 40 years ago 
and are typically single-threaded.  
At this point the risk of breaking existing applications 
due to re-engineering of these utilities is far greater 
than the perceived benefit of doing so, 
since they work fine for non-distributed filesystems.  
But there is no reason why we can't implement new utilities that are compatible with 
the old utilities' on-disk format, but have the desired parallelism.  
So why is single-threadedness an issue at all?   Why can't we just use client-side caching?

Linux filesystems are typically POSIX-compliant or attempting to be POSIX compliant.  
But the POSIX standard mandates certain behavior around 
persistence and visibility of metadata changes 
such as file creates, deletes and renames.   
For a distributed filesystem such as Gluster to implement this behavior, 
transactions must be performed between client and server 
to commit changes to persistent storage, 
and changes have to become visible on all clients.  
In some cases, such as a file create, multiple transactions are required.   
By the way, One of the attractive features of object storage 
is that multiple transactions are NOT required, 
but object-store functionality is a subset of POSIX functionality and therefore 
existing applications can't always be implemented easily on top of object storage.

We could get rid of this transactional behavior by relaxing POSIX constraints, 
but this has consequences to applications.  
For example, suppose that we relax the constraint that 
the file must persistently exist and be visible to all clients 
after the creat() system call completes.  
If we relax this rule, then the client can batch creates and 
radically improve performance in some cases, but what if 
2 clients try to create the same file at the same time?  
Unfortunately neither client will return EEXIST to the caller, 
but one of the clients will discover at some later time that 
it cannot write or do other things to the file because 
it was already created by some other client.  
There is no reasonable way to tell the the application the bad news at this point, 
since a POSIX application does not expect 
to get EEXIST back from a write() call or a close() call, for example.

    
Continuing with our example of creat() system call, 
in situations where only one process at a time is accessing a directory, 
the client could lock the directory and then rely on the directory contents cached in the client 
to process creat() system calls, allowing batching of create requests to the server.   
This may well be the best solution.  
But the risk here is that if the client crashes, 
then the files created up to that point and not sent to the server will not exist.  
This could be overcome by allowing the client app to use fsync() on the directory 
to control when the files truly become persistent.  
However, other clients will not see the files 
until the files are committed to the server, 
hence violating POSIX semantics again.  
Perhaps such a feature would be best done as a mount option, 
where the user takes responsibility for ensuring that it is safe 
to perform these optimizations for the specific workload being run.

