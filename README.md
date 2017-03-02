Taskcluster Backup
==================
[![Build Status](https://travis-ci.org/taskcluster/taskcluster-backup.svg?branch=master)](https://travis-ci.org/taskcluster/taskcluster-backup)

It is important to remember that the restore functionality must not
rely on any part of Taskcluster functioning, since it will probably
be used in an event that involves Taskcluster being hard down.
