# unicron

A Clojure library designed to watch for new files.

Basic Unicron flow:
 * Watch S3 for new files (implement on top of dwd)
 * Possibly download new files
 * Execute some command, e.g.
   * Schema resolve + project + load to Vertica (for Humperdink)

User caveats when designing file keys:
 * Your filepath/s3-key must have timestamp reflecting the file's
   relative position in the overall sequence of files.
 * You must have read-after-write consistency on the system you're
   loading to, so your uploads don't appear in a different order than
   you wrote them to.
 * The date-expr that represents your filepath must have increasingly
   fine granularity of conversion-specs as you read it left-to-right.

## High level remarks

Watcher -- implemented in DWD
 - S3
 - local/NFS (inotify)
 - SFTP

Watcher pulls; Unicron is a cronned push-wrapper around Watcher.

## Implementation notes

Use dwd to watch for new files. Always remember the "newest" file
you've seen, then ask for everything newer than that.

## Usage

Deploy via dancible. Must specify the CFG_DIR token, i.e. the
directory where unicron should look to find its configuration.

In the CFG_DIR, must have a feeds.clj file and a history.yml file. See
doc/examples for examples!

## License

Copyright © 2014 One Kings Lane

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
