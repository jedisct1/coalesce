
Coalesce
========

Fixed-size files hierarchically organized by date is a common way to
store logs in HDFS.

When processing these data, the number of files they are split into
plays a significant role. Recursively traversing directories in HDFS
and accessing individual files can make Spark and MapReduce jobs
significantly slower than the same data split across less, but larger
files.

Coalesce is a simple maintenance tool to merge directories into files.

```
/q/2014/01/14/130528/1923
/q/2014/01/14/130528/1924
/q/2014/01/14/130528/1925   
/q/2014/01/14/130528/1926 
/q/2014/01/14/742290/48             /q/2014/01/14/130528
/q/2014/01/14/742290/49    =====>   /q/2014/01/14/742290
/q/2014/01/14/742290/50             /q/2014/01/15/123114
/q/2014/01/15/123114/01
/q/2014/01/15/123114/02
/q/2014/01/15/123114/03
```

Coalesce traverses the filesystem, finds directories that contain
nothing but files and are at least `<minimum depth>` steps away from
the root, and replaces each matching directory with a file. The
content of that file is the concatenation of the files previously
stored in the directory.

A path given to Pig or Spark can be either a file or a directory,
whose content will be transparently merged. In the previous example,
`/q/2014/01/14/130528` was initially a directory, and became a single file
after running Coalesce. Loading `/q/2014/01/14/130528` using Pig or Spark
will load the same data set in both cases.

Usage
=====
```
$ sbt 'run <minimum depth> <root path>'
```

Example:
```
$ sbt 'run 3 hdfs://nn.example.com/querylog/2015'

The tool only merges the content of directories that are more than
`<minimum depth>` steps away from the root. For example, with
`<minimum depth>` set to `3`, and starting from `/querylog/2015`:

- `/querylog/2015/02/11/03/12/*` will be reduced to `/querylog/2015/02/11/03/12`
- `/querylog/2015/02/11/03/*` will be reduced to `/querylog/2015/02/11/03`
- `/querylog/2015/02/11/*` will not be altered.

Coalesce is trivial and the code is really dumb, but I couldn't find any
existing tools to do the job.
