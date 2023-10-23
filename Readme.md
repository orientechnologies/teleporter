

[![REUSE status](https://api.reuse.software/badge/github.com/orientechnologies/teleporter)](https://api.reuse.software/info/github.com/orientechnologies/teleporter)

OrientDB Teleporter is a tool that synchronizes a RDBMS to an OrientDB database.
You can use Teleporter to:

- Import your existing RDBMS to OrientDB.
- Keep your OrientDB database synchronized with changes from the RDBMS. In this case the database
  on your RDBMS remains the primary and the database on OrientDB a synchronized copy. 
  Synchronization is one way, so all the changes in OrientDB database will not be propagated to
  the RDBMS.


You can execute Teleporter via OrientDB Studio as described here:
https://github.com/orientechnologies/orientdb-labs/blob/master/Studio-Teleporter.md

or just run the tool through the script as described in the documentation:
https://github.com/orientechnologies/orientdb-labs/blob/master/Teleporter-Index.md.
