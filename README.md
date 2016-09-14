# SimpleDB Database System

This project is the lab assignments of [MIT Open Course 6.830](http://ocw.mit.edu/courses/electrical-engineering-and-computer-science/6-830-database-systems-fall-2010/assignments/): the implementation of a tiny database management system using the given code skeleton.


## What I Have Done

Up to now, I have accomplished 4 out of the 5 lab assignments, and a naive RDBMS has come into being.
* By doing [Lab 1](http://db.csail.mit.edu/6.830/assignments/lab1.html), I implemented the core modules of the storage system.
* By doing [Lab 2](http://db.csail.mit.edu/6.830/assignments/lab2.html), I added support for various query processing operators and consummated the storage system.
* By doing [Lab 3](http://db.csail.mit.edu/6.830/assignments/lab3.html), I added the concurrency control by implementing strict 2PL and NO STEAL/FORCE policy.
* By doing [Lab 5](http://db.csail.mit.edu/6.830/assignments/lab5.html), I implemented log-based rollback for aborts and log-based crash recovery.

The four lab assignments above were finished in my spare time within one week.
Actually, a lot of time was spent on concurrency debugging and deadlock handling.

Due to a tight schedule, I am unable to finish Lab 4 (query optimization) for the time being.
I hope this could be done in the future.

## Build and Run Tests

To create the executable files, simply type "ant" in the project's root directory.

Type "ant test" to execute all the unit tests provided by the course staff.

Type "ant systemtest" to execute all their system tests.

Type "ant clean" to clean all executable files.

Type "ant dist" to create the JAR file dist/simpledb.jar.

To load a schema file and start the interpreter, use the following command:

	java -jar dist/simpledb.jar parser dblp_data/dblp_simpledb.schema

