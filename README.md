SUMMARY
=======
This project allows to copy data from MySQL tables to Cassandra and to keep the consistency. 
Obviuosly, moving SQL schemas to noSQL has its drawbacks due to the schema design nature, so I don't recommend this solution unless it is an emergency. 
Useful tool where you need to move this data from databases quickly as a temp solution.
Code needs some cleaning and doc.
Runs as a daemon.

AUTHOR
======
luis.martin.gil@indigital.net
INdigital Telecom. 2012

TAGS
====
mysql, cassandra, java, hector

Instructions
============
Some 'dont forget' commands related to this project:
; Clean the project
mvn clean

; Build the jar file
mvn -Dinstall install assembly:single

; Execute the project
java  -Dlog4j.logFile=mysql2cass.log -jar target/mysql2cass-0.0.1-SNAPSHOT-jar-with-dependencies.jar config.example.xml INFO

; From cassandra client, we can get elements like this:
[default@KEYSPACE] GET COLUMN_FAMILY[int(KEY)]['NAME'];
