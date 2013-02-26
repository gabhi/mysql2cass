SUMMARY
=======
This project allows to copy data from MySQL tables to Cassandra and to keep the consistency. Useful tool where you need to move this data ASAP as a temp solution. Obviuosly, moving SQL schemas to noSQL has its drawbacks due to the schema design nature, so I don't recommend this solution unless it is an emergency. Runs as a daemon.


AUTHOR
======
luis.martin.gil@indigital.net
INdigital Telecom. 2012


Some 'dont forget' commands related to this project:
; Clean the project
mvn clean

; Build the jar file
mvn -Dinstall install assembly:single

; Execute the project
java  -Dlog4j.logFile=mysql2cass.log -jar target/mysql2cass-0.0.1-SNAPSHOT-jar-with-dependencies.jar config.example.xml INFO

; From cassandra client, we can get elements like this:
[default@KEYSPACE] GET COLUMN_FAMILY[int(KEY)]['NAME'];
