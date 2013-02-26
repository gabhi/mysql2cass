#!/bin/bash

# lmartin.
PATH=/sbin:/bin:/usr/sbin:/usr/bin:${JAVA_HOME}/bin
JAVA=/usr/bin/java
NAME=mysql2cass
LOG_FILE=/var/log/mysql2cass.log
CONFIG_FILE=/etc/mysql2cass/config.xml
JAR_FILE=/usr/src/mysql2cass-maven-git/target/mysql2cass-0.0.1-SNAPSHOT-jar-with-dependencies.jar
DEBUG_LEVEL=INFO
PID_NAME=${NAME}

function get_pid {
    PID=`ps -eo pid,args | grep "$JAR_FILE" | \grep -v grep | cut -c1-6`
    echo $PID
}

# Attempt to locate JAVA_HOME, code borrowed from jabref package
if [ -z $JAVA_HOME ]
then
	t=/usr/lib/jvm/java-1.5.0-sun && test -d $t && JAVA_HOME=$t
	t=/usr/lib/jvm/java-6-sun && test -d $t && JAVA_HOME=$t
	t=/usr/lib/jvm/java-6-openjdk && test -d $t && JAVA_HOME=$t
fi

test -x $JAVA || exit 0

DAEMON_OPTS="$DAEMON_OPTS -Dlog4j.logFile=${LOG_FILE} -jar ${JAR_FILE} ${CONFIG_FILE} ${DEBUG_LEVEL}" 


#Helper functions
function start() {
    get_pid
    if [ -z $PID ]; then
	echo  "Starting mysql2cass..."
	start-stop-daemon --start --quiet --background --make-pidfile --pidfile /var/run/$PID_NAME.pid --chuid root:root --exec $JAVA -- $DAEMON_OPTS
	get_pid
	echo "Done. PID=$PID"
    else
	echo "mysql2cass is already running, PID=$PID"
    fi
}

function stop() {
    get_pid
    if [ -z $PID ]; then
	echo "mysql2cass is not running."
	exit 1
    else
	echo -n "Stopping mysql2cass..."
	echo "PID="$PID
	start-stop-daemon --stop --quiet --pidfile /var/run/$PID_NAME.pid --exec $JAVA --retry 4
	sudo kill $PID
	sleep 1
	echo ".. Done."
    fi    
}

function restart {
   echo  "Restarting mysql2cass..."
   get_pid
   if [ -z $PID ]; then
      start
   else
      stop
      start
   fi
}

function status {
   get_pid
   if [ -z  $PID ]; then
      echo "mysql2cass is not running."
      exit 1
   else
      echo "mysql2cass is running, PID=$PID"
   fi
}

case "$1" in
   start)
      start
   ;;
   stop)
      stop
   ;;
   restart)
      restart
   ;;
   status)
      status
   ;;
   *)
      echo "Usage: $0 {start|stop|restart|status}"
esac


exit 0

