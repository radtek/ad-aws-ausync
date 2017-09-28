#!/bin/sh

SERVER_HOME="/home/upsmart/works/projects/adserver/ad-ausync/ad-ausync-server"

JAR_PATH="$SERVER_HOME/target/ad-ausync-server-1.0.0-SNAPSHOT.jar"

PROPERTIES_PATH="$SERVER_HOME/properties/local/"

JAVA_OPS="-server
          -Xms512m
          -Xmx512m
          -XX:PermSize=128m
          -XX:MaxPermSize=128m
          -XX:+UseParNewGC
          -XX:+CMSParallelRemarkEnabled
          -XX:+UseConcMarkSweepGC
          -XX:+PrintGCDetails
          -XX:+PrintGCDateStamps
          -XX:+HeapDumpOnOutOfMemoryError
          -XX:HeapDumpPath=$SERVER_HOME/runtime-logs
          -Xloggc:$SERVER_HOME/runtime-logs/gc.log"

RETVAL=$?
PID=$2
PID_FILE=$SERVER_HOME/pid.log
 case "$1" in
 start)
 if [ -f $JAR_PATH ];then
    echo "Start Application."
    mkdir -p $SERVER_HOME/runtime-logs
    nohup java $JAVA_OPS -jar $JAR_PATH $PROPERTIES_PATH >/dev/null 2>&1 &
    echo $! > $PID_FILE
    #java $JAVA_OPS -jar $JAR_PATH $PROPERTIES_PATH
 fi
 ;;
 stop)
 if [ -f $JAR_PATH ];then
    if [ -f $PID_FILE ];then
        PID=`cat $PID_FILE`
        if kill -12 $PID ;then
            echo "$PID","Application Stopped."
        else
            echo "$PID","Application could not be Stopped."
        fi
        rm -f $PID_FILE
    else
        if kill -12 $PID ;then
            echo "$PID","Application Stopped."
        else
            echo "$PID","Application could not be Stopped."
        fi
    fi
 fi
 ;;
 *)
 echo "Usage: $0 {start|stop}"
 exit 1
 ;;
 esac
 exit $RETVAL
