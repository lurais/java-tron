#!/bin/bash
MAX_STOP_TIME=600
APP_NAME='FullNode'

checkPid() {
  pid=$(ps -ef | grep -v start | grep $APP_NAME | grep -v grep | awk '{print $2}')
}

restartService() {
  count=1
  while [ $count -le $MAX_STOP_TIME ]; do
    checkPid
    if [ $pid ]; then
      sleep 1
    else
      nohup java -Xms9G -Xmx9G -XX:ReservedCodeCacheSize=256m \
            -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m \
            -XX:MaxDirectMemorySize=1G -XX:+PrintGCDetails \
            -XX:+PrintGCDateStamps  -Xloggc:gc.log \
            -XX:+UseConcMarkSweepGC -XX:NewRatio=2 \
            -XX:+CMSScavengeBeforeRemark -XX:+ParallelRefProcEnabled \
            -XX:+HeapDumpOnOutOfMemoryError \
            -XX:+UseCMSInitiatingOccupancyOnly  -XX:CMSInitiatingOccupancyFraction=70 \
            -jar FullNode.jar -c main_net_config.conf --net-init false >> start.log 2>&1 &
      return
    fi
    count=$(($count + 1))
  done
}

restartService