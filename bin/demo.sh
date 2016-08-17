#!/bin/bash

WORKLOAD="workloads/demowork"

echo "*****************"
echo "*** Agar DEMO ***"
echo "*****************"

echo "Workload: $WORKLOAD"
echo "Regions: `cat workloads/demowork | grep regions | awk -F '[=]' '{print $2}'`"
echo "Buckets: `cat workloads/demowork | grep buckets | awk -F '[=]' '{print $2}'`"
echo "Data item size: `cat workloads/demowork | grep fieldlength | awk -F '[=]' '{print $2}'` bytes"
echo "Number of data items: `cat workloads/demowork | grep recordcount | awk -F '[=]' '{print $2}'`"
echo "Number of read operations: `cat workloads/demowork | grep operationcount | awk -F '[=]' '{print $2}'`"
echo "*****************"

echo "1. Dependencies..."
echo "* java 1.8"
echo "* maven 3.x"
echo "* memcached 1.4"
echo "* gnome-terminal"
echo "*****************"

echo "2. Building..."
mvn clean package #&> /dev/null
echo "*****************"

echo "3. Loading backend..."
bin/ycsb load backend -demo -P $WORKLOAD 1> /dev/null
echo "*****************"

echo "4. Start one memcached server"
gnome-terminal --tab -t Memcached -e "memcached -p 11211 -m 6 -I 115k -vv"
echo "*****************"

echo "5. Starting Agar proxy..."
gnome-terminal --tab -t AgarProxy -e "bin/ycsb proxy -cachemanager agar -demo -P $WORKLOAD" 1> /dev/null
sleep 10s
echo "*****************"

echo "6. Starting Agar client..."
./bin/ycsb run agar -P $WORKLOAD -demo

exit 0
