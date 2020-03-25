#!/bin/bash
# 自动生成日志
function create_kafka_topic {
    $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 1 --topic $1
}

function send_messages_to_kafka {
    msg=$(generator_message)
    echo -e $msg | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list master:9092 --topic $TOPIC
}

function rand {
    min=$1
    max=$(($2-$min+1))
    num=$(date +%s%N)
    echo $(($num%$max+$min))
}

function generator_message {
    userId=$(rand 1 100);
    articleId=$(rand 1 10);
    timestamp=`date '+%s'`;
    action=1;
    msg=$userId","$articleId","$timestamp","$action;
    echo $msg
}

TOPIC="log"
create_kafka_topic $TOPIC
while true
do
 send_messages_to_kafka
 sleep 0.1
done

