#!/bin/sh

for topic in "$@"; do
    echo "Deleting topic: $topic"
    kafka-topics --delete --topic "$topic" --bootstrap-server localhost:9092
done

echo "Topic list:"
kafka-topics --bootstrap-server localhost:9092 --list

