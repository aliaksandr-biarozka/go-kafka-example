#!/bin/sh

for topic in "$@"; do
    echo "Creating topic: $topic"
    kafka-topics --create --topic "$topic" --bootstrap-server localhost:9092
done

echo "Topic list:"
kafka-topics --bootstrap-server localhost:9092 --list

