#!/bin/bash

log_file='logs'
output_file='output.csv'

# Remove output file if it exists
if [ -e $output_file ]; then
    rm $output_file
fi

# Extract total_batches and total_packets fields from each log line
while read -r line; do
    if [[ $line == *"total_batches="* && $line == *"total_packets="* ]]; then
        total_batches=$(echo "$line" | grep -o 'total_batches=[^ ]*' | cut -d'=' -f2 | tr -d '[:alpha:]')
        total_packets=$(echo "$line" | grep -o 'total_packets=[^ ]*' | cut -d'=' -f2 | tr -d '[:alpha:]')
        echo "$total_batches,$total_packets" >> $output_file
    fi
done < $log_file
