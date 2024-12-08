#!/bin/bash

grep -r "Throughput"
grep -r "Throughput =" . | awk -F '= ' '{sum += $2;} END {print "Aggregate Throughput is: " sum}'
grep -r "Average read latency:" . | awk -F ': ' 'max == "" || $2 > max {max = $2} END {print "Average read latency across clients: " max}'
grep -r "Average write latency:" . | awk -F ': ' 'max == "" || $2 > max {max = $2} END {print "Average write latency across clients: " max}'  
grep -r "No. of read failures:" . | awk -F ': ' '{sum += $2} END {print "Total number of Read failures: " sum}'
grep -r "No. of write failures:" . | awk -F ': ' '{sum += $2} END {print "Total number of Write failures: " sum}'

# Directory containing the files
folder_path=$(pwd)

# Initialize variables
declare -A max_latency
declare -A total_read_throughput
declare -A total_write_throughput
aggregate_final_throughput=0

# Percentile labels to process
percentiles=("50th" "70th" "90th" "99th")

# Initialize max latency and throughput sums for each percentile
for p in "${percentiles[@]}"; do
    max_latency["read_$p"]=0
    max_latency["write_$p"]=0
    total_read_throughput["$p"]=0
    total_write_throughput["$p"]=0
done

# Process each file in the folder
for file in "$folder_path"/client_*; do
    if [[ -f "$file" ]]; then
        while IFS= read -r line; do
            # Extract final throughput
            if [[ $line =~ Throughput\ =\ ([0-9.]+) ]]; then
                aggregate_final_throughput=$(awk "BEGIN {print $aggregate_final_throughput + ${BASH_REMATCH[1]}}")
            fi

            # Process read latencies and throughputs
            if [[ $line =~ ([0-9]+)th\ percentile\ read\ latency:\ ([0-9]+),\ throughput:\ ([0-9.]+) ]]; then
                percentile="${BASH_REMATCH[1]}th"
                latency="${BASH_REMATCH[2]}"
                throughput="${BASH_REMATCH[3]}"
                max_latency["read_$percentile"]=$(awk "BEGIN {print ($latency > ${max_latency["read_$percentile"]} ? $latency : ${max_latency["read_$percentile"]})}")
                total_read_throughput["$percentile"]=$(awk "BEGIN {print ${total_read_throughput["$percentile"]} + $throughput}")
            fi

            # Process write latencies and throughputs
            if [[ $line =~ ([0-9]+)th\ percentile\ write\ latency:\ ([0-9]+),\ throughput:\ ([0-9.]+) ]]; then
                percentile="${BASH_REMATCH[1]}th"
                latency="${BASH_REMATCH[2]}"
                throughput="${BASH_REMATCH[3]}"
                max_latency["write_$percentile"]=$(awk "BEGIN {print ($latency > ${max_latency["write_$percentile"]} ? $latency : ${max_latency["write_$percentile"]})}")
                total_write_throughput["$percentile"]=$(awk "BEGIN {print ${total_write_throughput["$percentile"]} + $throughput}")
            fi
        done < "$file"
    fi
done

# Output results
echo -e "\nAggregate Final Throughput: $aggregate_final_throughput"

# Maximum latencies
echo -e "\nMaximum Latencies:"
for p in "${percentiles[@]}"; do
    echo "$p percentile read latency: ${max_latency["read_$p"]}"
    echo "$p percentile write latency: ${max_latency["write_$p"]}"
done

# Aggregate read and write throughputs for each percentile
echo -e "\nAggregate Read Throughputs:"
for p in "${percentiles[@]}"; do
    echo "$p percentile: ${total_read_throughput["$p"]}"
done

echo -e "\nAggregate Write Throughputs:"
for p in "${percentiles[@]}"; do
    echo "$p percentile: ${total_write_throughput["$p"]}"
done

