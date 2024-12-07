grep -r "Throughput"
grep -r "Throughput =" . | awk -F '= ' '{sum += $2;} END {print "Aggregate Throughput is: " sum}'
grep -r "No. of read failures:" . | awk -F ': ' '{sum += $2} END {print "Total number of Read failures: " sum}'
grep -r "No. of write failures:" . | awk -F ': ' '{sum += $2} END {print "Total number of Write failures: " sum}'
