import os

# remove the output file if it exists
# if os.path.exists('output.csv'):
#     os.remove('output.csv')

# import re

# # open the log file for reading
# with open('logs', 'r') as f:
#     # read the file contents into a string
#     log_contents = f.read()

# # use a regular expression to extract the numbers after "scheduler loop time"
# numbers = re.findall(r'scheduler loop time (\d+), count (\d+), packet_reception (\d+), working_bank (\d+), retry_and_garbage_collection (\d+), batch_processing (\d+), tx_counter (\d+)', log_contents)

# # join the numbers into a comma-separated string with a newline character between each line
# csv_string = '\n'.join(','.join(group) for group in numbers)

# # open a file for writing in text mode with newline='' to handle newlines explicitly, and write the CSV string
# with open('output.csv', 'w', newline='') as f:
#     f.write(csv_string)

import re

log_file = 'logs'
output_file = 'output.csv'

# Remove output file if it exists
# import os
# if os.path.exists(output_file):
#     os.remove(output_file)

# Define regular expression for parsing log lines
pattern = r'^\[(.*)\].*datapoint:.*recv_batches_us_90pct=(\d+)i.*verify_batches_pp_us_90pct=(\d+)i.*discard_packets_pp_us_90pct=(\d+)i.*dedup_packets_pp_us_90pct=(\d+)i.*batches_90pct=(\d+)i.*packets_90pct=(\d+)i.*num_deduper_saturations=(\d+)i.*total_batches=(\d+)i.*total_packets=(\d+)i.*total_dedup=(\d+)i.*total_excess_fail=(\d+)i.*total_valid_packets=(\d+)i.*total_discard_random=(\d+)i.*total_shrinks=(\d+)i.*total_dedup_time_us=(\d+)i.*total_discard_time_us=(\d+)i.*total_discard_random_time_us=(\d+)i.*total_verify_time_us=(\d+)i.*total_shrink_time_us=(\d+)i'

with open(log_file, 'r') as f:
    for line in f:
        match = re.match(pattern, line)
        if match:
            print(f"Found matching line: {line}")
            timestamp = match.group(1)
            recv_batches_us_90pct = match.group(2)
            verify_batches_pp_us_90pct = match.group(3)
            discard_packets_pp_us_90pct = match.group(4)
            dedup_packets_pp_us_90pct = match.group(5)
            batches_90pct = match.group(6)
            packets_90pct = match.group(7)
            num_deduper_saturations = match.group(8)
            total_batches = match.group(9)
            total_packets = match.group(10)
            total_dedup = match.group(11)
            total_excess_fail = match.group(12)
            total_valid_packets = match.group(13)
            total_discard_random = match.group(14)
            total_shrinks = match.group(15)
            total_dedup_time_us = match.group(16)
            total_discard_time_us = match.group(17)
            total_discard_random_time_us = match.group(18)
            total_verify_time_us = match.group(19)
            total_shrink_time_us = match.group(20)
            
            # Write data to output file
            with open(output_file, 'a') as out:
                out.write(f"{timestamp},{total_batches},{total_packets}\n")
        else:
            print(f"No match found for line: {line}")


