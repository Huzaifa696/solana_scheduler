import re
import os

# remove the output file if it exists
if os.path.exists('output.csv'):
    os.remove('output.csv')


# open the log file for reading
with open('logs', 'r') as f:
    # read the file contents into a string
    log_contents = f.read()

# use a regular expression to extract the numbers after "scheduler loop time"
numbers = re.findall(
    r'scheduler loop time (\d+), count (\d+), packet_reception (\d+), working_bank (\d+), retry_and_garbage_collection (\d+), batch_processing (\d+), tx_counter (\d+), tx_recv_counter (\d+), not_leader_counter (\d+), vote_counter (\d+), empty_buffer (\d+)', log_contents)

# join the numbers into a comma-separated string with a newline character between each line
csv_string = '\n'.join(','.join(group) for group in numbers)

# open a file for writing in text mode with newline='' to handle newlines explicitly, and write the CSV string
with open('output.csv', 'w', newline='') as f:
    f.write(csv_string)
