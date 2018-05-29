import csv
import os
import datetime
import collections

# set up the input and output dirs
script_dir = os.path.dirname(__file__)
input_dir = os.path.join(script_dir, os.pardir, 'input')
log_dir = os.path.join(input_dir, "log.csv")
inactivity_dir = os.path.join(input_dir, "inactivity_period.txt")
output_dir = os.path.join(script_dir, os.pardir, "output", "sessionization.txt")

# inactivity window (in seconds) to identify a user session
with open(inactivity_dir, 'r') as read_file:
    inactivity = int(float(read_file.read()))
    tdelta = datetime.timedelta(seconds=inactivity)

# read input file
with open(log_dir, 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)

    # temp_data: <key=ip, value=dict('ip', 'first_dt', 'last_dt', 'duration', 'count', 'rank')>
    # 'rank' is the order in the input streaming data
    temp_data = collections.OrderedDict()

    # temp_time: <key=datetime, value=list('ip')>
    temp_time = collections.OrderedDict()
    # writing the output file
    with open(output_dir, 'w', newline='') as new_file:
        fieldnames = ['ip', 'first_dt', 'last_dt', 'duration', 'count']
        csv_writer = csv.DictWriter(new_file, fieldnames=fieldnames, delimiter=",")
        # initialize rank
        order = 0

        # read the input data line by line
        for line in csv_reader:
            # auto incl
            order += 1
            # Set the bool value of time changed
            # false if the current time is equal to the previous time, this value is, true otherwise
            time_changed = False
            # Transfer the input date & time into datetime object for future use
            time_str = line['date'] + " " + line['time']
            time = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

            # If current IP is not in the dictionary, add it to dict.
            if line["ip"] not in temp_data.keys():
                temp_data[line['ip']] = {'ip': line['ip'], 'first_dt': time, 'last_dt': time, 'duration': 1, 'count': 1, 'rank': order}
            # Otherwise, update temp_data.
            else:
                # If the time is not as same as the previous one, then updating the last_dt and setting time_changed true.
                if time != temp_data[line['ip']]['last_dt']:
                    time_changed = True
                    last_time = temp_data[line['ip']]['last_dt']
                temp_data[line['ip']]['last_dt'] = time
                temp_data[line['ip']]['duration'] = time.second - temp_data[line['ip']]['first_dt'].second + 1
                temp_data[line['ip']]['count'] += 1

            # If temp_time is empty, add the current time into temp_time
            if len(temp_time) == 0:
                temp_time[time] = [line['ip']]
            # Otherwise, update the temp_time and temp_data
            else:
                if time not in temp_time.keys():
                    # If time_changed is true, move the current IP from the previous time to the current time.
                    if time_changed and last_time in temp_time.keys():
                        temp_time[last_time].remove(line['ip'])
                    temp_time[time] = [line['ip']]

                    # Check if the times in temp_time are expired.
                    exp_keys = []
                    exp_ips = []
                    for key, value in temp_time.items():
                        if time - key > tdelta:
                            # Add the expired time and relative IP into lists.
                            exp_keys.append(key)
                            exp_ips.extend(value)
                    # Remove the expired time from temp_time
                    for key in exp_keys:
                        temp_time.pop(key, None)

                    # order the exp_ips list by rank
                    order_ips = sorted(exp_ips, key=lambda k: temp_data[k]['rank'])

                    # Remove the expired IP from temp_data and write to the output file
                    for ip in order_ips:
                        write_data = temp_data.pop(ip, None)
                        # delete the unwanted column
                        del write_data['rank']
                        csv_writer.writerow(write_data)
                else:
                    if time_changed and last_time in temp_time.keys():
                        temp_time[last_time].remove(line['ip'])
                    if line['ip'] not in temp_time[time]:
                        temp_time[time].append(line['ip'])
        # If reaching the end of input file, append the rest data to output by rank order.
        for value in temp_data.values():
            del value['rank']
            csv_writer.writerow(value)