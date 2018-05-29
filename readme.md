# Edgar-Analytics

### Libraries, Environments, Dependencies

Python 3 without any "exotic" packages

```python
import csv
import os
import datetime
import collections
```

### Input & Output

Read input data and write the output data line by line

```python
# set up relative directories for input and output 
script_dir = os.path.dirname(__file__)
input_dir = os.path.join(script_dir, os.pardir, 'input')
log_dir = os.path.join(input_dir, "log.csv")
inactivity_dir = os.path.join(input_dir, "inactivity_period.txt")
output_dir = os.path.join(script_dir, os.pardir, "output", "sessionization.txt")

# read input file
with open(log_dir, 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    # writing output file
    with open(output_dir, 'w', newline='') as new_file:
```
### Date Structure to solve the challenge
The challenge is about how to handle stateful computation in stream processing with an efficient data structure. I use two ordered dictionaries to store and update the input information.

##### `temp_data` 

- count webpage requests during the session
- update the date and time of the last webpage request in the session
- label the order of the user's first request for that session appeared in the input `log.csv` file.

```python
# <key=ip, value=dict('ip', 'first_dt', 'last_dt', 'duration', 'count', 'rank')>
temp_data = collections.OrderedDict()
```

The key is IP, and the value is also a dictionary with 6 keys. The first 5 keys are required in the output file, while the last one 'rank' is used to mark the order of the user's first request for that session appeared in the input file, which will be removed before writing to output.

##### `temp_time`

* keep and update the last active datetime for each user section, used to identify which session is over and which users in that session should be removed from the system.

```python
# temp_time: <key=datetime, value=list('ip')>
temp_time = collections.OrderedDict()
```

The key is the date and time of last webpage request in that session. The value is a list of IPs, because multiple IPs may have the same date and time of their last webpage request

### Implementation Details

update `temp_data` by updating count and datetime of latest request in that session 

* auxliary variable `time_changed` to help decide if we need to do clean up in the next step

```python
# new ip (new session)
if line["ip"] not in temp_data.keys():
    temp_data[line['ip']] = {'ip': line['ip'], 'first_dt': time, 'last_dt': time, 'duration': 1, 'count': 1, 'rank': order}
# existing ip in that session
else:
    if time != temp_data[line['ip']]['last_dt']:
        time_changed = True
        last_time = temp_data[line['ip']]['last_dt']
    temp_data[line['ip']]['last_dt'] = time
    temp_data[line['ip']]['duration'] = time.second - temp_data[line['ip']]['first_dt'].second + 1
    temp_data[line['ip']]['count'] += 1
```

update `temp_time` by checking inactivity window to see if we need to clean the expired sessions and corresponding IPs, and write them to output file

* we also clean temp_data if we need do cleanup while writing to output
* `exp_keys` and `exp_ips` to temporarily store the expired request datetime and related ips, which will be used to clean up two dictionaries and prepare output data.
* `rank` to decide output order of inactive sessions and store them in `order_ips`, which will be iterated for writing to output.

```python
# init temp_time
if len(temp_time) == 0:
    temp_time[time] = [line['ip']]
else:
    # check if meet the first request from new datetime 
    if time not in temp_time.keys():
        # if yes, check if we need to clean the expired sessions and corresponding IPs. 
        if time_changed and last_time in temp_time.keys():
            temp_time[last_time].remove(line['ip'])
        temp_time[time] = [line['ip']]
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
```

Take care of the remaining elements in dictionaries when reaching the end of reader by writing all the values in `temp_data` into output file by ranking order.

```python
# If reaching the end of input file, append the rest data to output by rank order.
for value in temp_data.values():
    del value['rank']
    csv_writer.writerow(value)
```

### Repo directory structure

```
├── README.md 
├── run.sh
├── src
│   └── sessionization.py
├── input
│   └── inactivity_period.txt
│   └── log.csv
├── output
|   └── sessionization.txt
├── insight_testsuite
    └── run_tests.sh
    └── tests
        └── test_1
        |   ├── input
        |   │   └── inactivity_period.txt
        |   │   └── log.csv
        |   |__ output
        |   │   └── sessionization.txt
        ├── your-own-test_1
            ├── input
            │   └── your-own-inputs
            |── output
                └── sessionization.txt
```

### Run Instructions

 `run.sh` in the top-most directory of my repo will execute the python script as below

```bash
#!/bin/bash
python ./src/sessionization.py
```

You can run `run.sh` with the following command from top-most directory of the repo 

```
edgar-analytics~$ ./run.sh 
```

### Testing

Test1: I changed the inactivity_period and modified the `log.csv` to test my code. It works well under different values of inactivity period and differnt log data. 

Test2: In my own test fold, I changed the value of inactivity period from 2 seconds to 3 seconds and kept the `log.csv` file as same. The result was as same as my expected.

Test3: Moreover, I would like to test my script with the big dataset, so I downloaded [a real EDGAR log file](http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2017/Qtr2/log20170630.zip), `log20170630.zip`, directly from [directly from the SEC](https://www.sec.gov/dera/data/edgar-log-file-data-set.html). I only fetch the first eight hundreds records plus the header as my test input file. 

And I also set a 6 seconds inactivity window in `inactivity_period.txt`

```shell
$ cd edgar-analytics/insight_testsuite/tests/test_2/input
$ ls -l
log.csv inactivity_period.txt
```

And it passed both default testsuite and my own testsuite with a log as below

```
[PASS]: test_1 sessionization.txt
[PASS]: test_2 sessionization.txt
[Mon May 28 23:51:42 PDT 2018] 2 of 2 tests passed
```

### Discussion 

##### If the input has a large number of records in batch mode

I thought about using list or queue to store and update the state of ip and datetime of latest requests. But I gave it up after considering the traverse will take O(n) every time when you see the new datetime from input. Instead I use dictionary(hashmap) to maintain the state of IP and its datetime of last weblog request in that session. 

The good side is that it only takes amorted O(1) to update the state of each ip or datetime of last request. But you have to keep it in the memory all the time and then it has a risk to give you out-of-memory error when the inactivity window is super long and the input stream is super huge. 

Because ips are irrelevant of each other, so we can maintain and update their state independently. So the better solution for large number of input records is to use distributed solution (e.g. Spark, MapReduce) to split different ips to different workers, but sending all the requests of the same ip to the same worker, which can be implemented by the shuffle stage in those solutions. Then we can get rid of 

##### If the input is a realtime stream, and very dense

Flink or Spark Streaming could also be the distributed and realtime solution in this case.

Take Flink as example, the general dataflow should be as follows.

* Define return type as `Tuple2<String, Tuple5<String, String, String, String, String>>`, which is mapping to `<key=ip, value=dict('ip', 'first_dt', 'last_dt', 'duration', 'count')>`

* Monitor the log file and only read if seeing new records appended to the file under `PROCESS_ONLY_APPENDED` mode

* Use `keyBy(ip)` to set up the dataflow for each ip

* Use `EventTimeSessionWindow` to set up the inactivity window, so it will automatically write inactive session to output after the session is over.

```java
DataStream<Tuple2<String, Tuple5<String, String, String, String, String>>> dataStream =
    env.readFileStream(path, Time.seconds(1).toMilliseconds(), WatchType.PROCESS_ONLY_APPENDED)
        // extract selected fields
        .map(new ExtractFields())
    	// use EventTime (available in data) as timestamp 
        .assignTimestamps(new AccessLogTimestampsGenerator())
		// extract <key=ip, value=dict('ip', 'first_dt', 'last_dt', 'duration', 'count')>
        .map(new ExtractKeyValue())
        // "0" means first field, which is IP, 
    	// then it will split the dataflow to different workers by IP
        .keyBy(0)
		// set the inactivity window based on EventTime
    	// trigger the aggregation when session is over
        .window(EventTimeSessionWindow.of(Time.hours(inactivity_window)))
        // sum column count for each IP
        .sum('count');
// write aggregation result ot output via OUTPUTSink Function
dataStream.addSink(new OUTPUTSink());
```











