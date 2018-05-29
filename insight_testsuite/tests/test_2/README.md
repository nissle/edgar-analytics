Test3: Moreover, I would like to test my script with the big dataset, so I downloaded a real EDGAR log file, log20170630.zip, directly from directly from the SEC. I only fetch the first eight hundreds records plus the header as my test input file. 

And I also set a 6 seconds inactivity window in inactivity_period.txt
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
