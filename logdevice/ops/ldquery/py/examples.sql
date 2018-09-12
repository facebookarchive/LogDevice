-- Copyright (c) Facebook, Inc. and its affiliates.
-- All rights reserved.
--
-- This source code is licensed under the BSD-style license found in the
-- LICENSE file in the root directory of this source tree.

-- In order to test these queries:
-- cat logdevice/fb/ldquery/py/examples.sql | -- _bin/logdevice/fb/ldquery/py/ldquery-cli.lpar -t logdevice.test.frc1b

-- Show the first 5 log groups
SELECT * FROM log_groups LIMIT 0, 5;

-- Show the first 5 sockets
SELECT * FROM sockets LIMIT 0, 5;

-- Show the first 5 readers
SELECT * FROM readers LIMIT 0, 5;

-- Find the top 5 log groups that have had the highest throughput in the last 10 minutes.
-- Also show the logid range of each log group.
SELECT log_groups.name, sum(append_throughput.throughput_10min) AS total_throughput, log_groups.logid_lo, log_groups.logid_hi
FROM append_throughput
LEFT JOIN log_groups ON log_groups.name = append_throughput.log_group_name 
GROUP BY log_group_name
ORDER BY total_throughput DESC
LIMIT 0, 5;

-- Find the top 5 log groups with the most readers
SELECT count(*) as num_readers, log_id, log_groups.name FROM readers LEFT JOIN log_groups ON readers.log_id >=
log_groups.logid_lo AND readers.log_id <= log_groups.logid_hi  group by log_id ORDER BY num_readers desc LIMIT 0, 5;

-- List readers with their associated socket
SELECT log_id, lsn_to_string(until_lsn) as until_lsn, lsn_to_string(read_pointer) as read_pointer,
       sockets.read_cnt, sockets.write_cnt
FROM readers
LEFT JOIN sockets ON readers.client = sockets.name
LIMIT 0, 5;

-- top 5 partitions by size
SELECT node_id, shard, id, min_time, max_time, approx_size / 1024.0 / 1024.0 as size_mb
FROM partitions
ORDER BY approx_size DESC
LIMIT 0, 5;

-- Count the number of log groups that haven't had any throughput in the last 10 mins
SELECT count(log_groups.name) as num_log_groups_no_throughput
FROM log_groups
LEFT JOIN append_throughput ON log_groups.name = append_throughput.log_group_name
WHERE throughput_10min IS NULL;

-- List of read streams that are rebuilding
SELECT * FROM readers WHERE required_in_copyset IS NOT NULL LIMIT 0, 5;
