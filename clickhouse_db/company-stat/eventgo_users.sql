CREATE TABLE `company-stat`.eventgo_users\n(\n    `user_id` UInt32,\n    `msisdn` String,\n    `reg_date` Date\n)\nENGINE = MergeTree\nORDER BY user_id\nSETTINGS index_granularity = 8192
