#!/usr/bin/env bash



spark-submit item_heat_score.py --master yarn --name item_heat_score_lzm \
--deploy-mode client \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.app.name=liuzimo\
--conf spark.driver.cores=4\
--conf spark.driver.memory=10g\
--conf spark.executor.memory=10g\
--conf spark.executor.cores=4\
--conf spark.executor.instances=100\
--conf spark.yarn.executor.memoryOverhead=4096\
--conf spark.sql.shuffle.partitions=200\
--conf spark.rpc.message.maxSize=2046\
--conf spark.network.timeout=1200s\
--conf spark.default.parallelism=1000\
--conf spark.driver.maxResultSize=6g\
--conf spark.shuffle.manager=SORT\
--files hive-site.xml

