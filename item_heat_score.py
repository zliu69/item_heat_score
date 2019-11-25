# %%
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions
import redis
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == '__main__':
    conf = SparkConf().setAll([\
        # ('spark.master', 'yarn'), ('spark.app.name', 'liuzimo'),\
        # ('spark.driver.cores', '4'), ('spark.driver.memory', '10g'), ('spark.executor.memory', '10g'),\
        # ('spark.executor.cores', '4'), ('spark.executor.instances', '100'), ('spark.yarn.executor.memoryOverhead', '4096'),\
        # ('spark.sql.shuffle.partitions', '200'), ('spark.rpc.message.maxSize', '2046'), ('spark.network.timeout', '1200s'),\
        # ('spark.default.parallelism', '1000'), ('spark.driver.maxResultSize', '6g'), ('spark.shuffle.manager', 'SORT'),\

        ("hive.metastore.uris","thrift://10.23.240.71:9083,thrift://10.23.240.72:9083,thrift://10.23.240.58:9083,""thrift://10.23.240.59:9083"), \
        ("datanucleus.schema.autoCreateAll", "false"), \
        ("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver"), \
        ("javax.jdo.option.ConnectionUserName", "hive") ,\
        ("hive.server2.thrift.port", "10001") ,\
        ("hive.metastore.execute.setugi", "true"), \
        ("hive.metastore.warehouse.dir", "hdfs://ns1/user/hive/warehouse"), \
        ("hive.security.authorization.enabled", "false") ,\
        ("hive.metastore.client.socket.timeout", "60s") ,\
        ("hive.metastore.sasl.enabled", "true") ,\
        ("hive.server2.authentication", "KERBEROS") ,\
        ("hive.server2.authentication.kerberos.principal","hive/hive-metastore-240-58.hadoop.lq@HADOOP.LQ2"), \
        ("hive.server2.authentication.kerberos.keytab","/data/sysdir/hadoop/etc/hadoop/hive.keytab"), \
        ("hive.server2.enable.impersonation", "true"), \
        ("hive.metastore.kerberos.keytab.file","/data/sysdir/hadoop/etc/hadoop/hive.keytab") ,\
        ("hive.metastore.kerberos.principal","hive/hive-metastore-240-58.hadoop.lq@HADOOP.LQ2"), \

            ])

    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().enableHiveSupport().getOrCreate()

    # %% md

    # 取前一天数据并筛选特征cmp_gdm.cmp_gdm_fetr_rcmd_item_di

    # %%


    sql_from_cmp_gdm_item = ("""select * from cmp_gdm.cmp_gdm_fetr_rcmd_item_di where datediff(date_sub(current_date,2),dt)<1""")
    df_gdm_item = spark.sql(sql_from_cmp_gdm_item)
    df_gdm_item = df_gdm_item.repartition(200)
    #
    df_gdm_item = df_gdm_item.select('biz_id', 'biz_type',\
                                     'reply_cnt_1d', \
                                     'reply_cnt_3d', \
                                     'reply_cnt_7d', \
                                     'reply_cnt_15d', \
                                     #                     'reply_cnt_30d',
                                     'collect_cnt_1d', \
                                     'collect_cnt_3d', \
                                     'collect_cnt_7d', \
                                     'collect_cnt_15d', \
                                     #                     'collect_cnt_30d',
                                     'like_cnt_1d', \
                                     'like_cnt_7d', \
                                     'like_cnt_15d', \
                                     #                     'like_cnt_30d',
                                     'dt')
    df_gdm_item = df_gdm_item.withColumn('item_id', functions.concat(functions.lit("lzm-"), functions.col("biz_type"), functions.lit("-"), functions.col("biz_id")))
    #
    # # %%
    #
    df_gdm_item = df_gdm_item.fillna('0')
    #
    # # %%
    #
    #
    #
    # # %%
    #
    # # 根据各个特征平均数权重计算历史求和权重
    #
    # # %%
    #
    #
    df_reply = df_gdm_item.describe('reply_cnt_1d', 'reply_cnt_3d', 'reply_cnt_7d', 'reply_cnt_15d')
    reply_w_dict = {'1d': 1.0, '3d': 0, '7d': 0, '15d': 0}
    reply_list = [0.0] * 4
    for i in range(4):
        reply_list[i] = float(df_reply.filter(df_reply.summary == 'mean').collect()[0][i + 1])

    reply_w_dict['3d'] = 1.0 / (reply_w_dict['1d'] * (reply_list[0] * 3 / reply_list[1]))
    reply_w_dict['7d'] = 1.0 / (reply_w_dict['1d'] * (reply_list[0] * 7 / reply_list[2]))
    reply_w_dict['15d'] = 1.0 / (reply_w_dict['1d'] * (reply_list[0] * 15 / reply_list[3]))
    # reply_w_dict['30d'] = 1.0/ (reply_w_dict['1d']*(reply_list[0]*30/reply_list[4]))

    df_gdm_item = df_gdm_item.withColumn('reply_score', df_gdm_item.reply_cnt_1d +\
                                         df_gdm_item.reply_cnt_3d * reply_w_dict['3d'] +\
                                         df_gdm_item.reply_cnt_7d * reply_w_dict['7d'] +\
                                         df_gdm_item.reply_cnt_15d * reply_w_dict['15d']\
                                         #                            df_gdm_item_tmp.reply_cnt_30d * reply_w_dict['30d']
                                         )
    #
    # # %%
    #

    # #
    collect_w_dict = {'1d': 1.0, '3d': 0, '7d': 0, '15d': 0}
    df_collect = df_gdm_item.describe('collect_cnt_1d', 'collect_cnt_3d', 'collect_cnt_7d', 'collect_cnt_15d')
    collect_list = [0.0] * 4
    for i in range(4):
        collect_list[i] = float(df_collect.filter(df_collect.summary == 'mean').collect()[0][i + 1])

    collect_w_dict['3d'] = 1.0 / (collect_w_dict['1d'] * (collect_list[0] * 3 / collect_list[1]))
    collect_w_dict['7d'] = 1.0 / (collect_w_dict['1d'] * (collect_list[0] * 7 / collect_list[2]))
    collect_w_dict['15d'] = 1.0 / (collect_w_dict['1d'] * (collect_list[0] * 15 / collect_list[3]))
    # collect_w_dict['30d'] = 1.0/ (collect_w_dict['1d']*(collect_list[0]*30/collect_list[4]))

    df_gdm_item = df_gdm_item.withColumn('collect_score', df_gdm_item.collect_cnt_1d +\
                                         df_gdm_item.collect_cnt_3d * collect_w_dict['3d'] +\
                                         df_gdm_item.collect_cnt_7d * collect_w_dict['7d'] +\
                                         df_gdm_item.collect_cnt_15d * collect_w_dict['15d']\
                                         #                            df_gdm_item_tmp_1.collect_cnt_30d * collect_w_dict['30d']
                                         )
    # #
    # # # %%
    # #

    # #
    like_w_dict = {'1d': 1.0, '7d': 0, '15d': 0}
    df_like = df_gdm_item.describe('like_cnt_1d', 'like_cnt_7d', 'like_cnt_15d')
    like_list = [0.0] * 3
    for i in range(3):
        like_list[i] = float(df_like.filter(df_like.summary == 'mean').collect()[0][i + 1])

    like_w_dict['7d'] = 1.0 / (like_w_dict['1d'] * (like_list[0] * 7 / like_list[1]))
    like_w_dict['15d'] = 1.0 / (like_w_dict['1d'] * (like_list[0] * 15 / like_list[2]))
    # like_w_dict['30d'] = 1.0/ (like_w_dict['1d']*(like_list[0]*30/like_list[3]))

    df_gdm_item = df_gdm_item.withColumn('like_score', df_gdm_item.like_cnt_1d +\
                                         df_gdm_item.like_cnt_7d * like_w_dict['7d'] +\
                                         df_gdm_item.like_cnt_15d * like_w_dict['15d']\
                                         #                            df_gdm_item_tmp_1.like_cnt_30d * like_w_dict['30d']
                                         )
    # # #
    # # # # %% md
    # # #
    # # # # 根据特征平均数权重计算总分影响力权重
    # # #
    # # # # %%
    # # #
    score_w_dict = {'reply': 1.0, 'collect': 0, 'like': 0}
    score_list = [0.0] * 3
    sum_score = 0
    df_score = df_gdm_item.describe('reply_score', 'collect_score', 'like_score')

    for i in range(3):
        score_list[i] = float(df_score.filter(df_score.summary == 'mean').collect()[0][i + 1])
        sum_score = sum_score + score_list[i]

    score_w_dict['reply'] = score_list[0] / sum_score
    score_w_dict['collect'] = score_list[1] / sum_score
    score_w_dict['like'] = score_list[2] / sum_score

    df_gdm_item = df_gdm_item.withColumn('heat_score', df_gdm_item.reply_score * score_w_dict['reply'] +\
                                         df_gdm_item.collect_score * score_w_dict['collect'] +\
                                         df_gdm_item.like_score * score_w_dict['like'])
    # # #
    # # # # %%
    # # #
    #
    #
    df_gdm_item = df_gdm_item.select('item_id',\
                                     'heat_score',\

                                     )
    # #

    # #
    # # # %% md
    # #
    # # 存入redis hive
    #
    # # %%
    #
    # spark.sql("drop table if exists cmp_tmp.cmp_tmp_heat_score_reply")
    # df_gdm_item.registerTempTable('test_hive')
    # spark.sql("create table cmp_tmp.cmp_tmp_heat_score_reply select * from test_hive")
    # #
    # # # %%
    # #
    # #
    # #
    # # # %%
    # #
    pool = redis.ConnectionPool(host="codis.rcmsys2.lq.autohome.com.cn", port="19099")
    conn = redis.Redis(connection_pool=pool, decode_responses=True)
    dict_heat_score = df_gdm_item.rdd.collectAsMap()
    # #
    # # # %%
    # #

    # #
    # # # %%
    # #

    # #
    # # # %%
    # #
    with conn.pipeline(transaction=False) as p:
        for key in dict_heat_score:
            p.set(key, dict_heat_score[key], 90000)  # 6000代表6000秒，可以自己设置
        p.execute()

    # # %%
    #

    #
    # # %%

    spark.stop()

    # %%