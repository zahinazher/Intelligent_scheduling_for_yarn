from pyspark import SparkConf, SparkContext
from influxdb import InfluxDBClient
from copy import deepcopy
import tensorflow as tf
import re
from operator import add

timestamps = [
    1490706976000000000,
    1490706978000000000,
]


class InfluxTensorflow():

    def __init__(self, hostname, port, username, password, db): #'localhost', 8086, 'adminuser', 'adminpw', 'telegraf'):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.db = db

        #'/nodemanager.container.ContainerResource_container_.*.ContainerResource=container_.*.Context=container.ContainerPid=.*.Hostname=vagrant.PCpuUsagePercentMaxPercents/,'+\
        #'/nodemanager.container.ContainerResource_container_.*.ContainerResource=container_.*.Context=container.ContainerPid=.*.Hostname=vagrant.PMemUsageMBsMaxMBs/,'+\
        #'/application_.*.driver.BlockManager.memory.memUsed_MB/,/application_.*.driver.BlockManager.memory.remainingMem_MB/,'+\
        #'/application_.*.driver.BlockManager.disk.diskSpaceUsed_MB/,'+\
        #'"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemHeapCommittedM",'+\
        #'"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemHeapMaxM",'+\
        #'"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemHeapUsedM",'+\
        #'"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemNonHeapCommittedM",'+\
        #'"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemNonHeapMaxM",'+\
        #'"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemNonHeapUsedM",'+\
        #'"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AllocatedGB",'+\


        self.query_gg = 'select * from '+\
         '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AllocatedContainers",'+\
        '/nodemanager.container.ContainerResource_container_.*.ContainerResource=container_.*.Context=container.ContainerPid=.*.Hostname=vagrant.PCpuUsagePercentMaxPercents/,'+\
        '/nodemanager.container.ContainerResource_container_.*.ContainerResource=container_.*.Context=container.ContainerPid=.*.Hostname=vagrant.PMemUsageMBsMaxMBs/,'+\
        '/application_.*.driver.BlockManager.memory.memUsed_MB/,/application_.*.driver.BlockManager.memory.remainingMem_MB/,'+\
        '/application_.*.driver.BlockManager.disk.diskSpaceUsed_MB/,'+\
        '"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemHeapCommittedM",'+\
        '"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemHeapMaxM",'+\
        '"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemHeapUsedM",'+\
        '"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemNonHeapCommittedM",'+\
        '"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemNonHeapMaxM",'+\
        '"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemNonHeapUsedM",'+\
        '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AllocatedGB",'+\
         '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.ContainerLaunchDurationAvgTime" '


        self.query_g = 'select * from '+\
         '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AllocatedContainers",'+\
         '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AvailableVCores",'+\
         '"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemNonHeapCommittedM",'+\
         '"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemNonHeapMaxM",'+\
         '"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemNonHeapUsedM",'+\
         '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AllocatedGB",'+\
         '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.ContainerLaunchDurationAvgTime" '

        self.query_t = 'select * from cpu,mem '

    def query_batch(self, query, db, epoch='ns'):
        cli = InfluxDBClient(self.hostname, self.port, self.username, self.password)

        while True:
            res = cli.query(query, database=db, epoch=epoch)
            if not res:
                break
            #yield res
            return res

            offset += limit
            break

    def load_data_into_tensorflow(self, data):
        X = tf.placeholder(tf.float32, data)
        print X

    def add_column_name_to_data(self, values, value):
        return [val+[value] for val in values]

    def extract_time_list(self, values):
        return [val[0] for val in values]

    def join_rdd(self, rdd1_, rdd2_):
        rdd_ = rdd1_.join(rdd2_) #.collectAsMap() # .reduceByKey(lambda x,y : x+y)
        if rdd_:
            return rdd_.map(lambda x : (x[0],sum(x[1],())))
        else:
            return rdd1_

    def main(self):

        time1 = 1490706976000000000
        time2 = 1490706999000000000
#        query = "{0} limit {1} offset {2}".format(self.query_g, limit, offset)
        query = "{0} where time > {1} and time < {2} limit 3".format(self.query_g, time1, time2)
        results_g = self.query_batch(query, db="graphite")

        query = "{0} where time > {1} and time < {2} limit 3".format(self.query_t, time1, time2)
        results_t = self.query_batch(query, db="telegraf")
        values_t =(results_t.raw['series'])[1]['values']

        sc = SparkContext()

        count = 0
        for res_g in results_g.raw['series'][0:]:
            values_g = res_g['values']
            name_g = res_g['name']

            rdd1 = sc.parallelize(values_g)
            rdd1_ = rdd1.map(lambda x: (x[0], tuple(x[1:])))
            print rdd1_.collect()
            if count == 0:
                rdd_join = rdd1_
            else:
                rdd_join = self.join_rdd(rdd_join, rdd1_)
                pass
            count += 1
        print rdd_join.collect()
        #print rdd_join.coalesce(1).collect()   # .glom()

        """for res_t in results_t.raw['series'][:2]:
            values_t = res_t['values']
            name_t = res_t['name']
            rdd1 = sc.parallelize(values_t)
            rdd1_ = rdd1.map(lambda x: (x[0], tuple(x[1:])))
            rdd_join = self.join_rdd(rdd_join, rdd1_)
        print rdd_join.collect()"""

        #columns = a['columns']  # value and time
        if re.search(r'yarn.Hostname=(.*?)\.',name_g,re.I|re.S):
            hostname = re.search(r'yarn.Hostname=(.*?)\.',name_g,re.I|re.S).group(1)
            #print "hostname:",hostname
            #values = self.add_column_name_to_data(values=values, value=hostname)
            time_list = self.extract_time_list(values_g)

        #self.load_data_into_tensorflow(rdd_join.collect())


if __name__ == '__main__':
    indbtf = InfluxTensorflow('localhost', 8086, 'adminuser', 'adminpw', 'graphite')
    indbtf.main()

