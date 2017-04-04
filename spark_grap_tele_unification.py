from pyspark import SparkConf, SparkContext
from influxdb import InfluxDBClient
from copy import deepcopy
import tensorflow as tf
import re

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
        self.use_spark = False
        

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

        self.query_g = 'select * from '+\
        '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AllocatedContainers",'\
        '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AvailableVCores",'+\
        '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.ContainerLaunchDurationAvgTime" '

        self.query_t = 'select * from cpu,mem '# where time = 1490706992000000000'

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

    def main(self):

        limit = 5
        offset = 5
        query = "{0} limit {1} offset {2}".format(self.query_g, limit, offset)
        results_g = self.query_batch(query, db="graphite")
        count = 0

        if self.use_spark:
            sc = SparkContext()

        for res_g in results_g.raw['series']:
            values = res_g['values']
            name = res_g['name']
            #columns = a['columns']  # value and time
            if re.search(r'yarn.Hostname=(.*?)\.',name,re.I|re.S):
                hostname = re.search(r'yarn.Hostname=(.*?)\.',name,re.I|re.S).group(1)
                print "hostname:",hostname
                #values = self.add_column_name_to_data(values=values, value=hostname)
                time_list = self.extract_time_list(values)
                for tm in time_list[0:]:
                    print tm
                    query = "{0} where time = {1} limit {2} offset {3}".format(self.query_t, tm, 2, 2)
                    results_t = self.query_batch(query, db="telegraf")
                    for res_t in results_t.raw['series']:
                        values_t = res_t['values']
                        name_t = res_t['name']

                        val_lim = [value[:1]+value[21:23] for value in values_t]
                        print val_lim

            if self.use_spark:
                rdd = (sc.parallelize(values)) # .values and .keys  #.map(lambda x: 2 * x))
                #print rdd.collect()

            if self.use_spark:
                if count > 0:
                    res = res.join(rdd)
                else:
                    res = rdd
                count += 1
            else:
                print values

        #print ("Result: {0}".format(result_g.items))

        """
        count0 = 0
        for tab in result_t.raw['series']:
            values = tab['values']
            name = tab['columns']
            val_list = list()
            for value in values:
                val = value[:1]+value[21:23]
                val_list.append(val)
            val_list0 = deepcopy(val_list)
            rdd = (sc.parallelize(val_list)) #.map(lambda x: 2 * x))
            #print rdd.collect()
            if count0 > 0:
                res1 = res1.join(rdd)
            else:
                res1 = rdd
            count0 += 1
        print (res1.collect())
        """
        #self.load_data_into_tensorflow(res1.collect())

        #print (sc.parallelize([0, 2, 3, 4, 6], 5).glom().collect())

if __name__ == '__main__':
    indbtf = InfluxTensorflow('localhost', 8086, 'adminuser', 'adminpw', 'graphite')
    indbtf.main()
