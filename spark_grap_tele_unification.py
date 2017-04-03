from pyspark import SparkConf, SparkContext
from influxdb import InfluxDBClient
from copy import deepcopy
import tensorflow as tf

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

        self.query_g = 'select * from '+\
        '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AllocatedContainers",'\
        '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AvailableVCores",'+\
        '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.ContainerLaunchDurationAvgTime" '
        
        self.query_t = 'select * from cpu,"mem" where time = 1490706992000000000 '

    def query_batch(self, q, limit, offset, db, epoch='ms'):
        cli = InfluxDBClient(self.hostname, self.port, self.username, self.password)
        while True:
            qq = "{0} limit {1} offset {2}".format(q, limit, offset)
            res = cli.query(qq, database=db, epoch=epoch)
            if not res:
                break
            yield res
            #return res

            offset += limit
            break

    def load_data_into_tensorflow(self, data):
        X = tf.placeholder(tf.float32, data)
        print X

    def main(self):

        for res in self.query_batch(self.query_g, limit=50, offset=50, db="graphite"):
            print res


        #print ("Result: {0}".format(result_t))
        #print ("Result: {0}".format(result_g.items))

        #sc = SparkContext()
        count = 0
        """for tab in result.raw['series']:
            values = tab['values']
            name = tab['name']
            columns = tab['columns']
            rdd = (sc.parallelize(values)) # .values and .keys
            #print rdd.collect()
            if count > 0:
                res = res.join(rdd)
            else:
                res = rdd
            count += 1
        print (res.collect())"""

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
