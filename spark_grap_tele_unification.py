from pyspark import SparkConf, SparkContext
from influxdb import InfluxDBClient

timestamps = [
    1490706976000000000,
    1490706978000000000,
]

class InfluxTensorflow():

    def __init__(self, hostname, port, username, password, db):#'localhost', 8086, 'adminuser', 'adminpw', 'telegraf'):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.db = db

    def main(self):

        client_t = InfluxDBClient(self.hostname, self.port, self.username, self.password, 'telegraf')
        #result = client.query('select * from "nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AllocatedGB" limit 10')
        result_t = client_t.query('select * from cpu where time = 1490706992000000000')
        client_g = InfluxDBClient(self.hostname, self.port, self.username, self.password, self.db)
        
        result = client_g.query('select * from '+\
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
        '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AllocatedContainers",'+\
        '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AllocatedGB",'+\
        '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AvailableVCores",'+\
        '"nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.ContainerLaunchDurationAvgTime" '+\
        'limit 3')

        #print ("Result: {0}".format(result_t))
        #print ("Result: {0}".format(result_g))

        result_g0 = client_g.query('select * from "nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.ContainerLaunchDurationAvgTime" limit 3')
        result_g1 = client_g.query('select * from "nodemanager.yarn.NodeManagerMetrics.Context=yarn.Hostname=vagrant.AvailableVCores" limit 3')
        result_g2 = client_g.query('select * from "nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemHeapCommittedM" limit 3')
        result_g3 = client_g.query('select * from "nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemNonHeapUsedM" limit 3')

        #print (result_g0)
        #print (result_g1)

        val0 = result_g0.raw['series'][0]['values'] # 'name'
        val1 = result_g1.raw['series'][0]['values']
        val2 = result_g2.raw['series'][0]['values']
        val3 = result_g3.raw['series'][0]['values']

        #print (result_g.items)

        sc = SparkContext()
        x0 = sc.parallelize(val0) 
        x1 = sc.parallelize(val1)
        x2 = sc.parallelize(val2)
        x3 = sc.parallelize(val3)
        r1 = (x0.join(x1)).collect()
        print ("result join",r1)
        #print (sc.parallelize([0, 2, 3, 4, 6], 5).glom().collect())

        """for values in result_g:
            #print ("Result: {0}".format(val))
            for val in values:
                    print val
                    data_list.append(val)
        """

if __name__ == '__main__':
    indbtf = InfluxTensorflow('localhost', 8086, 'adminuser', 'adminpw', 'graphite')
    indbtf.main()

