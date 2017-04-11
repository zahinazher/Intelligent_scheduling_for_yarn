from pyspark import SparkConf, SparkContext
from influxdb import InfluxDBClient
from copy import deepcopy
import tensorflow as tf
import os
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
         '"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemHeapMaxM",'+\
         '"nodemanager.jvm.JvmMetrics.Context=jvm.ProcessName=NodeManager.Hostname=vagrant.MemHeapUsedM",'+\
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

    def training_step(i, update_test_data, update_train_data, XX, Y_, rdd_join):

        print "\r", i,
        ####### actual learning
        # reading batches of 100 images with 100 labels
        #batch_X, batch_Y = mnist.train.next_batch(100)
        batch_X = [[ 1,  0,  0,  1,  0,  1,  0,  0,  0,  0]
 [ 0,  0,  0,  1,  0,  0,  0,  0,  0,  0]
 [ 0,  1,  0,  0,  0,  0,  1,  0,  0,  0]]

        batch_Y = [[ 1,  0,  0,  1,  0,  1,  0,  1,  0,  0]
 [ 0,  0,  0,  1,  1,  0,  1,  0,  0,  0]
 [ 0,  1,  0,  0,  1,  0,  1,  0,  0,  0]]

        # the backpropagation training step
        sess.run(train_step, feed_dict={XX: batch_X, Y_: batch_Y})

        ####### evaluating model performance for printing purposes
        # evaluation used to later visualize how well you did at a particular time in the training
        train_a = []
        train_c = []
        test_a = []
        test_c = []
        if update_train_data:
            a, c = sess.run([accuracy, cross_entropy], feed_dict={XX: batch_X, Y_: batch_Y})
            train_a.append(a)
            train_c.append(c)

        """if update_test_data:
            a, c = sess.run([accuracy, cross_entropy], feed_dict={XX: mnist.test.images, Y_: mnist.test.labels})
            test_a.append(a)
            test_c.append(c)"""

        return (train_a, train_c, test_a, test_c)

    def train_model(self, rdd_join):
        X = tf.placeholder(tf.float32, [1,10])
        # load data, 60K trainset and 10K testset
        #mnist = input_data.read_data_sets("MNIST_data/", one_hot=True)

        #a1 = tf.placeholder(list, data.collect())
        #print a1
        # 1. Define Variables and Placeholders
        X = tf.placeholder(tf.float32, [None, 1, 10]) #the first dimension (None) will index the images
        # Y_ = ?
        Y_ = tf.placeholder(tf.float32, [None, 1, 10]) # one hot encoding
         # correct answers
        # Weights initialised with small random values between -0.2 and +0.2
        # 200, 100, 60, 30 and 10 neurons for each layer
        W1 = tf.Variable(tf.truncated_normal([1, 10], stddev=0.1)) # 784 = 28 * 28
        B1 = tf.Variable(tf.zeros([3]))
        W2 = tf.Variable(tf.truncated_normal([1, 10], stddev=0.1)) # 784 = 28 * 28
        B2 = tf.Variable(tf.zeros([3]))
        """W3 = tf.Variable(tf.truncated_normal([100, 60], stddev=0.1)) # 784 = 28 * 28
        B3 = tf.Variable(tf.zeros([60]))
        W4 = tf.Variable(tf.truncated_normal([60, 30], stddev=0.1)) # 784 = 28 * 28
        B4 = tf.Variable(tf.zeros([30]))
        W5 = tf.Variable(tf.truncated_normal([30, 10], stddev=0.1)) # 784 = 28 * 28
        B5 = tf.Variable(tf.zeros([10]))"""
        # 2. Define the model
        # XX = ?
        XX = tf.reshape(X, [-1, 1]) # flattening images

        # Y = Wx + b
        ######## SIGMOID activation func #######
        # Y1 = tf.nn.sigmoid(tf.matmul(XX, W1) + B1)
        # Y2 = tf.nn.sigmoid(tf.matmul(Y1, W2) + B2)
        # Y3 = tf.nn.sigmoid(tf.matmul(Y2, W3) + B3)
        # Y4 = tf.nn.sigmoid(tf.matmul(Y3, W4) + B4)
        ######## ReLU activation func #######

        Y1 = tf.nn.relu(tf.matmul(XX, W1) + B1)
        #Y2 = tf.nn.relu(tf.matmul(Y1, W2) + B2)
        #Y3 = tf.nn.relu(tf.matmul(Y2, W3) + B3)
        #Y4 = tf.nn.relu(tf.matmul(Y3, W4) + B4)

        # Ylogits = ?
        Ylogits = tf.matmul(Y1, W2) + B2 # (Y4, W5) + B5

        # Y = tf.nn.?(Ylogits)
        Y = tf.nn.softmax(Ylogits)

        # 3. Define the loss function
        # cross_entropy = tf.nn.?(Ylogits, Y_) # calculate cross-entropy with logits
        cross_entropy = tf.nn.softmax_cross_entropy_with_logits(Ylogits, Y_)
        cross_entropy = tf.reduce_mean(cross_entropy)

        # cross_entropy = tf.nn.softmax(Ylogits, Y_) # calculate cross-entropy with logits
        # cross_entropy = tf.reduce_mean(?)*?
        # correct_prediction = tf.equal(tf.argmax(Y, 1), tf.argmax(Y_, 1))
        # cross_entropy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

        # 4. Define the accuracy
        is_correct = tf.equal(tf.argmax(Y,1), tf.argmax(Y_,1))
        accuracy = tf.reduce_mean(tf.cast(is_correct, tf.float32))

        # 5. Define an optimizer
        # optimizer = tf.train.GradientDescentOptimizer(0.5)
        # train_step = optimizer.minimize(cross_entropy)
        optimizer = tf.train.AdamOptimizer(0.005)  ## do not use gradient descent
        train_step = optimizer.minimize(cross_entropy)

        # initialize and train
        #init = tf.initialize_all_variables()
        init = tf.global_variables_initializer()
        sess = tf.Session()
        sess.run(init)

        # 6. Train and test the model, store the accuracy and loss per iteration

        train_a = []
        train_c = []
        test_a = []
        test_c = []

        sess.run(feed_dict=rdd_join)

        training_iter = 1000
        epoch_size = 100

        for i in range(training_iter):
            test = False
            if i % epoch_size == 0:
                test = True
            a, c, ta, tc = training_step(i, test, test, X, Y_, rdd_join)
            train_a += a
            train_c += c
            test_a += ta
            test_c += tc

        print X
        return []

    def train_model_test(self, rdd_join):
        training_data = rdd_join.collect()
        data_initializer = tf.placeholder(dtype=tf.float32,
                                    shape=[1, 3])
        input_data = tf.Variable(data_initializer, trainable=False, collections=[])
        sess = tf.Session()
        sess.run(input_data.initializer, feed_dict={data_initializer: training_data})

    def load_data_into_tensorflow(self, data):
        return self.train_model_test(data)

    def join_rdd(self, rdd1_, rdd2_):
        rdd_ = rdd1_.join(rdd2_) #.collectAsMap() # .reduceByKey(lambda x,y : x+y)
        if rdd_:
            return rdd_.map(lambda x : (x[0],sum(x[1],())))
        else:
            return rdd1_

    def join_graphite_metrics(self, results_g, sc):
        count = 0
        for res_g in results_g.raw['series'][0:4]:
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
            if re.search(r'yarn.Hostname=(.*?)\.',name_g,re.I|re.S):
                hostname = re.search(r'yarn.Hostname=(.*?)\.',name_g,re.I|re.S).group(1)
                #print "hostname:",hostname
        return rdd_join

    def join_telegraf_metrics(self, results_t, rdd_join, sc):
        for res_t in results_t.raw['series'][:2]:
            values_t = res_t['values']
            name_t = res_t['name']
            rdd1 = sc.parallelize(values_t)
            rdd1_ = rdd1.map(lambda x: (x[0], tuple(x[1:])))
            rdd_join = self.join_rdd(rdd_join, rdd1_)
        return rdd_join

    def main(self):

        time1 = 1490706976000000000
        time2 = 1490706999000000000
        query = "{0} where time > {1} and time < {2} limit 3".format(self.query_g, time1, time2)
        results_g = self.query_batch(query, db="graphite")

        query = "{0} where time > {1} and time < {2} limit 3".format(self.query_t, time1, time2)
        results_t = self.query_batch(query, db="telegraf")
        values_t =(results_t.raw['series'])[1]['values']

        sc = SparkContext()

        rdd_join = self.join_graphite_metrics(results_g, sc)
        #rdd_join = self.join_telegraf_metrics(results_t, rdd_join, sc)
        rdd_join = (rdd_join.map(lambda x : [x[0]] + list(x[1])))
        print rdd_join.collect()
        #print rdd_join.coalesce(1).glom().collect()   # .glom()

        result = self.load_data_into_tensorflow(rdd_join)


if __name__ == '__main__':
    username = 'adminuser'
    password = 'adminpw'
    f = open('/home/vagrant/config.txt', 'rb')
    info = (f.read()).split("\n")
    username = info[0]
    password = info[1]
    indbtf = InfluxTensorflow('localhost', 8086, username, password, 'graphite')
    indbtf.main()
