from pyspark import SparkConf, SparkContext
from influxdb import InfluxDBClient
from copy import deepcopy
import tensorflow as tf
import os
import re
from operator import add
import numpy as np
import argparse

#os.environ['TF_CPP_MIN_LOG_LEVEL']='2'

class InfluxTensorflow():

    def __init__(self, hostname, port, username, password, db):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.db = db
        self.len_features = 5

        """
        these queries are only for testing and shall be removed after code testing
        """
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

        self.query_rns = 'select * from'

        self.query_t = 'select * from cpu,mem'

    def query_batch(self, query, db, epoch='ns'):
        """
        get results of a query from database

        :param query:
        :param db:
        :param epoch:
        :return 
        """
        cli = InfluxDBClient(self.hostname, self.port, self.username, self.password)

        while True:
            res = cli.query(query, database=db, epoch=epoch)
            if not res:
                break
            #yield res
            return res

            offset += limit
            break

    def training_step(self, i, update_test_data, update_train_data, XX, Y_, training_data, train_step, sess):
        """
        traininig the machine learning model on specific iterations

        :param i: iteration count
        :param update_test_data: contains updated testing data
        :param update_train_data: contains updated training data
        :param XX: data 
        :param Y_: one hot encoding 
        :param training_data: data used for training
        :param train_step: step size during training
        :param sess: session 
        :return: training and testing lists
        """

        print "\r", i,
        ####### actual learning
        # reading batches of 100 images with 100 labels
        #batch_X, batch_Y = mnist.train.next_batch(100)

        ####### evaluating model performance for printing purposes
        # evaluation used to later visualize how well you did at a particular time in the training
        train_a = []
        train_c = []
        test_a = []
        test_c = []

        data_initializer = tf.placeholder(dtype=tf.float32,
                                    shape=[3, self.len_features])
        input_data = tf.Variable(data_initializer, trainable=False, collections=[])

        # the backpropagation training step
        #sess.run(train_step, feed_dict={XX: training_data, Y_: training_data})

        if update_train_data:
            a, c = sess.run(input_data.initializer, feed_dict={data_initializer: training_data, Y_: training_data})
            train_a.append(a)
            train_c.append(c)

        if update_test_data:
            a, c = sess.run([accuracy, cross_entropy], feed_dict={XX: mnist.test.images, Y_: mnist.test.labels})
            test_a.append(a)
            test_c.append(c)

        return (train_a, train_c, test_a, test_c)

    def train_model(self, rdd_join):
        """
        train the machine learning Model 

        :param: rdd_join: Resilient distributed datasets
        :return: result for ML model
        """
        nc = self.len_features # number of columns
        nr = 3 # number of rows
        X = tf.placeholder(tf.float32, [nr,nc])

        # 1. Define Variables and Placeholders
        X = tf.placeholder(tf.float32, [nr, nc]) #the first dimension (None) will index the images
        # Y_ = ?
        Y_ = tf.placeholder(tf.float32, [nr, nc]) # one hot encoding
        # Weights initialised with small random values between -0.2 and +0.2 ; 200, 100, 60, 30 and 10 neurons for each layer
        W1 = tf.Variable(tf.truncated_normal([nr, nc], stddev=0.1)) # 784 = 28 * 28
        B1 = tf.Variable(tf.zeros([nc]))
        W2 = tf.Variable(tf.truncated_normal([nr, nc], stddev=0.1)) # 784 = 28 * 28
        B2 = tf.Variable(tf.zeros([nc]))
        # 2. Define the model
        XX = tf.reshape(X, [nc, nr]) # flattening images

        Y = Wx + b
        ######## SIGMOID activation func #######
        Y1 = tf.nn.sigmoid(tf.matmul(XX, W1) + B1)
        ######## ReLU activation func #######
        Y1 = tf.nn.relu(tf.matmul(XX, W1) + B1)

        Ylogits = tf.matmul(Y1, W2) + B2 # (Y4, W5) + B5

        Y = tf.nn.softmax(Y1) # Ylogits

        cross_entropy = tf.nn.softmax_cross_entropy_with_logits(Y1, Y_) # Ylogits with Y1
        #cross_entropy = tf.reduce_mean(cross_entropy)

        # 4. Define the accuracy
        is_correct = tf.equal(tf.argmax(Y,1), tf.argmax(Y_,1))
        # tf.argmax(Y,1) label our model thinks is most likely for each input
        # tf.argmax(y_,1) is the correct label
        accuracy = tf.reduce_mean(tf.cast(is_correct, tf.float32))

        # 5. Define an optimizer
        # optimizer = tf.train.GradientDescentOptimizer(0.5)
        # train_step = optimizer.minimize(cross_entropy)
        optimizer = tf.train.AdamOptimizer(0.005)  ## do not use gradient descent
        train_step = optimizer.minimize(cross_entropy)
        train_step = 1

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

        training_iter = 1000
        epoch_size = 100
        training_data = rdd_join.collect()

        for i in range(training_iter):
            test = False
            if i % epoch_size == 0:
                test = True
            a, c, ta, tc = self.training_step(i, test, test, X, Y_, training_data, train_step, sess)
            train_a += a
            train_c += c
            test_a += ta
            test_c += tc

        print X

        training_data = rdd_join.collect()

        data_initializer = tf.placeholder(dtype=tf.float32,
                                    shape=[nr, self.len_features])
        input_data = tf.Variable(data_initializer, trainable=False, collections=[])
        res = sess.run(input_data.initializer, feed_dict={data_initializer: training_data})
        print res

    def train_model_test(self, rdd_join):
        """
        only for test purpose
        """
        val = rdd_join.collect()
        training_data = np.array(rdd_join.collect())

        x = tf.placeholder(tf.float32, shape=(3, 5))
        y = tf.matmul(tf.reshape(x, [5, 3]), x)
        #with tf.Session() as sess:
        #    print (sess.run(y, feed_dict={x: val}))

        # Comments below
        # Specify that all features have real-value data
        feature_columns = [tf.contrib.layers.real_valued_column("", dimension=5)]

        # Build 3 layer DNN with 10, 20, 10 units respectively.
        classifier = tf.contrib.learn.DNNClassifier(feature_columns=feature_columns,
                                            hidden_units=[10, 20, 10],
                                            n_classes=3)
        # Fit model.
        classifier.fit(x=training_data,
               y=training_data,
               steps=10)

        # Evaluate accuracy.
        accuracy_score = classifier.evaluate(x=test_set.data,
                                     y=test_set.target)["accuracy"]
        print ('Accuracy: {0:f}'.format(accuracy_score))

        return []

    def train_model_lstm(self, data):
        """
        training the LSTM ML Model

        :param data: input data with features
        :return: result of LSTM ML model
        """
        num_steps = 1
        data = data.collect()
        batch_size = len(data) # total rows of data
        col_length = len(data[0])
        #data = np.array(data)
        print ("data",data)

        lstm_size = col_length
        # Placeholder for the inputs in a given iteration.
        #words = tf.placeholder(tf.float32, [batch_size, num_steps])

        lstm = tf.contrib.rnn.BasicLSTMCell(lstm_size)
        print ('lstm_size',lstm.state_size)
        # Initial state of the LSTM memory.
        initial_state = state = tf.zeros([col_length, lstm.state_size]) #lstm.state_size])
        probablilities = []
        loss = 0.0

        #for i in range(num_steps):
        if True:
            # The value of state is updated after processing each batch of words.
            output, state = lstm(data, state)

            # The LSTM output can be used to make next word predictions
            logits = tf.matmul(output, softmax_w) + softmax_b
            probabilities.append(tf.nn.softmax(logits))
            loss += loss_function(probabilities, target_words)

        final_state = state
        return final_state

    def load_data_into_tensorflow(self, data):
        #return self.train_model_test(data)
        return self.train_model_lstm(data)

    def join_rdd(self, rdd1_, rdd2_):
        """
        Join two spark RDD

        :param rdd1_: first RDD
        :param rdd2_: second RDD
        :return: result after joining two RDD
        """
        rdd_ = rdd1_.join(rdd2_) #.collectAsMap() # .reduceByKey(lambda x,y : x+y)
        if rdd_:
            return rdd_.map(lambda x : (x[0],sum(x[1],())))
        else:
            return rdd1_

    def join_graphite_metrics(self, results_g, sc):
        """
        Convert grahite db results into RDD

        :param results_g: query results from graphite db
        :param sc: sparkcontext
        :return: RDD
        """
        if results_g:
            for count, res_g in enumerate(results_g.raw['series'][0:]):
                values_g = res_g['values']
                name_g = res_g['name']
                columns_g = res_g['columns']

                rdd1 = sc.parallelize(values_g)
                rdd1_ = rdd1.map(lambda x: (x[0], tuple(x[1:])))
                print rdd1_.collect()
                if count == 0:
                    rdd_join = rdd1_
                else:
                    rdd_join = self.join_rdd(rdd_join, rdd1_)
                    pass
                if re.search(r'yarn.Hostname=(.*?)\.',name_g,re.I|re.S):
                    hostname = re.search(r'yarn.Hostname=(.*?)\.',name_g,re.I|re.S).group(1)
                    #print "hostname:",hostname
            return rdd_join
        else:
            return []

    def join_telegraf_metrics(self, results_t, sc):
        """
        Convert telegraf db results into RDD

        :param results_t: query results from telegraf db
        :param sc: sparkcontext
        :return: RDD
        """
        for count, res_t in enumerate(results_t.raw['series'][0:]):
            values_t = res_t['values']
            name_t = res_t['name']
            columns_t = res_t['columns']; print ("colmmns",columns_t)
            rdd1 = sc.parallelize(values_t)
            #rdd1_ = rdd1.map(lambda x: (x[0], tuple(x[1:])))
            rdd1_ = rdd1.map(lambda x: (1493287029000000000, tuple(x[1:]))) # hardcoded time need to be replaced after
            print ("rdd1____",rdd1_.collect())
            if count == 0:
                rdd_join = rdd1_
            else:
                rdd_join = self.join_rdd(rdd_join, rdd1_)
        return rdd_join

    def get_results_from_graphite(self, time1, time2):
        query = "{0} where time > {1} and time < {2} group by /time/ limit 2".format(self.query_g, time1, time2)
        return self.query_batch(query, db="graphite")

    def get_results_from_telegraf(self, time1, time2):
        query = "{0} where time > {1} and time < {2} group by /time/ limit 2".format(self.query_t, time1, time2)
        return self.query_batch(query, db="telegraf")

    def get_results_from_graphite_nm(self, time1, time2):
        query = "{0} nodemanager where source =~ /container.*$/ and time = {1} group by /time/,/cpu/ limit 2".format(self.query_rns, time1)
        #query = "{0} nodemanager where service =~ /jvm.*/ and source =~ /JvmMetrics/ limit 2".format(self.query_rns)
        return self.query_batch(query, db="graphite")

    def get_results_from_graphite_spark(self, time1, time2):
        query = "{0} spark where source =~ /jvm/ and service =~ /driver/ and time = {1} limit 2".format(self.query_rns, time1)
        return self.query_batch(query, db="graphite")

    def main(self):

        time1 = 1493287029000000000 # for testing purpose spark  | 1490706976000000000
        time2 = 1492514927000000000 # for testing purpose spark  | 1490706999000000000

        t_nm = 1493038354000000000 # for test

        results_t = self.get_results_from_telegraf(time11, time22); print ("result_telegraf", results_t)
        values_t = (results_t.raw['series'])[0]['columns']; print values_t; #return
        #results_g = self.get_results_from_graphite(time1, time2)

        results_g_nm = self.get_results_from_graphite_nm(time1, time2)
        print "result_g_nm",results_g_nm
        results_g_spark = self.get_results_from_graphite_spark(time1, time2)
        #print "resutls_g_spark",results_g_spark
        if True: # results_t

            sc = SparkContext()

            #rdd_join_tele = self.join_telegraf_metrics(results_t, sc)
            #print ("tele",rdd_join_tele.collect())
            rdd_join_g_nm = self.join_graphite_metrics(results_g_nm, sc)
            print ("nm",rdd_join_g_nm.collect())

            rdd_join_g_spark = self.join_graphite_metrics(results_g_spark, sc)
            print ("spark",rdd_join_g_spark.collect())

            rdd_join = self.join_rdd(rdd_join_g_nm, rdd_join_g_spark)
            rdd_join = self.join_rdd(rdd_join, rdd_join_tele)

            rdd_join = self.join_graphite_metrics(results_t, sc)
            #rdd_join = self.join_telegraf_metrics(results_t, rdd_join, sc)
            rdd_join = (rdd_join.map(lambda x : [x[0]] + list(x[1])))
            data = rdd_join.collect()
            print ("joined results", data)

            #print rdd_join.coalesce(1).glom().collect()   # .glom()  # coalesce to reduce  no of partitions
            result = self.load_data_into_tensorflow(rdd_join)

def parse_args():
    parser = argparse.ArgumentParser(
        description='Optional arguments for InfluxDB')
    parser.add_argument('--host', type=str, required=False,
                        default='localhost',
                        help='hostname of InfluxDB http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port of InfluxDB http API')
    parser.add_argument('--configfile', type=str, required=False, default='/home/vagrant/config.txt',
                        help='path to config file containing username & password')

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()

    f = open(args.configfile, 'rb')
    info = (f.read()).split("\n")
    username = info[0]
    password = info[1]
    indbtf = InfluxTensorflow(args.host, args.port, username, password, 'graphite')
    indbtf.main()

