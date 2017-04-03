
# Copyright 2015 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

"""Converts MNIST data to TFRecords file format with Example protos."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import sys

import tensorflow as tf
import numpy as np
from tensorflow.contrib.learn.python.learn.datasets import mnist

#from influxdb import query_batch, convert
from influxdb import InfluxDBClient
from tensorflow.python.framework import dtypes
from tensorflow.contrib import learn as tflearn
from tensorflow.contrib import layers as tflayers

#from sklearn.metrics import mean_squared_error
#from sklearn.model_selection import train_test_split

FLAGS = None

# Data sets
IRIS_TRAINING = "iris_training.csv"
IRIS_TEST = "iris_test.csv"

# Load datasets.
training_set = tf.contrib.learn.datasets.base.load_csv_with_header(
    filename=IRIS_TRAINING,
    target_dtype=np.int,
    features_dtype=np.float32)
test_set = tf.contrib.learn.datasets.base.load_csv_with_header(
    filename=IRIS_TEST,
    target_dtype=np.int,
    features_dtype=np.float32)

json_body = [
    {
        "measurement": "cpu_load_short",
        "tags": {
            "host": "server01",
            "region": "us-west"
        },
        "time": "2009-11-10T23:00:00Z",
        "fields": {
            "value": 0.64
        }
    }
]

client = InfluxDBClient('localhost', 8086, 'adminuser', 'adminpw', 'graphite')
#result = client.query('select * from "nodemanager.container.ContainerResource_container_1489768375814_0012_01_000002.ContainerResource=container_1489768375814_0012_01_000002.'+\
#'Context=container.ContainerPid=22220.Hostname=vagrant.PMemUsageMBsIMinMBs" where time < now() limit 10')
    result = client.query('select * from '+\
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

#client = InfluxDBClient('localhost', 8086, 'adminuser', 'adminpw', 'telegraf')
#result = client.query('select * from cpu where time < now() - 4h limit 10')
print (type(result))
print (len(result))
print  ("Result: {0}".format(result))


"""
# Specify that all features have real-value data
feature_columns = [tf.contrib.layers.real_valued_column("", dimension=4)]

# Build 3 layer DNN with 10, 20, 10 units respectively.
classifier = tf.contrib.learn.DNNClassifier(feature_columns=feature_columns,
                                            hidden_units=[10, 20, 10],
                                            n_classes=3,
                                            model_dir="/tmp/iris_model")
# Fit model.
classifier.fit(x=training_set.data,
               y=training_set.target,
               steps=2000)

# Evaluate accuracy.
accuracy_score = classifier.evaluate(x=test_set.data,
                                     y=test_set.target)["accuracy"]
print('Accuracy: {0:f}'.format(accuracy_score))

# Classify two new flower samples.
new_samples = np.array(
    [[6.4, 3.2, 4.5, 1.5], [5.8, 3.1, 5.0, 1.7]], dtype=float)
y = list(classifier.predict(new_samples, as_iterable=True))
print('Predictions: {}'.format(str(y)))
"""

