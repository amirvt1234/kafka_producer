import sys
from pykafka import KafkaClient
from pykafka.partitioners import HashingPartitioner
from pykafka.partitioners import BasePartitioner
import numpy as np
import time


hashing_partitioner = HashingPartitioner()


client   = KafkaClient("localhost:9092") # , socket_timeout_ms=1000
topic    = client.topics["jtest"]
#producer = topic.get_producer(partitioner=hashing_partitioner, linger_ms = 200)
producer = topic.get_producer()

#times = time.time()
#with topic.get_sync_producer() as producer:
#	for i in range(8000):
#		producer.produce('test message ' + str(i))
#print time.time()-times
times = time.time()
with topic.get_producer(linger_ms = 100) as producer:
	for i in range(80000):
		producer.produce('test message ' + str(i))
print time.time()-times
"""
     count = 0
     while True:
         count += 1
         producer.produce('test msg', partition_key='{}'.format(count))
         if count % 10 ** 5 == 0:  # adjust this or bring lots of RAM ;)
             while True:
                 try:
                     msg, exc = producer.get_delivery_report(block=False)
                     if exc is not None:
                         print 'Failed to deliver msg {}: {}'.format(
                             msg.partition_key, repr(exc))
                     else:
                         print 'Successfully delivered msg {}'.format(
                         msg.partition_key)
                 except Queue.Empty:
                     break

"""

"""
#ld1  = ["1;1", "1;2", "1;3", "1;4", "2;1", "2;1", "2;1", "3;1", "3;1"]
ld1  = [[1,1], [1,2], [1,3], [1,4], [2,5], [2,7], [2,7.5], [3,8], [3,8.2]]
for index, item in enumerate(ld1):
	outputStr = "%s;%s" % (item[0], int(item[1]))
	print (outputStr)
	#producer.produce(outputStr, partition_key=str(item[0]))
	producer.produce(outputStr)
#outputStr = "%s;%s" % (currKey, int(currID))
#producer.produce(outputStr, partition_key=str(currKey))
"""
