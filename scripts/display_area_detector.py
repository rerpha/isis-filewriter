from kafka import KafkaConsumer, TopicPartition
from streaming_data_types import area_detector_NDAr
import matplotlib.pyplot as plt

NUM_OF_MESSAGES = 2

consumer = KafkaConsumer(bootstrap_servers='livedata.isis.cclrc.ac.uk:9092', auto_offset_reset='earliest')
topic_partition = TopicPartition('LOQ_areaDetector', 0)
consumer.assign([topic_partition])
end = consumer.end_offsets([topic_partition])

consumer.seek(topic_partition, end[topic_partition]-NUM_OF_MESSAGES)


for message in range(NUM_OF_MESSAGES):
    message = next(consumer)
    ad_message = area_detector_NDAr.deserialise_ndar(message.value)

    plt.imshow(ad_message.data)
    plt.show()
