from kafka import KafkaConsumer, TopicPartition
from streaming_data_types import area_detector_NDAr
from matplotlib import colors
import matplotlib.pyplot as plt

consumer = KafkaConsumer(bootstrap_servers='livedata.isis.cclrc.ac.uk:9092', auto_offset_reset='earliest')
topic_partition = TopicPartition('DEMO_areaDetector', 0)
consumer.assign([topic_partition])
end = consumer.end_offsets([topic_partition])

consumer.seek(topic_partition, end[topic_partition]-1)

message = next(consumer)
ad_message = area_detector_NDAr.deserialise_ndar(message.value)

image = []
index = 0
for y in range(ad_message.dims[0]):
    row = []
    for x in range(ad_message.dims[1]):
        first_byte = ad_message.data[index:index + 8]

        total = sum([num * pow(2, i * 8) for i, num in enumerate(first_byte)])

        row.append(total)
        index += 8
    image.append(row)

plt.imshow(image)
plt.show()