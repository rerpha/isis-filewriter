from kafka import KafkaConsumer, TopicPartition
from streaming_data_types import area_detector_NDAr
from streaming_data_types.fbschemas.NDAr_NDArray_schema.DType import DType
import matplotlib.pyplot as plt
from struct import *

NUM_OF_MESSAGES = 2

types = {
    DType.Int32: (4, "i"),
    DType.Int64: (8, "q"),
    DType.Float32: (4, "f"),
    DType.Float64: (8, "d"),
}

consumer = KafkaConsumer(bootstrap_servers='livedata.isis.cclrc.ac.uk:9092', auto_offset_reset='earliest')
topic_partition = TopicPartition('LOQ_areaDetector', 0)
consumer.assign([topic_partition])
end = consumer.end_offsets([topic_partition])

consumer.seek(topic_partition, end[topic_partition]-NUM_OF_MESSAGES)

for message in range(NUM_OF_MESSAGES):
    message = next(consumer)
    ad_message = area_detector_NDAr.deserialise_ndar(message.value)

    data_type = types[ad_message.data_type]

    image = []
    index = 0
    for y in range(ad_message.dims[0]):
        row = []
        for x in range(ad_message.dims[1]):
            data_point = ad_message.data[index:index + data_type[0]]

            byte_data = pack('B' * data_type[0], *data_point)
            converted_value = unpack(data_type[1], byte_data)[0]

            row.append(converted_value)
            index += data_type[0]
        image.append(row)

    plt.imshow(image)
    plt.show()
