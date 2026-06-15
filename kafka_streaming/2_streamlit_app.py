from pathlib import Path

import cv2
from confluent_kafka import Producer
import numpy as np
import streamlit as st

from utils import ccloud_lib

CONFIG_FILE = Path(__file__).with_name("kafka.config")


@st.cache_resource
def get_kafka_config():
    return ccloud_lib.read_ccloud_config(str(CONFIG_FILE))


@st.cache_resource
def get_kafka_producer():
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(
        get_kafka_config().copy()
    )
    return Producer(producer_conf)


def convert_image_to_bytes(image):
    image = cv2.resize(image, (360, 240), interpolation=cv2.INTER_LINEAR)
    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 70]
    _, buffer = cv2.imencode(".jpg", image, encode_param)
    return buffer.tobytes()


def main():
    st.title("Capture and Send Images to Kafka")
    topic_name = st.text_input("Kafka Topic", "webcam-feed")

    kafka_config = get_kafka_config()
    producer = get_kafka_producer()
    img_file_buffer = st.camera_input("Capture from your webcam")

    if img_file_buffer is not None:
        ccloud_lib.create_topic(kafka_config, topic_name)

        file_bytes = np.asarray(bytearray(img_file_buffer.getvalue()), dtype=np.uint8)
        img = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)
        st.image(img, caption="Captured Image", use_column_width=True)

        img_bytes = convert_image_to_bytes(img)
        producer.produce(topic_name, img_bytes)
        producer.poll(0)
        st.success("Frame sent to Kafka")

    st.button("Flush Kafka", on_click=lambda: producer.flush())


if __name__ == "__main__":
    main()
