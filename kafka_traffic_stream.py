from selenium import webdriver
import threading
from confluent.kafka import Producer

from images_processing_layers.images_processor import Images_Processing

""" Link camera """
CAMERA_NGUYENTHAISON_PHANVANTRI2_STREET = "https://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=5a6060e08576340017d0660f&camLocation=Nguy%E1%BB%85n%20Th%C3%A1i%20S%C6%A1n%20-%20Phan%20V%C4%83n%20Tr%E1%BB%8B%202&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CAMERA_HOANGVANTHU_CONGHOA_STREET = "https://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=5d8cdbdc766c88001718896a&camLocation=Ho%C3%A0ng%20V%C4%83n%20Th%E1%BB%A5%20-%20C%E1%BB%99ng%20H%C3%B2a&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CAMERA_TRUONGCHINH_TANKITANQUY_STREET = "https://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=586e25e1f9fab7001111b0ae&camLocation=Tr%C6%B0%E1%BB%9Dng%20Chinh%20-%20T%C3%A2n%20K%E1%BB%B3%20T%C3%A2n%20Qu%C3%BD&camMode=camera&videoUrl=http://camera.thongtingiaothong.vn/s/586e25e1f9fab7001111b0ae/index.m3u8"
CAMERA_CMT8_TRUONGSON_STREET = "https://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae79aabfd3d90017e8f26a&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20Tr%C6%B0%E1%BB%9Dng%20S%C6%A1n&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"

""" List of camera """
camera_list = {"nguyenthaison_phanvantri2": CAMERA_NGUYENTHAISON_PHANVANTRI2_STREET, 
               "hoangvanthu_conghoa": CAMERA_HOANGVANTHU_CONGHOA_STREET,
               "truongchinh_tankitanquy": CAMERA_TRUONGCHINH_TANKITANQUY_STREET,
               "cmt8_truongson": CAMERA_CMT8_TRUONGSON_STREET}


def read_realtime_traffic(driver: webdriver.Chrome, link: str, street: str, data_vehicle_count: list):

    #call Images_Processing class to take screenshot and process image
    processor = Images_Processing(driver, link, street).implement()

    #append vehicle count to data list
    data_vehicle_count.append(processor._vehicle_count)


if __name__ == "__main__":
    """ Create driver for taking screenshot """
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome()

    """ Create producer for sending data to Kafka """
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    while True:

        data_vehicle_count = []

        """ Multithreading for taking screenshot and processing image """
        threads = []
        for street, link in camera_list.items():
            thread = threading.Thread(target = read_realtime_traffic, 
                                    args = (driver, link, street, data_vehicle_count))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        """ Create producer for sending data to Kafka """

        for data in data_vehicle_count:
            producer.produce("traffic", value = data)
