# -*- coding: utf-8 -*-
import os
import sys
import time
import warnings
from multiprocessing import cpu_count

import boto3

warnings.filterwarnings('ignore')

tree_str = ''


def lambda_handler(event, context):
    """
    查漏补缺函数
    Lambda 内存设置 1024M，超时 15分钟

    src_dir 源文件s3目录
    bucket  S3桶
    """
    src_dir = 'src-hmm-10d-test/'
    bucket = "fc-jrtz"

    print("Lambda function ARN: ", context.invoked_function_arn)
    print("CloudWatch log stream name: ", context.log_stream_name)
    print("CloudWatch log group name: ", context.log_group_name)
    print("Memory limit: ", context.memory_limit_in_mb)

    stm = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stm))
    print("sys.version ==> ", sys.version)
    print("cpu count ==> ", cpu_count())

    s3 = boto3.resource('s3')


    # 创建2个临时文件夹 tmp-zip, tmp-data, 若存在，先删除再创建
    zip_path = '/tmp/tmp-zip'
    if os.path.exists(zip_path):
        delete_dir(zip_path)
    else:
        os.makedirs(zip_path)
    data_path = '/tmp/tmp-data'
    if os.path.exists(data_path):
        delete_dir(data_path)
    else:
        os.makedirs(data_path)


    bucket_resource = s3.Bucket(bucket)
    with open("unfinished.txt", "r") as f:
        data = f.read()
        data_list = data.split(",")
        print("读取未完成文件数量 =>", len(data_list))
        for d in data_list:
            fileName = d + "_raw_data.xlsx"
            data_file_path = data_path + '/' + fileName
            bucket_resource.download_file(src_dir + fileName, data_file_path)
        print("下载文件完成")

        for dd in data_list:
            file_name = dd + "_raw_data.xlsx"
            data_file_path = data_path + '/' + file_name
            s3.meta.client.upload_file(data_file_path, bucket, src_dir + file_name)
        print("上传文件完成")


    etm = time.time()
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(etm))
    print("Start time ==> ", start_time)
    print("End time ==> ", end_time)
    duration = round(etm - stm, 3)
    print("Duration (s)==>", duration)
    return 'success'


def delete_dir(pathname):
    for root, dirs, files in os.walk(pathname, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
