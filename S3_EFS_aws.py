# -*- coding: utf-8 -*-
import os
import sys
import time
import urllib.parse
import warnings
import zipfile
from multiprocessing import cpu_count
from pathlib import Path

import boto3

warnings.filterwarnings('ignore')

tree_str = ''


def lambda_handler(event, context):
    """
    Lambda 内存设置 1024M，超时 15分钟
    top_path: EFC文件系统 mount 路径
    S3 测试事件配置：
        Records.s3.object.key: python依赖包的压缩程序（包含在 python目录下）. eg: python-layer/hmm_10d_test_layer.zip
        Records.s3.bucket.name: fc-jrtz
        Records.s3.bucket.arn: arn:aws:s3:::fc-jrtz
    """
    top_path = '/mnt/jrtz'

    stm = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stm))
    print("sys.version ==> ", sys.version)
    print("cpu count ==> ", cpu_count())

    s3 = boto3.resource('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    fileName = os.path.basename(key)
    print("s3 fileName ==> ", fileName)

    # 先删除所有文件夹和文件
    delete_dir(top_path)

    # 下载到挂载文件
    zip_file_path = '/tmp/' + fileName
    s3.meta.client.download_file(bucket, key, zip_file_path)
    print("s3 fileName 下载完成: ", fileName)

    if os.path.splitext(zip_file_path)[1] == ".xlsx" or not zipfile.is_zipfile(zip_file_path):
        raise Exception(fileName + 'is not zip file!')

    # 解压
    zip_file = zipfile.ZipFile(zip_file_path)
    zip_list = zip_file.namelist()  # 得到压缩包里所有文件

    # dst_path = '/mnt/jrtz/python'
    # os.mkdir(dst_path)
    print("zip文件个数 => ", len(zip_list))
    for f in zip_list:
        zip_file.extract(f, top_path)  # 循环解压文件到指定目录

    zip_file.close()  # 关闭文件，必须有，释放内存

    generate_tree(Path(top_path))
    print(tree_str)
    print("zip文件个数 => ", len(zip_list))

    etm = time.time()
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(etm))
    print("Start time ==> ", start_time)
    print("End time ==> ", end_time)
    duration = round(etm - stm, 3)
    print("Duration (s)==> ", duration)
    return 'success'


def generate_tree(pathname, n=0):
    global tree_str
    if pathname.is_file():
        tree_str += '    |' * n + '-' * 4 + pathname.name + '\n'
    elif pathname.is_dir():
        tree_str += '    |' * n + '-' * 4 + \
                    str(pathname.relative_to(pathname.parent)) + '\\' + '\n'
        for cp in pathname.iterdir():
            generate_tree(cp, n + 1)


def delete_dir(pathname):
    for root, dirs, files in os.walk(pathname, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
