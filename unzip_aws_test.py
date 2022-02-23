# -*- coding: utf-8 -*-
import json
import os
import pathlib
import sys
import time
import urllib.parse
import warnings
import zipfile
from multiprocessing import cpu_count
from pathlib import Path

import boto3
import requests

warnings.filterwarnings('ignore')

tree_str = ''


def lambda_handler(event, context):
    """
    Lambda 内存设置 1024M，超时 15分钟, 解压的zip文件限制最大500个文件
    参数变量：
    src_path  上传S3的源文件目录
    zip_input_dir 解压下载 zip文件 的 S3目录
    """
    src_path = "src-hmm-10d-test/"
    zip_input_dir = "zip-input/"

    stm = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stm))
    print("sys.version ==> ", sys.version)
    print("cpu count ==> ", cpu_count())

    s3 = boto3.resource('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    fileName = os.path.basename(key)
    print("S3 fileName ==> ", fileName)

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

    # 下载到挂载文件
    zip_file_path = zip_path + '/' + fileName
    # 重新指向zip真实目录
    key2 = zip_input_dir + fileName
    print("S3 下载文件开始==> " + key2 + " => " + zip_file_path)
    s3.meta.client.download_file(bucket, key2, zip_file_path)
    print("S3 下载文件完成==> " + key2 + " => " + zip_file_path)

    if os.path.splitext(zip_file_path)[1] == ".xlsx" or not zipfile.is_zipfile(zip_file_path):
        raise Exception(fileName + 'is not zip file!')

    # 解压
    zip_file = zipfile.ZipFile(zip_file_path)
    zip_list = zip_file.namelist()  # 得到压缩包里所有文件

    llen = len(zip_list)
    print("zip文件个数 => ", llen)
    j = 0
    for f in zip_list:
        j = j + 1
        if j % 100 == 0:
            print("解压文件个数 ==> ", j)
        zip_file.extract(f, data_path)  # 循环解压文件到指定目录

    zip_file.close()  # 关闭文件，必须有，释放内存

    generate_tree(Path(data_path))
    # print(tree_str)

    print("解压完成")
    # 上传本地文件
    i = 0
    for name in os.listdir(data_path):
        i = i + 1
        if i % 100 == 0:
            print("上传文件个数 ==> ", i)
        s3.meta.client.upload_file(data_path + '/' + name, bucket, src_path + name)

    print("上传所有文件完成")

    # 删除2个临时文件夹 tmp-zip, tmp-data
    if os.path.exists(zip_path):
        delete_dir(zip_path)
    if os.path.exists(data_path):
        delete_dir(data_path)

    # 触发下一个
    # trigger_next(s3, bucket, key)

    print("zip文件个数 => ", llen)
    etm = time.time()
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(etm))
    print("Start time ==> ", start_time)
    print("End time ==> ", end_time)
    duration = round(etm - stm, 3)
    print("Duration (s)==> ", duration)
    return 'success'


def get_next_zip_name(fileName):
    fileNameList = fileName.split(".")
    fileNamePre = fileNameList[0]
    fileNameEnd = fileNameList[1]
    fileNameCount = int(fileNamePre[-1:]) + 1
    if fileNameCount > 9:
        return ""
    fileNamePre = fileNamePre[:-1]
    return fileNamePre + str(fileNameCount) + "." + fileNameEnd


def trigger_next(s3, bucket, objKey):
    fileName = os.path.basename(objKey)
    fileName2 = get_next_zip_name(fileName)
    if len(fileName2) == 0:
        print("触发下一个结束")
        send_weixin_msg()
        return
    zip_file_path2 = '/tmp/' + fileName2
    pathlib.Path(zip_file_path2).touch()

    objKey2 = objKey.replace(fileName, fileName2)
    print("S3 触发next开始 =====>" + zip_file_path2 + " ==> " + objKey2)
    s3.meta.client.upload_file(zip_file_path2, bucket, objKey2)
    print("S3 触发next结束 =====>" + zip_file_path2 + " ==> " + objKey2)


def send_weixin_msg():
    data = {
        "msgtype": "text",
        "text": {
            "content": "半年云计算完成"
        }
    }
    headers = {'content-type': 'application/json'}
    resp = requests.post("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=b91a0ab6-4dfd-4074-974a-7c61b9b2a146",
                         data=json.dumps(data), headers=headers, timeout=10)
    resp.encoding = 'utf-8'
    print("response code => ", resp.status_code)
    print("response text => ", resp.text)


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