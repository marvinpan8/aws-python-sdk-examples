# -*- coding: utf-8 -*-
import os
import sys
import time
import warnings
import urllib.parse
import zipfile
from multiprocessing import cpu_count

import boto3

warnings.filterwarnings('ignore')

tree_str = ''


def lambda_handler(event, context):
    """
    Lambda 内存设置 1024M，超时 15分钟

    zip_name 输出目标文件名，从S3触发器获取
    bucket  S3桶
    src_dir 源文件s3目录
    dst_dir 生成的结果文件s3目录
    zip_dir 输出目标zip文件s3目录
    """
    bucket = "fc-jrtz-bj"
    src_dir = 'src-hmm-10d-test/'
    dst_dir = 'dst-hmm-10d-test/'
    zip_dir = 'zip-output/'

    print("Lambda function ARN: ", context.invoked_function_arn)
    print("CloudWatch log stream name: ", context.log_stream_name)
    print("CloudWatch log group name: ", context.log_group_name)
    print("Memory limit: ", context.memory_limit_in_mb)

    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    zip_name = os.path.basename(key)
    print("S3 zip_name ==> ", zip_name)

    stm = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stm))
    print("sys.version ==> ", sys.version)
    print("cpu count ==> ", cpu_count())

    # 创建2个临时文件夹 tmp-zip, tmp-data, 若存在，先删除再创建
    zip_path = '/mnt/jrtz/tmp-zip'
    if os.path.exists(zip_path):
        delete_dir(zip_path)
    else:
        os.makedirs(zip_path)
    data_path = '/mnt/jrtz/tmp-data'
    if os.path.exists(data_path):
        delete_dir(data_path)
    else:
        os.makedirs(data_path)


    # 循环下载到挂载文件
    s3 = boto3.resource('s3')
    total = 0
    bucket_resource = s3.Bucket(bucket)
    for obj in bucket_resource.objects.filter(Prefix=dst_dir):
        if obj.key.endswith('.xlsx'):
            fileName = obj.key.split('/')[1]
            data_file_path = data_path + '/' + fileName
            bucket_resource.download_file(obj.key, data_file_path)

            total += 1
            if total % 100 == 0:
                print("Total: " + str(total) + "---fileName => ", fileName)

    print(dst_dir + "下载文件成功，数量 =>", total)

    zip_file_path = zip_path + "/" + zip_name
    zipDir(data_path, zip_file_path)

    # 上传本地 zip文件
    s3.meta.client.upload_file(zip_file_path, bucket, zip_dir + zip_name)

    # 删除 src 目录
    batch_delete_obj(bucket, src_dir)
    # 删除 dst 目录
    batch_delete_obj(bucket, dst_dir)

    # 删除2个临时文件夹 tmp-zip, tmp-data
    if os.path.exists(zip_path):
        delete_dir(zip_path)
    if os.path.exists(data_path):
        delete_dir(data_path)

    etm = time.time()
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(etm))
    print("Start time ==> ", start_time)
    print("End time ==> ", end_time)
    duration = round(etm - stm, 3)
    print("Duration (s)==>", duration)
    return 'success'


def batch_delete_obj(bucket, prefix):
    s3_client = boto3.client('s3')
    flag = True
    while flag:
        flag = delete_obj(s3_client, bucket, prefix)


def delete_obj(s3_client, bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    delete_key_list = []
    for obj in response['Contents']:
        # print('Deleting', obj['Key'])
        del_obj = {
            'Key': obj['Key']
        }
        delete_key_list.append(del_obj)
    len_list = len(delete_key_list)
    print("delete_key_list len =>", len_list)
    delObjs = {
        'Objects': delete_key_list
    }
    if len_list > 0:
        s3_client.delete_objects(Bucket=bucket, Delete=delObjs)
    return len_list == 1000


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


def zipDir(dirPath, outFullName):
    """
    压缩指定文件夹
    :param dirPath: 目标文件夹路径
    :param outFullName: 压缩文件保存路径 + xxxx.zip
    :return: 无
    """
    zip_process = zipfile.ZipFile(outFullName, "w", zipfile.ZIP_DEFLATED)
    for path, dirNames, filenames in os.walk(dirPath):
        # 去掉目标根路径，只对目标文件夹下边的文件及文件夹进行压缩
        fspath = path.replace(dirPath, '')

        for filename in filenames:
            zip_process.write(os.path.join(path, filename), os.path.join(fspath, filename))
    zip_process.close()