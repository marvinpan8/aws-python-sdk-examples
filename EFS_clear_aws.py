# -*- coding: utf-8 -*-
import os
import sys
import time
import warnings
from multiprocessing import cpu_count
from pathlib import Path

warnings.filterwarnings('ignore')

tree_str = ''


def lambda_handler(event, context):
    """
    Lambda 内存设置 128M，使用44M，超时 5分钟
    top_path: EFC文件系统 mount 路径
    """
    top_path = '/mnt/jrtz'

    stm = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stm))
    print("sys.version ==> ", sys.version)
    print("cpu count ==> ", cpu_count())

    # 删除所有文件夹和文件
    delete_dir(top_path)

    generate_tree(Path(top_path))
    print(tree_str)

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
