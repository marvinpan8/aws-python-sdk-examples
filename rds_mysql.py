import sys

import pymysql

rds_host = "mysql-2.ckhifsttrftv.ap-east-1.rds.amazonaws.com"
name = "XXXXX"
password = "XXXXXX"
db_name = "fc_log"
port = 3306

try:
    conn = pymysql.connect(host=rds_host,
                           user=name,
                           passwd=password,
                           db=db_name,
                           connect_timeout=5,
                           cursorclass=pymysql.cursors.DictCursor)
except:
    sys.exit()


def lambda_handler(event, context):
    with conn.cursor() as cur:
        insert = "insert into cloud_fc_log values ('33333333',1,'333333','1111',256,'2022-02-22 09:21:30',99,'ttt','2022-02-22 09:21:30')"
        cur.execute(insert)
        conn.commit()

        qry = "select * from cloud_fc_log"
        cur.execute(qry)
        body = cur.fetchall()

        print(body)

        # 关闭cursor对象
        cur.close()
        # 关闭connection对象
        conn.close()
        return 'OK'
