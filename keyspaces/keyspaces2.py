import json
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ConsistencyLevel, EXEC_PROFILE_DEFAULT


def lambda_handler(event, context):
    """
    注意：暴露了秘钥
    """
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.load_verify_locations('sf-class2-root.crt')
    ssl_context.verify_mode = CERT_REQUIRED
    # auth_provider = PlainTextAuthProvider(username='ServiceUserName', password='ServicePassword')
    auth_provider = PlainTextAuthProvider(username='test-at-Sample', password='1lbKG+cP1T1Samplet504=Sample')
    cluster = Cluster(['cassandra.ap-east-1.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142)
    session = cluster.connect()

    execution_profile = session.execution_profile_clone_update(session.get_execution_profile(EXEC_PROFILE_DEFAULT))
    execution_profile.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    # r = session.execute('select * from system_schema.keyspaces')
    session.execute("INSERT INTO lambda.cloud_fc_log (request_id,cloud_type,fc_svc_name,fc_name,memory,exec_time,duration,remark,created_time) VALUES ('22222223',1,'333333','1111',256,'2022-02-22 09:21:30',99,'ttt','2022-02-22 09:21:30')",
                    execution_profile=execution_profile)

    r = session.execute('select * from lambda.cloud_fc_log')
    print(r.current_rows)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

if __name__ == "__main__":
    lambda_handler(None,None)