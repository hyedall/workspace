from elasticsearch import Elasticsearch

# # Elasticsearch 호스트 및 포트 설정
# # ip = 'http://192.168.219.199:9200'
# ip = 'http://localhost:9200'
# es = Elasticsearch([ip])

# 데이터 불러오기------------------------------------

import json 
with open('dummy_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

import pandas as pd

df = pd.DataFrame(data['dataList'])
print(df.head())
# http://192.168.219.126/
def insertData(json_data):
    import json
    import sys
    from elasticsearch import Elasticsearch

    # es_host = 'localhost'
    es_host = '192.168.219.126'
    es_port = 9200

    # basic_auth_p = ('elastic', 'u*pAj2CV6oEEes*5xnHE')
    basic_auth_p = ('elastic', '123456')

    es = Elasticsearch(
        [
            {
                'host':es_host,
                'port':es_port,
                'scheme': "https"
            }
        ], 
        basic_auth=basic_auth_p,
        request_timeout=300, #5min
        verify_certs=False
        #verify_certs: SSL 인증서를 검증할지 여부를 지정합니다. 여기서는 False로 설정되어 있어, 인증서 검증이 비활성화되어 있습니다. 
        #이러한 설정은 개발 또는 테스트 환경에서만 사용되어야 하며, 실제 운영 환경에서는 보안 위험으로 인해 사용되어서는 안 됩니다.
    )


    # 연결 테스트----------------------------------------
    response = es.ping()
    #ping(): Elasticsearch 클라이언트를 사용하여 Elasticsearch 서버의 핑을 검사
    if response:
        print('Connection successful')
    else:
        sys.exit("Connection failed")  
    
    index="dummy_data"

    if es.indices.exists(index=index): # 해당 인덱스가 있으면 삭제
        res = es.indices.delete(index=index, ignore=[400, 404])
        print(f'delete {index} {res}')
    
    with open('mapping.json', 'r') as f:
        mapping = json.load(f)
        
    es.indices.create(index=index, body=mapping)
    print(f">>> create index : {index}")
    print(">>> begin")
    
    from datetime import datetime
    seq = 0
    for obj in json_data:
        obj["@timestamp"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        obj['@version'] = '1'

        # 파이썬 객체(dic)을 json 문자열로 반환
        doc = json.dumps(obj, ensure_ascii=False)
        result = es.index(index=index, body=doc)
        print(f'doc : {doc}')
        print(f'{seq} : {result}')
        seq += 1
    print(f">>> end : {seq}개 완료")

insertData(data['dataList'])
