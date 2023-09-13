'''
Created on 2023. 9. 12.

@author: lloydk-jisoo
'''
import json
import sys
import pickle

#연결 객체 생성-------------------------------------
from elasticsearch import Elasticsearch

es_host = 'localhost'
es_port = 9200

#id, password 튜플 객체 불러오기
with open('basic_auth.dat', mode='rb') as obj:
    basic_auth_p = pickle.load(obj)

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

#print(es) #<Elasticsearch(['https://localhost:9200'])>

# 연결 테스트----------------------------------------
response = es.ping()
#ping(): Elasticsearch 클라이언트를 사용하여 Elasticsearch 서버의 핑을 검사
if response:
    print('Connection successful')
else:
    sys.exit("Connection failed")   
    
#------------------------------------------------

#[파일 열기]
with open('/testdata/dummy_data2.json', 'r', encoding='utf-8') as f:
    json_string = f.read()
    row_data = json.loads(json_string, strict=False) 
    
#[인덱스 생성]
#es.indices.create(index='dummy_data_json', ignore=400)  #DeprecationWarning 뜨지만 생성됨

#[적재]
#1. 그대로 적재해보기 => 실패
#es.index(index='dummy_data_json', body=row_data)
#[실행 기록]
#413에러 코드(요청이 너무 커서 서버가 처리할 수 없다.) -> yml 파일 http.max_content_length 200mb로 변경(기본 100mb)
#-> 타임아웃 에러, 연결객체에 옵션 추가 -> gc overhead 경고 메시지 반복해서 뜨다가 팅김

#2. 데이터 나눠서 적재
from elasticsearch import helpers

#데이터 chunk로 나누기 (객체 100개씩 나눔)
def chunk_data(row_data, chunk_size): #row_data는 list
    for i in range(0, len(row_data), chunk_size):
        yield row_data[i:i + chunk_size]
        #yield: row_data의 일부분(청크)이 슬라이싱되어 yield를 통해 제너레이터 객체(이터레이터 사용 가능)로 반환 
        #-> 다시 호출될 때 마지막 상태부터 실행을 계속함

def index_data(index_name, data):
    actions = [
        {
            "_index": index_name, #_index키: 인덱스 이름
            #"_source": doc,
            "_source": {"dataList": doc}, #_source키: 데이터
        }
        for doc in data #chunk(list)의 dict 객체 개수 만큼 위의 dict객체 생성
    ]
    helpers.bulk(es, actions) #actions 리스트 적재
    #helpers.bulk(): Elasticsearch 클라이언트의 helpers 모듈에 있는 함수.
    #여러 개의 문서를 한 번에 Elasticsearch에 삽입

# 데이터 청크로 나누고 색인
chunk_size = 100
for chunk in chunk_data(row_data["dataList"], chunk_size):
    index_data("dummy_data_json", chunk)

#개수 확인 -----------------------------------------------------
#res = es.count(index='dummy_data_json')
#print(f"Indexed {res['count']} documents")
#Indexed 9000 documents




























