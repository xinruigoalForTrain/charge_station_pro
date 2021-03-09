import pandas as pd
from proxy_util import ProxyUtil
from proxy_util_jg import ProxyUtil_JG
from fake_useragent import UserAgent
import traceback
import pprint

import requests
import json
import sys
import os

from concurrent.futures import ProcessPoolExecutor,ALL_COMPLETED,FIRST_EXCEPTION,wait
from concurrent.futures import ThreadPoolExecutor
import threading
import asyncio
import aiohttp
import httpx
from pyquery import PyQuery as pq

import time
from datetime import datetime
import math
import re
import random

import pymongo
import redis
from icecream import ic

STREAM_POI_ID_NAME = "poi_ids"
STREAM_POI_DETAIL_NAME = 'poi_detail'
BASE_MSG_LIST = 'charge_station_base_msg'
DETAIL_MSG_LIST = 'charge_station_detail_msg'     # 提取/爬取完一个stream发一个信号量，后缀为city_code，所有工作结束后缀为done
STREAM_CRAWL_ERROR = 'charge_station_error_data'

proxy_gen = ProxyUtil()
proxy_gen_jg = ProxyUtil_JG()
ua_gen = UserAgent()     # 尝试直接实例化

session = None
limits = httpx.Limits(max_connections=5,max_keepalive_connections=5)

def time_format():
    return f'{datetime.now()} '

ic.configureOutput(prefix=time_format)

URL_DOMAIN = 'https://detail.amap.com/detail/'     # aiohttp不支持https代理，在此做httpx代理尝试

header_common = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Connection': 'keep-alive',
            # 'Cookie': 'guid=36eb-0656-bf10-31b7; UM_distinctid=175a0db10c31-0e0dbd2a31ca3e-303464-100200-175a0db10c49d; cna=vfcKGMjqUisCAbSkOO7waCtY; _uab_collina=160472152888805863689171; CNZZDATA1255827602=1721747437-1604823062-https%253A%252F%252Fditu.amap.com%252F%7C1604823062; xlly_s=1; CNZZDATA1255626299=1164073884-1604718411-https%253A%252F%252Fcn.bing.com%252F%7C1604995816; x5sec=7b22617365727665723b32223a223735653666373531646235626137316163323566306266666362663738303933434e696e71663046454e614e38596d5779374675227d; tfstk=cuqRBsxbFsfky6MYQzQDAca15dBcZMb-tscwvO9biiBcGu-diFqgXMF6mAd-DtC..; l=eBEHImfVO6-uiqOUBOfwourza77OSIRAguPzaNbMiOCPOK1p5YIlWZSw5uL9C3GVhs6wR3Scn0QQBeYBqCqzObH43LD5raDmn; isg=BFRUAnAM9605KGPthVS6ApvuJZLGrXiXZOEfQe414F9i2fQjFr1IJwpX2dHBIbDv',
            # 'Host': URL_DOMAIN,     # host不能用DOMAIN？
            'Upgrade-Insecure-Requests': '1',
            # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'
         }
UA_STANDBY = 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18362'

"""通用方法"""
def get_proxy(cur_url,cur_adcode,cur_header):
    try:
        ip = proxy_gen.output_proxy(cur_adcode,cur_header,cur_url)
        if ip == '121':
            ic('output proxy from jg')
            ip == proxy_gen_jg.output_proxy(cur_adcode,cur_header,cur_url)     # 从不同途径获取IP
        return ip
    except Exception as ex:
        ic(traceback.format_exc())
        ic(f'\033[41;32;8m output proxy error:{ex}\033[0m')
        return None

def get_fake_ua():
    try:
        fake_ua = ua_gen.random
        header_common['user-agent'] = fake_ua
        return header_common
    except Exception as ex:
        ic(f'\033[41;32;1m output header error:{ex}\033[0m')
        return None

def get_redis_connection():
    redis_connection_pool = redis.ConnectionPool(host='127.0.0.1',port=6379,db=4,decode_responses=True,health_check_interval=5555)
    rcon = redis.Redis(connection_pool=redis_connection_pool)
    return rcon

def get_mongodb_collection():
    mongo_conn = pymongo.MongoClient(host='127.0.0.1',port=27017)
    m_db = mongo_conn.get_database('charge_station')     # 可指定数据库，但base和detail不放在同一个collection
    return m_db

def re_match_df(row):
    adcode = row['adcode']
    district_name = row['中文名']
    re_match_res1 = re.search(r'^\d{4}00',str(adcode))
    re_match_res2 = re.search(r'(^\d{2}0000)',str(adcode))
    re_match_res3 = re.search(r'(\S+市辖区)',str(district_name))
    if (re_match_res1 is None) and (re_match_res2 is None) and (re_match_res3 is None):     # 非省非城市非辖区
        row['role'] = 'for_crawl'
    elif (re_match_res1 is None) and (re_match_res2 is None) and (re_match_res3 is not None):
        row['role'] = 'for_nothing'
    else:
        row['role'] = 'for_map'
    return row

def start_loop(crawl_loop):
    asyncio.set_event_loop(crawl_loop)
    crawl_loop.run_forever()

"""主逻辑"""
# 从excel中获得区域和行业编码
def load_base_info_from_amap():
    df_poi_map = pd.read_excel('..\\..\\amap\\amap_poicode.xlsx',sheet_name='POI分类与编码（中英文）',dtype={'NEW_TYPE':object})
    s_charge_station = df_poi_map[df_poi_map['中类'] == '充电站']     # 此处为充电站
    biz_type = s_charge_station.NEW_TYPE.to_list()[0]
    # df_city_map = pd.read_excel('..\\..\\amap\\AMap_adcode_citycode.xlsx',sheet_name='Sheet1',dtype=object)
    df_city_map = pd.read_excel('..\\..\\amap\\AMap_adcode_citycode.xlsx',sheet_name='Sheet2',dtype=object)
    # df_valid_districts = df_city_map[21:][~(adcode_filter | districtname_filter)]     # 注：北京市区充电站返回数据异常，暂时不予考虑
    # df_valid_districts = df_city_map[~(adcode_filter | districtname_filter)]
    df_city_separate_into_roles = df_city_map.apply(re_match_df, axis=1)
    # df_city_exclude_bj = df_city_separate_into_roles[21:]
    df_city_exclude_bj = df_city_separate_into_roles
    df_valid_districts = df_city_exclude_bj[df_city_exclude_bj['role'] == 'for_crawl']
    return biz_type,df_valid_districts

# 下载所有充电桩基础信息
def download_all_charge_station_poi_basic_info(biz_type,df_city_groups):
    try:
        ic(f"爬取充电站基础进程号：{os.getpid()}")
        r_con_base_operate = get_redis_connection()     # 减少重复创建连接
        m_db = get_mongodb_collection()
        coll_charge_station_base = m_db.get_collection('charge_station_base')
        amap_api_used = 0     # 控制高德API使用量，超过29990，将暂停API爬取，记录暂停点,发出爬取结束信号
        for i,df_city_group in enumerate(df_city_groups):
            df_district = pd.DataFrame(df_city_group[1])     # 第一项为省份编号，第二项为dataFrame
            district_list = df_district['adcode'].to_list()
            last_city_code = ''
            for adcode in district_list:
                cur_city_code = re.sub(r'(?<=\d{4})(\d{2})', '00', str(adcode))
                ic(f"开始爬取 地区：{adcode} 基础信息")
                if last_city_code != cur_city_code:
                    # 在此处发上一个城市爬取完毕的信号(注意两个方法使用的city_code不同)
                    if last_city_code != '':
                        ic(f"charge base info in city_{last_city_code} complete")
                        msg_base_content = '_'.join(['base',last_city_code])
                        r_con_base_operate.lpush(BASE_MSG_LIST,msg_base_content)
                    last_city_code = cur_city_code
                used_count = download_charge_station_poi_by_district(biz_type,adcode,cur_city_code,r_con_base_operate,coll_charge_station_base)
                amap_api_used += used_count
                if amap_api_used > 29955:
                    break     # 每日API调用次数为30000，当使用超过29955时，继续爬取会有危险，此处未完成，退出方式需要重新考虑
            # 所有区域爬取完毕，发出done信号(先发最后一个区域的爬取结束的信号)
            ic(f"charge base info in city_{last_city_code} complete")
            msg_base_content = '_'.join(['base', last_city_code])
            r_con_base_operate.lpush(BASE_MSG_LIST, msg_base_content)
        msg_base_content = '_'.join(['base','done'])
        r_con_base_operate.lpush(BASE_MSG_LIST, msg_base_content)
        ic(f"amap api used:{amap_api_used}")
    except Exception as ex:
        ic(f"{ex} in 基础信息")
        ic(traceback.format_exc())

# 调用高德地图api,将充电站基础信息存入mongoDB，并遍历出充电站id和区号存入redis（生产）
def download_charge_station_poi_by_district(biz_type,district_code,cur_city_code,r_con_base_operate,coll_charge_station_base):
    page_num = 25
    cur_page = 1
    page_count = 0
    total_count = 0
    api_used = 0
    an_count = 0
    while True:
        try:
            resp = requests.get(f'https://restapi.amap.com/v3/place/text?types={biz_type}&city={district_code}&citylimit=true&output=json&offset={page_num}&page={cur_page}&key=95b675159054714130c2b834f47fd4a1')
            api_used += 1
            resp_dict = resp.json()
            infocode = resp_dict['infocode']
            pois = resp_dict['pois']
            if (len(pois) == 0) and (infocode == '10000'):     # 返回为0时，即为爬取结束
                break
            elif infocode != '10000':
                ic(f'请求API错误，错误代码：{infocode}')
                raise RuntimeError
            else:
                # 从api拿到的数据，一次读取id，写入redis（key里面带上adcode）
                push_id_into_redis(pois,district_code,cur_city_code,r_con_base_operate)
                # 从api直接取回的数据存mongoDB
                coll_charge_station_base.insert_many(pois)     # 储存时只能一页一页的存
                ic(f"第{cur_page}页 have load {len(pois)} items")
                if page_count == 0:
                    an_count = resp_dict["count"]
                    page_count = math.ceil(int(resp_dict['count'])/page_num)
                if cur_page < page_count:
                    cur_page += 1
                    total_count += len(pois)
                else:
                    total_count += len(pois)     # 在退出前计入最后一页的数量
                    break
                time.sleep(0.05)     # 并发量上限
        except Exception as ex:
            ic(f"{ex},district_code:{district_code},page_no:{cur_page}")
            ic(traceback.format_exc())
            continue
    if int(an_count) > total_count:
        ic(f'\033[1;30;45m地区_{district_code} 充电站基础信息下载完毕,共{page_count}页,宣称:{an_count}条记录,实际返回:{total_count}条记录\033[0m;')
    else:
        ic(f'地区_{district_code} 充电站基础信息下载完毕,共{page_count}页,宣称:{an_count}条记录,实际返回:{total_count}条记录')
    return api_used

# 单独把id写入stream
def push_id_into_redis(poi_list,district_code,cur_city_code,r_con_push_base):
    for poi in poi_list:
        id = poi['id']
        stream_name = '_'.join([STREAM_POI_ID_NAME,cur_city_code])
        r_con_push_base.xadd(stream_name,{'id':id,'district_code':district_code})     # 带district_code方便对区域汇总分析

# 爬取充电桩详情(消费),poi_ids和adcode都从stream中获得(处理完一个流,给一个流的完成标记,再处理下一个流),city_code从流名字中获得
def download_charge_station_detail_each_city():
    ic(f"爬取充电站详情进程号：{os.getpid()}")
    r_con_crawl_detail = get_redis_connection()
    # 单独创建子线程运行执行协程的循环
    if sys.platform == 'win32':
        crawl_loop = asyncio.ProactorEventLoop()     # 获得主线程中的循环，在子线程中设置循环
    else:
        crawl_loop = asyncio.get_event_loop()
    thread_run_coro = threading.Thread(target=start_loop,args=(crawl_loop,),daemon=True)     # 设置守护线程使得主线程正常结束后此进程能顺利结束
    thread_run_coro.start()
    # 创建消费组后开始分线程消费
    executor = ThreadPoolExecutor(max_workers=5)
    group_name = 'base_reader'
    consumer_name_table = {'Ashin':'waiting','Monster':'waiting','Ming':'waiting','Masa':'waiting','Stone':'waiting'}
    while True:
        try:
            # 主线程阻塞监听base_msg_list，从消息列表中读取新消息正则出city_code
            base_msg = r_con_crawl_detail.brpop(BASE_MSG_LIST,timeout=5555)     # 返回tuple,第一项为key,第二项为消息内容
            msg_content = base_msg[1]
            ic(f'read msg from base_line:{msg_content}')
            re_res = re.search(r'base_(\S+)',msg_content)
            if re_res:
                msg_suffix = re_res.group(1)
                if msg_suffix == 'done':
                    msg_content = '_'.join(['detail','done'])
                    r_con_crawl_detail.lpush(DETAIL_MSG_LIST,msg_content)     # 发出二级生产结束的信号
                    break     # 读取到生产结束的信号，此时charge_station_detail的爬取任务已经全部结束，可以退出
                else:
                    city_code = msg_suffix
                    # 开始处理poi_id_base流
                    stream_name = '_'.join([STREAM_POI_ID_NAME,city_code])
                    ic(f'deal with stream {stream_name}')
                    r_con_crawl_detail.xgroup_create(stream_name,group_name,'0')     # 直接新建消费组，多个stream消费组不重复
                    task_read_and_crawl = []
                    for j in range(0,5):
                        sub_task = executor.submit(read_charge_station_base,r_con_crawl_detail,stream_name,group_name,
                                                   consumer_name_table,crawl_loop)
                        task_read_and_crawl.append(sub_task)
                    # 所有子线程完成任务后自然通过此处，发出stream_cur_city_code消费完毕信号,从msg读取下一个消息，处理下一个流
                    status_read_and_crawl = wait(task_read_and_crawl,return_when=ALL_COMPLETED)
                    ic(f"task {stream_name} status:{status_read_and_crawl}")
                    msg_detail_content = '_'.join(['detail',city_code])
                    r_con_crawl_detail.lpush(DETAIL_MSG_LIST,msg_detail_content)
                    # 删除已消费的流（暂不开放）
            else:
                raise Exception(f'invalid msg:{msg_content}')
        except Exception as ex:
            ic(f"{ex} in 详情")
            ic(traceback.format_exc())
    ic(thread_run_coro.is_alive())

# 从redis读取poi_id,提交到coroutine
def read_charge_station_base(r_con_crawl_detail,stream_name,group_name,consumer_name_table,crawl_loop):
    cur_thread = threading.current_thread()
    if cur_thread.name not in consumer_name_table:
        ic('new separate thread name')
        time.sleep(0.05)
        table_tmp = {k: v for k, v in consumer_name_table.items() if v == 'waiting'}
        ran_ind = random.randint(0, len(table_tmp) - 1)
        key = list(table_tmp.keys())[ran_ind]
        cur_thread.setName(key)
        consumer_name_table[key] = 'used'
    consumer = cur_thread.name
    while True:
        try:
            ret = r_con_crawl_detail.xreadgroup(group_name, consumer, {stream_name:'>'}, count=5)
            if ret:
                for poi_base_msg in ret[0][1]:     # ret[0][0]是stream_name(此时是base)
                    msg_id = poi_base_msg[0]
                    poi_base_data = poi_base_msg[1]
                    adcode = poi_base_data['district_code']
                    city_code_re = re.sub(r'(?<=\d{4})(\d{2})','00',adcode)
                    province_code = re.sub(r'(?<=\d{2})(\d{4})','0000',adcode)
                    poi_base_data['city_code'] = city_code_re
                    poi_base_data['province_code'] = province_code
                    poi_base_data['create_time'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
                    cur_header = get_fake_ua()
                    if cur_header is None:
                        cur_header = UA_STANDBY
                    the_proxy_str = get_proxy(URL_DOMAIN,adcode,cur_header)
                    # http_kind = URL_DOMAIN.split(':')[0]
                    # the_proxy = '://'.join([http_kind,the_proxy_str])
                    asyncio.run_coroutine_threadsafe(
                        crawl_coro(poi_base_data,r_con_crawl_detail,msg_id,cur_header,the_proxy_str,stream_name,group_name)
                        ,crawl_loop)
            else:
                group_info = r_con_crawl_detail.xinfo_groups(stream_name)
                if group_info[0]['pending'] == 0:
                    ic(f'{consumer} consume over')
                    # 线程工作在这里将结束，但无权修改crawl_done状态（消费者爬取一个stream的工作完成，等待下一个stream）
                    break
                else:
                    continue
        except Exception as ex:
            ic(f'unknown exceptions happened as {ex}')
            ic(traceback.format_exc())

# 爬取协程
async def crawl_coro(poi_base_data,r_con_crawl_detail,msg_id,cur_header,the_proxy,stream_name_base,group_name):
    try:
        poi_id = poi_base_data['id']
        district_code = poi_base_data['district_code']
        city_code = poi_base_data['city_code']
        ack_type = 'well'
        async with asyncio.Semaphore(5):     # 怀疑在run_forever的状态下的有效性
            proxy_dict = {"https":f"http://{the_proxy}"}
            async with httpx.AsyncClient(headers=cur_header,proxies=proxy_dict,timeout=30,limits=limits) as cli:
                url_poi = URL_DOMAIN + poi_id
                resp = await cli.get(url_poi)
                resp_code = resp.status_code
                if resp_code == 200:
                    page_text = resp.text
                    pq_page = pq(page_text)
                    scripts = pq_page('script')[0]
                    data_area = re.search('window.detail = (.*)', scripts.text, re.M)
                    if data_area:
                        data_area_ele = data_area.group(1)
                        data_poi = json.loads(data_area_ele.replace(';', ''))
                        data_poi_more = {**data_poi,**poi_base_data}     # 合并两个字典的'神奇'方法(如有字段重复后者覆盖前者)
                        stream_name_detail = '_'.join([STREAM_POI_DETAIL_NAME,city_code])
                        r_con_crawl_detail.xadd(stream_name_detail,{'poi_info':json.dumps(data_poi_more,ensure_ascii=False)})
                    else:
                        ic(f'could not find detail info at {poi_id}')
                        # 页面解析失败,将此类失败直接放入error_stream并ack
                        r_con_crawl_detail.xadd(STREAM_CRAWL_ERROR,{'poi_info':json.dumps(poi_base_data,ensure_ascii=False)})
                        ack_type = 'lost'
                elif resp_code == 403:
                    raise Exception('banned for 403')
                else:
                    ic(f'other status_code:{resp_code}')     # 除反爬外，也有可能出现站点失效等问题
                    raise Exception('other exceptions')
    except Exception as ex:     # 捕获超时类，此类将重试(和403反爬一样。其他异常将放到error_stream)
        ic(f'error poi_id:{poi_id}')
        ic(traceback.format_exc())
        if 'retry' in poi_base_data.keys():
            retry = int(poi_base_data['retry']) + 1
        else:
            retry = 1
        poi_base_data['retry'] = retry
        if retry >= 5:     # 多次尝试失败则放到error_stream,并ack
            r_con_crawl_detail.xadd(STREAM_CRAWL_ERROR, {'id':poi_id,'district_code':poi_base_data['district_code']})
        else:     # 否则放回到原stream中，此时redis记录的id已经变化，前一个必须ack
            r_con_crawl_detail.xadd(stream_name_base, {'id':poi_id,'district_code':district_code,'retry':retry})
        # 这里爬取超时的IP不用刻意处理，在下次提取IP前验证IP有效性时会自动检测
        ack_type = 'damn'
    finally:
        ic(f'{ack_type} ack from id {msg_id} to base')
        r_con_crawl_detail.xack(stream_name_base, group_name, msg_id)     # 无论是否爬取成功,都需要回应原stream避免堵塞

# 从redis中取出数据写入mongoDB
def write_down_charge_station_data():
    ic(f"写入数据进程号：{os.getpid()}")
    r_conn_writer = get_redis_connection()
    m_db_charge_station = get_mongodb_collection()
    coll_charge_station_detail = m_db_charge_station.get_collection('charge_station_detail')
    '''
    解析detail_msg内容:
    1.如果是某个城市爬取完成的消息，从详情流中读取对应city_code获得城市数据并写入
    2.如果是详细爬取工作结束的消息，直接退出循环，进程结束
    '''
    try:
        while True:
            msg_obj = r_conn_writer.brpop("charge_station_detail_msg",timeout=5555)
            ic(f"receive msg from detail_line:{msg_obj}")
            re_res = re.search(r'detail_(\S+)',msg_obj[1])
            if re_res is None:
                raise Exception(f'invalid msg:{msg_obj} from stream_detail')
            msg_suffix = re_res.group(1)
            con_group_name = 'redis_to_mongodb'
            if msg_suffix == 'done':
                ic('write work complete')
                break
            else:
                write_tasks = []
                city_code = msg_suffix
                stream_name = '_'.join([STREAM_POI_DETAIL_NAME,city_code])
                r_conn_writer.xgroup_create(stream_name,con_group_name,0)     # 处理不同的流，需要新建消费组
                stream_len = r_conn_writer.xlen(stream_name)
                read_count = stream_len
                if stream_len > 25:
                    read_count = 25
                write_from_stream_done = False
                while not write_from_stream_done:
                    # 不需要再block
                    ret = r_conn_writer.xreadgroup(con_group_name,'writer',{stream_name:'>'},count=read_count)
                    if ret:
                        for stream_info in ret[0][1]:
                            msg_id = stream_info[0]
                            stream_data = stream_info[1]
                            coro = write_data_into_mongodb(r_conn_writer,coll_charge_station_detail,stream_data,
                                                           msg_id,stream_name,con_group_name)
                            task = asyncio.ensure_future(coro)
                            write_tasks.append(task)
                        asyncio.get_event_loop().run_until_complete(asyncio.gather(*write_tasks))
                    else:
                        if r_conn_writer.xpending(stream_name,con_group_name)['pending'] == 0:
                            write_from_stream_done = True
                            ic(f'write charge_station_detail from {city_code} into mongodb complete')
                            # 写完一个流删除一个流（暂不开放）
                            r_conn_writer.delete(stream_name)
                        else:
                            ic('waiting')
                            continue
    except Exception as ex:
        ic(f"{ex} in 写数据")
        ic(traceback.format_exc())

# 写入mongoDB并ack
async def write_data_into_mongodb(r_conn_writer,m_coll,r_data,msg_id,stream_name,con_group_name):
    try:
        m_coll.insert_one(r_data)
        r_conn_writer.xack(stream_name,con_group_name,msg_id)
        ic(f'save charge_station_detail into mongoDB success,msg_id:{msg_id}')
    except Exception as ex:
        ic(traceback.format_exc())
        ic(f'save charge_station_detail into mongoDB failed, msg_id:{msg_id}')

# 创建aio连接
async def create_aio_session(cur_header):
    ic('新建aio-session')  # 无法在协程外创建aiohttp.clientSession，只能将其作为全局变量在调用时检查是否为None决定新建连接
    aio_tcp_conn = aiohttp.TCPConnector(verify_ssl=False, limit=5)  # 通过limit限制并发，视实际情况决定是否用verify_ssl规避bug
    s = aiohttp.ClientSession(headers=cur_header, timeout=aiohttp.ClientTimeout(total=25), connector=aio_tcp_conn)
    s.connector
    return s

# httpx关闭
async def close_httpx_client(client):
    await client.aclose()

if __name__ == '__main__':
    t_start = time.time()
    ic(f"主进程：{os.getpid()}")
    biz_type,df_city_district = load_base_info_from_amap()
    df_city_groups = df_city_district.groupby('citycode', as_index=False)     # 先按照城市分组
    process_executor = ProcessPoolExecutor(max_workers=3)
    task_download_basic = process_executor.submit(download_all_charge_station_poi_basic_info,biz_type,df_city_groups)
    task_crawl_detail = process_executor.submit(download_charge_station_detail_each_city)
    task_write_down_data = process_executor.submit(write_down_charge_station_data)
    rset = wait([task_download_basic,task_crawl_detail,task_write_down_data],return_when=ALL_COMPLETED)     # 顺序不要错
    # rset = wait([task_write_down_data,task_crawl_detail,task_write_down_data],return_when=FIRST_EXCEPTION)     # 顺序不要错
    t_end = time.time()
    ic(f'总耗时：{t_end-t_start}')


