import requests
import random
import re
import time
import pandas as pd
from pyquery import PyQuery as pq

"""初始化芝麻表（芝麻代理中可供提取的城市和省份代码）"""
def get_proxy_source_df():
    go_ahead = True
    page_num = 1
    last_page = 1
    source_area_list = []
    while go_ahead:
        resp = requests.post('http://wapi.http.linkudp.com/index/api/get_city_code', {'page': page_num},
                             headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36'})
        if resp.status_code == 200:
            resp_data = resp.json()
            last_page = int(resp_data['ret_data']['last'])
            pq_page = pq(resp_data['ret_data']['list'])
            pq_rows = pq_page('tr:gt(0)')
            for i, pq_row in enumerate(pq_rows.items()):
                proxy_content = pq_row('td')
                source_area_list.append((proxy_content[1].text, '', proxy_content[2].text, proxy_content[3].text))
        else:
            print(f'error at page {page_num},err_code:{resp.status_code}')
        page_num += 1
        if page_num > last_page:
            go_ahead = False
        time.sleep(0.89)
    df_proxy_source_zm = pd.DataFrame(source_area_list, columns=['province', 'province_code', 'city', 'city_code'])
    df_proxy_source_zm['province_code'] = df_proxy_source_zm['city_code'].apply(lambda x: re.sub(r'(?<=\d{2})(\d{4})','0000', x))
    return df_proxy_source_zm

class ProxyUtil:
    def __init__(self):
        self.dynamic_proxy_table = {}
        self.proxy_select = None
        self.df_proxy_source_from_zm = get_proxy_source_df()
        self.proxy_recorder = open(r'logs\proxy_zm.log','a+',encoding='utf-8')
        self.proxy_recorder.write('*****proxy_zm produce begin*****\n')

    def proxy_logger(self,msg):
        record_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        self.proxy_recorder.write(f"{record_time}: {msg}")
        self.proxy_recorder.write('\n')
        self.proxy_recorder.flush()

    def get_proxy_from_zm(self,num,province_code,city_code,level_code):
        if level_code == 1:
            api_zm = f'http://webapi.http.zhimacangku.com/getip?num={num}&type=2&pro={province_code}&city={city_code}&yys=0&port=11&pack=115882&ts=1&ys=0&cs=1&lb=1&sb=0&pb=45&mr=1&regions='
            # api_zm = f'http://http.tiqu.letecs.com/getip3?num={num}&type=2&pro={province_code}&city={city_code}&yys=0&port=11&pack=115882&ts=1&ys=0&cs=1&lb=1&sb=0&pb=45&mr=1&regions=&gm=4'
        elif level_code == 2:
            api_zm = f'http://webapi.http.zhimacangku.com/getip?num={num}&type=2&pro=0&city=0&yys=0&port=11&pack=115882&ts=1&ys=0&cs=1&lb=1&sb=0&pb=45&mr=1&regions={province_code}'
            # api_zm = f'http://http.tiqu.letecs.com/getip3?num={num}&type=2&pro=0&city=0&yys=0&port=11&pack=115882&ts=1&ys=0&cs=1&lb=1&sb=0&pb=45&mr=1&regions={province_code}&gm=4'
        else:
            api_zm = f'http://webapi.http.zhimacangku.com/getip?num={num}&type=2&pro=0&city=0&yys=0&port=11&pack=115882&ts=1&ys=0&cs=1&lb=1&sb=0&pb=45&mr=1&regions=110000'
            # api_zm = f'http://http.tiqu.letecs.com/getip3?num={num}&type=2&pro=0&city=0&yys=0&port=11&pack=115882&ts=1&ys=0&cs=1&lb=1&sb=0&pb=45&mr=1&regions=110000&gm=4'
        resp_ip = requests.get(api_zm)
        if resp_ip.status_code == 200:
            resp_ip_code = resp_ip.json()['code']
            if resp_ip_code == 0:
                self.enrich_proxy_list(resp_ip.json()['data'])
            time.sleep(2.5)     # 调用成功后及时扩充到IP池，然后迅速等待，调用API间隔为2秒
            return resp_ip_code
        else:
            raise Exception('代理API不可用，请尽快处理')

    def enrich_proxy_list(self,proxy_zm):
        # self.proxy_logger(f"keys in dynamic_proxy_table:{self.dynamic_proxy_table.keys()}")
        proxy_list_city = []
        proxy_list_province = []
        for proxy_data in proxy_zm:
            proxy_str = ':'.join([proxy_data['ip'],str(proxy_data['port'])])
            proxy_data['banned_times'] = 0
            proxy_city_zm = proxy_data['city']
            se_picked = self.df_proxy_source_from_zm[self.df_proxy_source_from_zm['city'].str.contains(rf'\S?{proxy_city_zm}\S?')].head(1)
            if se_picked.shape[0] == 1:
                city_code_zm = se_picked.iloc[0]['city_code']
                province_code_zm = se_picked.iloc[0]['province_code']
                if city_code_zm in self.dynamic_proxy_table.keys():
                    proxy_list_city = self.dynamic_proxy_table[city_code_zm]
                if province_code_zm in self.dynamic_proxy_table.keys():
                    proxy_list_province = self.dynamic_proxy_table[province_code_zm]
                self.proxy_logger(f'{proxy_str} join in,from city {proxy_city_zm}')
            else:
                self.proxy_logger(f'proxy from unknown area or from beijing:{proxy_str}')
                area_city_code = '110000'
                area_province_code = '110105'     # 反正用不着
                proxy_list_city = self.dynamic_proxy_table[area_city_code]
                proxy_list_province = self.dynamic_proxy_table[area_province_code]
            proxy_entry = {proxy_str:proxy_data}
            proxy_list_city.append(proxy_entry)
            proxy_list_province.append(proxy_entry)
        self.dynamic_proxy_table[city_code_zm] = proxy_list_city
        self.dynamic_proxy_table[province_code_zm] = proxy_list_province

    def output_proxy(self,cur_adcode,cur_header,cur_url):
        """
        level_code对应级别列表：1：城市；2：省份；3：北京
        从芝麻表中查找该区域是需要用城市代码匹配还是需要用省份代码匹配，得到对应key_code
        在dynamic_proxy_table中查找，如找到，取出key_code下的代理池从中取出代理并验证后返回，否则向代理池中注入对应代理
        """
        province_code = re.sub(r'(?<=\d{2})(\d{4})', '0000', cur_adcode)
        cur_city_code = re.sub(r'(?<=\d{4})(\d{2})', '00', cur_adcode)
        if self.df_proxy_source_from_zm[self.df_proxy_source_from_zm['city_code'] == cur_city_code].shape[0] > 0:
            level_code = 1
            key_code = cur_city_code
        elif self.df_proxy_source_from_zm[self.df_proxy_source_from_zm['province_code'] == province_code].shape[0] > 0:
            level_code = 2
            key_code = province_code
        else:
            level_code = 3
            key_code = '110000'
        if key_code in self.dynamic_proxy_table.keys():
            proxy_list_tmp = self.dynamic_proxy_table[key_code]
            # 指定池中代理数量不够及时补充
            if len(proxy_list_tmp) < 3:
                try:
                    self.proxy_logger(f'enrich proxy_pool for {key_code} $$$$$')
                    enrich_res = self.get_proxy_from_zm(3-len(proxy_list_tmp), province_code, cur_city_code,level_code)
                    if enrich_res != 0:
                        if enrich_res == 121:     # 当且仅当返回121，且dynamic对应的proxy_pool为空时再换
                            if len(proxy_list_tmp) == 0:
                                self.proxy_select = str(enrich_res)
                                return self.proxy_select
                            else:
                                self.proxy_logger(f'enrich failed on {enrich_res},pick out proxy from rest')
                        else:
                            self.proxy_logger(f'enrich proxy_pool failed,at {key_code},err_code:{enrich_res}')
                            # return None     # 扩充IP池失败不用着急返回None，但报错需解决
                except Exception as ex:
                    self.proxy_logger(ex)
                    return None
            ran_index = random.randint(0,len(proxy_list_tmp)-1)
            proxy_entry = proxy_list_tmp[ran_index]
            proxy_str = list(proxy_entry.keys())[0]
            proxy_checked = self.check_proxy_valid(proxy_str,cur_header,cur_url,key_code,ran_index)
            if proxy_checked is None:
                # 检查IP失效的原因：先判断是否过期，再判断被banned次数
                now = time.time()
                expire_time = time.mktime(time.strptime(proxy_entry[proxy_str]['expire_time'],'%Y-%m-%d %H:%M:%S'))
                if now > expire_time:
                    self.proxy_logger(f'proxy {proxy_str} is expired,removed!')
                    if level_code == 3:
                        proxy_list_tmp.remove(proxy_entry)
                    else:
                        self.union_remove_proxy(cur_city_code,province_code,proxy_entry)
                    self.dynamic_proxy_table[key_code] = proxy_list_tmp
                    try:
                        self.proxy_logger(f'enrich proxy_pool for {key_code} *****')
                        enrich_res = self.get_proxy_from_zm(1, province_code, cur_city_code,level_code)
                        if enrich_res != 0:
                            if enrich_res == 121:
                                if len(proxy_list_tmp) == 0:
                                    self.proxy_select = str(enrich_res)
                                    return self.proxy_select
                                else:
                                    self.proxy_logger(f'enrich failed on {enrich_res},pick out proxy from rest')
                            else:
                                self.proxy_logger(f'enrich proxy_pool failed,at {key_code},err_code:{enrich_res}')
                                return None
                    except Exception as ex1:
                        self.proxy_logger(ex1)
                        return None
                else:
                    banned_times = proxy_entry[proxy_str]['banned_times']
                    if banned_times >= 5:
                        self.proxy_logger(f'proxy {proxy_str} is valid,removed!')
                        if level_code == 3:
                            proxy_list_tmp.remove(proxy_entry)
                        else:
                            self.union_remove_proxy(cur_city_code,province_code,proxy_str)
                        try:
                            self.proxy_logger(f'enrich proxy_pool for {key_code} #####')
                            enrich_res = self.get_proxy_from_zm(1, province_code, cur_city_code,level_code)
                            if enrich_res != 0:
                                if enrich_res == 121:
                                    if len(proxy_list_tmp) == 0:
                                        self.proxy_select = str(enrich_res)
                                        return self.proxy_select
                                    else:
                                        self.proxy_logger(f'enrich failed for {enrich_res},pick out proxy from rest')
                                else:
                                    self.proxy_logger(f'enrich proxy_pool failed,at {key_code},err_code:{enrich_res}')
                                    return None
                        except Exception as ex2:
                            self.proxy_logger(ex2)
                            return None
                    else:
                        self.proxy_logger(f'{proxy_str} not pass,just try again')
                self.output_proxy(cur_adcode,cur_header,cur_url)
            else:
                self.proxy_select = proxy_checked
        else:
            try:
                self.proxy_logger(f'init proxy_pool {key_code}')
                enrich_res = self.get_proxy_from_zm(3,province_code,cur_city_code,level_code)
                if enrich_res == 0:
                    self.output_proxy(cur_adcode,cur_header,cur_url)
                elif (enrich_res == 115) or (enrich_res == 111):
                    self.proxy_logger(f'err_code:{enrich_res},standby')
                    time.sleep(2.5)
                    # cur_adcode = '110105'     # 暂不可用时使用北京的proxy(完整爬取时再使用此语句)
                    self.output_proxy(cur_adcode,cur_header,cur_url)
                elif enrich_res == 121:
                    self.proxy_logger('zm-API is run out,change to jg')
                    self.proxy_select = str(enrich_res)
                    return self.proxy_select
                else:
                    raise Exception(f"enrich pool failed,Error code from API:{enrich_res}")
            except Exception as ex:
                self.proxy_logger(f"init except-msg:{ex}")
                self.proxy_select = str(enrich_res)
                return self.proxy_select
        return self.proxy_select

    def check_proxy_valid(self,proxy_str,cur_header,cur_url,key_code,index):
        http_kind = cur_url.split(':')[0]
        proxy_picked = {f'{http_kind}':f'{http_kind}://{proxy_str}'}
        retry_times = 0
        match_res = re.search('https?://[^/]*/',cur_url)
        if match_res is not None:
            url_domain = match_res.group()
        else:
            self.proxy_logger('url is invalid,check it again')
            return None
        while retry_times < 5:
            try:
                resp_con = requests.get(url_domain,proxies=proxy_picked,headers=cur_header,timeout=(5,25))
            except requests.ConnectTimeout as e:
                if retry_times == 5:
                    self.proxy_logger(f'{proxy_str} is offline')
                    return None
                else:
                    retry_times += 1
                    continue
            except requests.exceptions.ProxyError:
                self.proxy_logger(f'proxy {proxy_str} is banned for Proxy Error')
                invalid_proxy_data = self.dynamic_proxy_table[key_code][index]
                banned_times = int(invalid_proxy_data[proxy_str]['banned_times'])
                self.dynamic_proxy_table[key_code][index][proxy_str]['banned_times'] = (banned_times + 1)
                return None
            conn_code = resp_con.status_code
            if conn_code < 400:
                self.proxy_logger(f'{proxy_str} is effective,oh yeah~')
                return proxy_str
            else:
                self.proxy_logger(f'Err code {conn_code},proxy is {proxy_str}')
                if conn_code == 403:
                    self.proxy_logger(f'proxy {proxy_str} is banned for 403')
                    invalid_proxy_data = self.dynamic_proxy_table[key_code][index]
                    banned_times = int(invalid_proxy_data[proxy_str]['banned_times'])
                    self.dynamic_proxy_table[key_code][index][proxy_str]['banned_times'] = (banned_times + 1)
                    return None

    # 某个IP失效，对应的城市和省份列表中都需要删除(北京除外)
    def union_remove_proxy(self,city_code,province_code,proxy):
        if city_code in self.dynamic_proxy_table.keys():
            proxy_pool = self.dynamic_proxy_table[city_code]
            if proxy in proxy_pool:
                proxy_pool.remove(proxy)
        if city_code in self.dynamic_proxy_table.keys():
            proxy_pool = self.dynamic_proxy_table[province_code]
            if proxy in proxy_pool:
                proxy_pool.remove(proxy)


    def __del__(self):
        self.proxy_recorder.close()
        print('proxy_recorder closed')
