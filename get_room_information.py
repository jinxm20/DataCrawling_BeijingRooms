import numpy as np
import requests
from bs4 import BeautifulSoup
import pandas as pd
from multiprocessing.pool import ThreadPool
from datetime import datetime
import random
import time
import os.path
import threading

lock = threading.Lock()

total_count = 1
g_df_middle_district_house_saved = pd.DataFrame({'big district name': [], 'middle district name': [], 'page num': [], 'middle house index': [], 'url': []})

def print_process_info(big_district, middle_district, page):
    # Use a breakpoint in the code line below to debug your script.
    timestr = datetime.now().strftime('%Y%m%d%H%M')
    print(f'...............[{timestr}][{big_district}][{middle_district}][{page}] processing ...............')

def check_site_blocked() :
    timestr = datetime.now().strftime('%Y%m%d%H%M')
    print('Time[' + timestr +']. Url is blocked. Verify the site. ')
    delay_time = random.uniform(5, 10)
    delay_time = round(delay_time, 2)
    time.sleep(delay_time)

def get_district_list():

    # df_big_district_list = pd.DataFrame({'idx':[], 'district':[]})
    df_big_district_list = pd.DataFrame({'district': []})
    while 1:
        url_base = "https://bj.ke.com/ershoufang/dongcheng"
        url = requests.get(url_base)
        html = url.content
        soup = BeautifulSoup(html, "html.parser")
        check_block = soup.find_all('div', id="captcha")
        if len(check_block) > 0 :
            check_site_blocked()
        else :
            break

    idx = 0
    list = []
    for distLink in soup.find_all('a',"CLICKDATA"):
        href = distLink.get('href')
        list.append('https://bj.ke.com'+href)
        idx += 1
        # new_df = pd.DataFrame([[1, href]], columns=['idx', 'district'])
        # df_big_district_list.append(new_df)

    df_big_district_list = pd.DataFrame(list, columns=['district'])

    df_big_district_list = df_big_district_list[0:17]
    # print(df_big_district_list)

    return df_big_district_list

def soup_total_house_num_for_district(str) :

    url = requests.get(str)
    soup = BeautifulSoup(url.content, "html.parser")

    # get district name to add column to df_roomnum
    str = str[29:-1]

    # 각 구역 방 개수 세기
    big_district_list = []
    room_num_list = []
    is_more_than_3000 = []

    for num_room in soup.find('h2', "total fl").find_all('span'):
        result = [[str], [int(num_room.get_text())], [1 if int(num_room.get_text()) > 3000 else 0 ]]

        big_district_list.append(str)
        room_num_list.append(int(num_room.get_text()))
        is_more_than_3000.append(1 if int(num_room.get_text()) > 3000 else 0)

    # df  = pd.DataFrame([big_district_list,room_num_list, is_more_than_3000], columns=['district name', 'number of room', 'more than 3000'])

    delay_time = random.uniform(1, 3)
    delay_time = round(delay_time, 2)
    time.sleep(delay_time)

    # return df
    return [big_district_list, room_num_list, is_more_than_3000]

def get_total_house_num_for_district(df_big_district_list):
    # df_roomnum = pd.DataFrame({'district name':[], 'number of room':[], 'more than 3000':[]})
    df_roomnum = pd.DataFrame(columns = ['district name', 'number of room', 'more than 3000'])

    idx = 0
    with ThreadPool(10) as pool:
        for result in pool.map(soup_total_house_num_for_district,  df_big_district_list['district']):
            df_roomnum.loc[idx] = [result[0][0], result[1][0], result[2][0]]
            idx += 1

    pool.close()
    pool.join()

    return df_roomnum

# get middle district urls for each big districts
def soup_middle_district_info(big_district_name) :
    str = 'https://bj.ke.com/ershoufang/' + big_district_name + '/'
    while 1:
        url = requests.get(str)
        soup = BeautifulSoup(url.content, "html.parser")
        check_block = soup.find_all('div', id="captcha")
        if len(check_block) > 0:
            check_site_blocked()
        else:
            break

    for num_room in soup.find('h2', "total fl").find_all('span'):
        total_house_cnt = int(num_room.get_text())

    atags = soup.select("#beike > div.sellListPage > div.m-filter > div.position > dl > dd > div > div")[1].select('a')

    # column = 'big district name', 'middle district name'
    big_district_list = []
    middle_district_list = []

    for an in atags:
        href = an['href']

        big_district_list.append(big_district_name)
        middle_district_list.append(href[12:-1])

    delay_time = random.uniform(1, 3)
    delay_time = round(delay_time, 2)
    time.sleep(delay_time)

    return [big_district_list, middle_district_list]

def get_middle_district_info(df_big_district_list) :
    df_middle_district_info = pd.DataFrame(columns=['big district name', 'middle district name'])
    idx = 0
    with ThreadPool(10) as pool:
        for result in pool.map(soup_middle_district_info, df_big_district_list['district name']):
            # df_middle_district_info += result
            cnt = len(result[0])
            for i in range(cnt) :
                df_middle_district_info.loc[idx] = [result[0][i], result[1][i]]
                idx += 1

    pool.close()
    pool.join()
    return df_middle_district_info

def soup_big_distict_house_url_new(big_distict_name, page_num):

    df_big_distict_house_info = pd.DataFrame(columns=['big district name', 'middle district name', 'page num', 'middle house index', 'url'])
    each_url = 'https://bj.ke.com/ershoufang/' + big_distict_name + '/'
    while 1:
        url = requests.get(each_url + 'pg' + str(page_num))
        soup = BeautifulSoup(url.content, "html.parser")

        check_block = soup.find_all('div', id="captcha")
        if len(check_block) > 0 :
            check_site_blocked()
            continue
        else :
            break

    pageresult = soup.find_all('a', "VIEWDATA CLICKDATA maidian-detail")
    if len(pageresult) > 1:
        mid_idx = 1
        for j in pageresult:
            href = j.get('href')

            df_big_distict_house_info.loc[len(df_big_distict_house_info)] = [big_distict_name, '', page_num, mid_idx, href]

            mid_idx += 1

    delay_time = random.uniform(0.1, 1.0)
    delay_time = round(delay_time, 2)
    time.sleep(delay_time)

    return df_big_distict_house_info

def get_big_district_house_url_new(distrinct_info, start_page_num, end_page_num) :
    global g_df_middle_district_house_saved
    df_bigname = distrinct_info

    for page in range(start_page_num, end_page_num) :
        result = soup_big_distict_house_url_new(df_bigname, page)
        g_df_middle_district_house_saved = pd.concat([g_df_middle_district_house_saved, result])

    return g_df_middle_district_house_saved

def soup_middle_distict_house_url_new(big_distict_name, middle_distict_name, page_num):

    df_middle_distict_house_info = pd.DataFrame(columns=['big district name', 'middle district name', 'page num', 'middle house index', 'url'])
    each_url = 'https://bj.ke.com/ershoufang/' + middle_distict_name + '/'
    while 1:
        url = requests.get(each_url + 'pg' + str(page_num))
        soup = BeautifulSoup(url.content, "html.parser")

        check_block = soup.find_all('div', id="captcha")
        if len(check_block) > 0 :
            check_site_blocked()
            continue
        else :
            break

    pageresult = soup.find_all('a', "VIEWDATA CLICKDATA maidian-detail")
    if len(pageresult) > 1:
        mid_idx = 1
        for j in pageresult:
            href = j.get('href')

            df_middle_distict_house_info.loc[len(df_middle_distict_house_info)] = [big_distict_name, middle_distict_name, page_num, mid_idx, href]

            mid_idx += 1

    delay_time = random.uniform(0.1, 1.0)
    delay_time = round(delay_time, 2)
    time.sleep(delay_time)

    return df_middle_distict_house_info

def get_middle_district_house_url_new(distrinct_info, start_page_num, end_page_num) :
    global g_df_middle_district_house_saved
    df_bigname = distrinct_info[0]
    df_middlename = distrinct_info[1]

    for page in range(start_page_num, end_page_num) :
        result = soup_middle_distict_house_url_new(df_bigname, df_middlename, page)
        g_df_middle_district_house_saved = pd.concat([g_df_middle_district_house_saved, result])

    return g_df_middle_district_house_saved

def get_soup_middle_district_house_count(big_district_name, middle_district_name) :
    total_house_cnt = 0
    str = 'https://bj.ke.com/ershoufang/' + big_district_name + '/' + middle_district_name + '/'
    while 1:
        url = requests.get(str)
        soup = BeautifulSoup(url.content, "html.parser")
        check_block = soup.find_all('div', id="captcha")
        if len(check_block) > 0:
            check_site_blocked()
        else:
            break

    for num_room in soup.find('h2', "total fl").find_all('span'):
        total_house_cnt = int(num_room.get_text())

    return total_house_cnt

#파라미터: 각 방의 url, 각 방의 이름 가격...정보 제공하는 함수
def needed_room_info_list(each_url):
    global total_count
    global g_df_middle_district_house_saved
    index = 0
    headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'}

    while 1 :
        url = requests.get(each_url,headers=headers)
        url.encoding = 'utf-8'
        soup = BeautifulSoup(url.content, "html.parser")

        check_block = soup.find_all('div', id="captcha")
        if len(check_block) > 0:
            check_site_blocked()
        else:
            break

    col_info = ['房屋户型','建筑面积', '所在楼层','房屋朝向' ,'装修情况', '梯户比例', '供暖方式', '配备电梯']
    infolist = pd.DataFrame({}) #all
    info = pd.DataFrame({}) #organized

    df_url = pd.DataFrame({'url': []})
    df_url.loc[len(df_url)] = [each_url]

    name = pd.DataFrame({'name': []})
    for n in soup.find_all("h1", 'main'):
        each_name = n.get_text().replace('\n','').replace(' ','')
        name.loc[len(name)] = [each_name]

    price = pd.DataFrame({'价格(万）':[]})
    for p in soup.find_all("span",'total'):
        each_price = p.get_text()
        price.loc[len(price)] = [each_price]

    #price 기록하는 데이터프레임
    Average_price = pd.DataFrame({'平均价格(元/平米）': []})
    for AP in soup.find_all("span", 'unitPriceValue'):
        each_Average_price = AP.get_text()
        Average_price.loc[len(Average_price)] = [each_Average_price]

    for info in soup.select('#introduction > div > div > div.base > div.content > ul > li '):
        roominfo_text = info.get_text()


        info = pd.DataFrame({})

        infolist[index] = [roominfo_text]
        index += 1

    for all_info in infolist.loc[0]: #j all_info
        for needed_info in col_info: #i needed info

            if needed_info == all_info[0:4]:
                # print(j[0:4])
                info[all_info[0:4]] = [all_info[4:]]
                break

            elif needed_info != all_info[0:4]:
                continue

    for num in range(len(col_info)):
        if col_info[num] not in info.columns:
            info[col_info[num]] = ["none"]

    info = pd.concat([df_url, name, price, Average_price,info],axis = 1)
    #info = info.reset_index()

    delay_time = random.uniform(0.3, 0.5)
    delay_time = round(delay_time, 3)
    time.sleep(delay_time)

    lock.acquire()

    for n in info.columns:
        if (n not in g_df_middle_district_house_saved.columns):

            g_df_middle_district_house_saved.insert(len(g_df_middle_district_house_saved.columns), n, '')

    for v in info.columns:
        g_df_middle_district_house_saved.loc[g_df_middle_district_house_saved[ 'url'] == each_url , [v]] = info.iloc[0][v]

    if (total_count % 100) == 0:
        timestr = datetime.now().strftime('%Y%m%d%H%M')
        print('[' + timestr + '] ' + 'Processing total : ' + str(total_count))

        g_df_middle_district_house_saved.to_csv('house_info_result.csv', encoding='utf-8')

    total_count += 1

    lock.release()
    return total_count

def get_house_detail_info(house_url_list) :
    with ThreadPool(4) as pool:
        for result in pool.map(needed_room_info_list,  house_url_list):
            print('The End of Pool. Total Processed  : [' + str(result) + ']')

    pool.close()
    pool.join()

    return

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    df_big_district_list = get_district_list()
    district_list = df_big_district_list['district']

    # load the previous result
    if os.path.isfile('./df_big_district_house.csv'):
        g_df_middle_district_house_saved = pd.read_csv('./df_big_district_house.csv')

    else:
        g_df_middle_district_house_saved = pd.DataFrame(
            columns=['big district name', 'middle district name', 'page num', 'middle house index', 'url'])

    # load the previous result
    if os.path.isfile('./df_middle_district_house.csv'):
        df_middle = pd.read_csv('./df_middle_district_house.csv')
        g_df_middle_district_house_saved = pd.concat([g_df_middle_district_house_saved, df_middle])


    df_district_room_num_info = get_total_house_num_for_district(df_big_district_list)

    df_district_less_room_num_info = df_district_room_num_info[df_district_room_num_info['more than 3000'] == 0]
    df_district_more_room_num_info = df_district_room_num_info[df_district_room_num_info['more than 3000'] == 1]

    print(df_district_more_room_num_info)
    print(df_district_less_room_num_info)

    # Get the big district name not saving the processed excel file.
    df_district_less_room_num_info = df_district_less_room_num_info[~df_district_less_room_num_info['district name'].isin(g_df_middle_district_house_saved['big district name'])]

    for row in df_district_less_room_num_info.itertuples(index=False):
        big_district_name = row[0]
        last_page = row[1] // 30
        get_big_district_house_url_new(big_district_name, 1, last_page + 2)
        g_df_middle_district_house_saved.to_csv("df_middle_district_house.csv")

    # remake df_big_district_list with 'more than 3000' is 1
    df_big_middle_match = get_middle_district_info(df_district_more_room_num_info)
    df_big_middle_match.to_csv("df_middle_district_list.csv")

    df_not_processed = df_big_middle_match
    # filter the processed the middle district compared with the save csv file
    df_not_processed = df_not_processed[~df_not_processed['middle district name'].isin(g_df_middle_district_house_saved['middle district name'])]

    for row in df_not_processed.itertuples(index=False) :
        big_district_name = row[0]
        middle_distrct_name = row[1]
        last_page = get_soup_middle_district_house_count(big_district_name, middle_distrct_name) // 30
        get_middle_district_house_url_new([big_district_name, middle_distrct_name], 1, last_page + 2)
        g_df_middle_district_house_saved.to_csv("df_middle_district_house.csv")

    # load the previous result
    if os.path.isfile('./house_info_result.csv'):
        df_house_info_result = pd.read_csv('./house_info_result.csv')
    else:
        df_house_info_result = pd.DataFrame(
            columns=['big district name', 'middle district name', 'page num', 'middle house index', 'url', 'name'])

    # replace 'nan' to np.nan because 'nan' is saved when to_csv is called
    df_house_info_result.replace('nan', np.nan, inplace=True)
    df_not_processed_url_list = df_house_info_result[df_house_info_result['name'].isnull()]

    g_df_middle_district_house_saved = df_house_info_result
    tot = len(df_not_processed_url_list)

    print('Starting to get the houre info. [' + str(tot) + '........' )
    get_house_detail_info(df_not_processed_url_list['url'])
    print('The end of process ........')

