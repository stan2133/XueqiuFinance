# -*- coding: utf-8 -*-
import requests
import os
import urllib
import json
import pymongo
import datetime
import time
__author__ = 'stan'

_headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, sdch, br",
    "Accept-Language": "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,ja;q=0.2",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}

os.chdir('/Users/stan/Documents/Quant')
# save csv file to 'Quant' folder
_csvUrl = "http://api.xueqiu.com/stock/f10/{sheet_type}.csv?symbol={stock_id}&page={page}&size={size}"
_IndustryUrl = "https://xueqiu.com/stock/cata/stocklist.json?page={page}&size={size}&order={order}&orderby={orderby}&exchange={exchange}&plate="
_jsonUrl = "https://xueqiu.com/stock/f10/{sheet_type}.json?symbol={stock_id}&page={page}&size={size}"
_sheet_type = ['incstatement', 'cfstatement', 'balsheet' ]


def getJson(sheet_type, stock_id, cookie, page=1, size=10000):
    """
    @:param sheet_type:
    @:param stock_id:
    @:param cookie:
    @:param page:
    @:param size:
    """
    _orientUrl = _jsonUrl.format(sheet_type=sheet_type, stock_id=stock_id, page=str(page), size=str(size))
    headers = _headers.copy()
    headers.update({'cookie': cookie})
    r = requests.get(_orientUrl, headers=headers)
    if r.status_code == 200:
        print 'the Json file is requested'
        data = json.loads(r.text)
        data.update({'code': stock_id})
        return data
    else:
        print 'please check the cookie'


def getCsv(sheet_type, stock_id, page=1, size=10000):
    """
    @:param sheet_type:
    @:param stock_id:
    @:param cookie:
    @:param page:
    @:param size:
    """
    _orientUrl = _csvUrl.format(sheet_type=sheet_type, stock_id=stock_id, page=str(page), size=str(size))
    file_name = stock_id + sheet_type + '.csv'
    r = requests.get(_orientUrl, headers=_headers)
    if r.status_code == 200:
        print 'the csv file is requested'
        with open(file_name, "wb") as f:
            f.write(r.text.encode("UTF-8"))
    else:
        print 'please check the cookie'


def readStockByIndustry(plate, cookie, page=1, size=90, order='desc', orderby='pe_ttm', exchange='CN'):
    """
    @:param plate:
    @:param cookie:
    @:param page:
    @:param size:
    @:param order:
    @:param order_by:
    @:param exchange:
    """
    _orientUrl = _IndustryUrl.format(page=str(page), size=str(size), order=order, orderby=orderby, exchange=exchange) + urllib.pathname2url(plate)
    print _orientUrl
    headers = _headers.copy()
    headers.update({'cookie': cookie})
    # print headers
    r = requests.get(_orientUrl, headers=headers)
    if r.status_code == 200:
        data = json.loads(r.text)
        print 'the json file is requested'
        return data['stocks']

    else:
        print 'please check the cookie'


def stock_parsing(data, dbclient, date, db='Xueqiu', table='stockbyindustry'):
    for item in data:
        item.update({'date': date})
        dbclient[db][table].replace_one({'code': item['code'], 'date': item['date']}, item, upsert=True)


if __name__ == '__main__':
    dbclient = pymongo.MongoClient('mongodb://127.0.0.1')
    cookie = "s=8s125bbdhi; xq_a_token=353719375a63d9c5504083a962c65c231dfb715c; xqat=353719375a63d9c5504083a962c65c231dfb715c; xq_r_token=8063e0977bb80c6b9d90791a554d63e6285fef66; xq_is_login=1; u=3420308671; xq_token_expire=Sat%20Oct%2008%202016%2003%3A35%3A29%20GMT%2B0800%20(CST); bid=cfde5050318cc5826136d5dac4483954_it0g8w9o; __utma=1.1190508203.1473708993.1473789906.1473886061.3; __utmb=1.14.9.1473886250174; __utmc=1; __utmz=1.1473708993.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); Hm_lvt_1db88642e346389874251b5a1eded6e3=1473708927; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1473886918"
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    Ind_data = readStockByIndustry("传媒", cookie)
    stock_set = set()
    for item in Ind_data:
        stock_set.add(item['symbol'])
    print stock_set
    stock_parsing(Ind_data, dbclient, date)
    for s_type in _sheet_type:
        for symbol in stock_set:
            Fin_data = getJson(s_type, symbol, cookie)
            dbclient['Xueqiu'][u"media"+s_type].replace_one({'code': Fin_data['code'], 'date': date}, Fin_data, upsert=True)
            time.sleep(30)





