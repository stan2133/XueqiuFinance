# coding: utf-8
import pymongo
import pygal
import numpy as np
from spider import get_history_std,_headers,_historyUrl


_sheet_type = ['incstatement', 'cfstatement', 'balsheet']
# Create a list of string of digit number
_digit = [str(i) for i in range(1, 10)]


def parsing_each_stock(data):
    """
    @:param data: all stock financial statement record stored in MongoDB
    @:return result: a list of dicts(the statistic result of counting number on every digit number).
    """
    result = []
    for stat_class in data.keys():
        record = list(data[stat_class])
        record = record[0]
        # print record['code']
        useful_data = record['list']
        statistic = ben_ford(useful_data, stat_class)
        # print statistic
        result.append(statistic)
    return result


def clean_data(data):
    """
    @:param data: one stock financial statements including bal, cf and income statements stored in MongoDB
    @:return new_str_num: get all the first digit of all the numeric numbers in financial record.
    """
    new_str_num = []
    for item in data:
        keys = ["compcode","publishdate","begindate","enddate","reporttype","adjustdate","accstacode","accstaname"]
        for key in keys:
            try:
                item.pop(key)
            except Exception:
                pass
        clean_number = item.values()
        clean_number = [abs(x) for x in clean_number if isinstance(x, basestring)==False and x is not None]
        for number in clean_number:
            number = str(number)[0]
            new_str_num.append(number)
    return new_str_num


def ben_ford(useful_data, statement):
    """
    @:param useful_data: three stock financial statements including bal, cf and income statements stored in MongoDB
    @:param statement: the categorical of the financial statement
    @:return statistic: the counting number of every digits
    """
    stat = clean_data(useful_data)
    statistic = dict((x, stat.count(x)) for x in set(stat))
    statistic.pop('0')
    statistic.update({'statement': statement})
    return statistic


def gen_percentage(data):
    """
    @:param data: the return of parsing_each_stock
    @:return seq: the distributions of every stock in stock_sets as well as the standard distribution
    """
    sum_up = []
    for val in _digit:
        y = 0
        z = 0
        x = val
        for record in data:
            y += record[x]
            z += sum(record.values()[1:10])
        sum_up.append(float(y)/z)
    seq = dict({'sum_up': sum_up})
    y = 0
    for record in data:
        sep = []
        for val in _digit:
            x = val
            y = float(record[x])/sum(record.values()[1:10])
            sep.append(y)
        seq.update({record['statement']: sep})
    p = [np.log10(x+1)-np.log10(x) for x in range(1, 10)]
    seq.update({'original': p})
    return seq


def criterion(seq, Tol=0.2):
    """
    @:param seq: the return of gen_percentage
    @:param Tol: tolerance to make a decision whether the stock is fraud
    @:return flag: flag 1 means fraud, and flag 0 means pass
    """
    a = np.array(seq['original'])
    for key in seq.keys():
        b = np.array(seq[key])
        criter = np.linalg.norm(a-b)/np.linalg.norm(a)
        if criter > Tol:
            flag = 0
            print 'Financial fraud',criter
            break
    else:
        flag = 1
        print 'pass the Ben_Ford'
    return flag


if __name__ == '__main__':
    # using the data from database and generating svg files with pygql.
    dbclient = pymongo.MongoClient('mongodb://127.0.0.1')
    stock_set = list(dbclient['Xueqiu']['stockbyindustry'].find({}, {'symbol': 1, '_id': 0}))
    fraud = []
    for stock in stock_set:
        stock_id = stock.values()[0]
        print stock_id
        industry = 'media'
        data = dict()
        for sheet in _sheet_type:
            table = industry + sheet
            statement = dbclient['Xueqiu'][table].find({'code': stock_id})
            # print sheet
            data.update({sheet: statement})
        try:
            ss = parsing_each_stock(data)
            seq = gen_percentage(ss)
            flag = criterion(seq, Tol=0.2)
            bar_chart = pygal.Bar(style=pygal.style.LightStyle)
            bar_chart.x_labels = _digit
            for key in seq.keys():
                bar_chart.add(key, seq[key])
                bar_chart.render_to_file("./plots/"+stock_id+"_Ben_Ford.svg")
        except Exception:
            print 'Failure to get the picture of' + ' ' + stock_id
        if flag == 0:
            fraud.append(stock_id)
        else:
            pass
    print fraud
    cookie = 's=8s125bbdhi; xq_a_token=353719375a63d9c5504083a962c65c231dfb715c; xqat=353719375a63d9c5504083a962c65c231dfb715c; xq_r_token=8063e0977bb80c6b9d90791a554d63e6285fef66; xq_is_login=1; u=3420308671; xq_token_expire=Sat%20Oct%2008%202016%2003%3A35%3A29%20GMT%2B0800%20(CST); bid=cfde5050318cc5826136d5dac4483954_it0g8w9o; __utma=1.1190508203.1473708993.1474065963.1474235795.6; __utmb=1.9.10.1474235795; __utmc=1; __utmz=1.1473708993.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); Hm_lvt_1db88642e346389874251b5a1eded6e3=1473708927; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1474236378'
    # save to file:
    fraud_vol = []
    for stock_id in fraud:
        stock_vol = {str(stock_id): get_history_std(stock_id, cookie=cookie)}
        fraud_vol.append(stock_vol)
    print fraud_vol
    with open('./fraud_vol.txt', 'w') as f:
        f.writelines(["%s\n" % item for item in fraud_vol])




