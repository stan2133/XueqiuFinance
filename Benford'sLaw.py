# coding: utf-8
import pymongo
import pygal
import numpy as np


_sheet_type = ['incstatement', 'cfstatement', 'balsheet' ]
_digit = [str(i) for i in range(1, 10)]


def parsing_each_stock(data):
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


def ben_ford(useful_data,statement):
    stat = clean_data(useful_data)
    statistic = dict((x,stat.count(x)) for x in set(stat))
    statistic.pop('0')
    statistic.update({'statement':statement})
    return statistic


def gen_percentage(data):
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



