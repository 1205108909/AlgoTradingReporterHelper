import pymssql
import numpy as np
import pandas as pd
import h5py
import json
import logging
import sys, getopt
import time


class getClientOrders(object):
    def __init__(self, date):
        self.clientOrders = {}
        self.tickData = {}
        self.date = date
        self.ti5 = {}
        self.tr = {}
        self.transactionData = {}
        self.configFile = './config.json'
        with open(self.configFile, 'r') as f:
            config = json.load(f)
        self.SqlServer = config['sqlserver']
        self.transactionPath = config['transactionPath']
        self.tickPath = config['tickPath']
        logging.basicConfig(filename=config['logPath'],
                            format='%(asctime)s -%(name)s-%(levelname)s-%(module)s:%(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S %p',
                            level=logging.DEBUG)

    def readSql(self):
        logging.info("readSql started")
        conn = pymssql.connect(server=self.SqlServer['server'], user=self.SqlServer['user'],
                               password=self.SqlServer['password'], database=self.SqlServer['database'])
        # date = time.strftime("%Y%m%d")
        # sql = "select * from ClientOrderView where orderId = \'5d157eaa-ff13-458c-9edb-241d4627e77b\'"
        # 常用sql
        sql = "select * from ClientOrderView where orderQty>0 and (securityType=\'RPO\' or securityType=\'EQA\')   and tradingDay = \'" + self.date + '\'' #+ 'and (clientId like \'5038%\' or clientId like \'Cld_TRX%\')'
#修改泰铼14:57VWAP计算不准确的slipageInBps
        # sql = "select * from ClientOrderView where orderQty>0 and (securityType=\'RPO\' or securityType=\'EQA\')   and tradingDay = \'" + self.date + '\'' + 'and accountId = \'503800010101\''
 #        sql = "SELECT\
 # b.orderId,b.effectiveTime,b.expireTime,b.symbol,b.side,b.orderQty,b.price\
 # FROM\
	# ClientOrderAnalyze a\
 # INNER JOIN ClientOrder b ON a.orderId = b.orderId\
 # WHERE\
	# b.securityType = '0'\
 # AND a.arrivalPrice > '0'\
 # and b.cumQty > 0\
 # and ((b.side = 1 and a.arrivalFarPrice > a.arrivalFarPriceStrict) or (b.side = 2 and a.arrivalFarPrice < a.arrivalFarPriceStrict)) and tradingDay = \'" + self.date + '\''
        logging.info(sql)
        self.clientOrders = pd.read_sql(sql, conn)
        conn.close()
        logging.info("readSql finished!")

    def writeSql(self):
        logging.info("writeSql started")
        conn = pymssql.connect(server=self.SqlServer['server'], user=self.SqlServer['user'],
                               password=self.SqlServer['password'], database=self.SqlServer['database'])
        try:
            ## UPDATE
            with conn.cursor() as cursor:
            #     self.clientOrders.apply(lambda o: cursor.execute('update [ClientOrderAnalyze] set iTwap=%s, iTwapInLimitPrice=%s, dailyVwap=%s, dailyOpenPrice=%s, \
            #     dailyClosePrice=%s, arrivalPrice=%s, arrivalFarPrice=%s, arrivalFarPriceStrict=%s, stockReturn=%s, arrivalFarBestPrice=%s, where orderId=%s',
            #     (o['iTwap'], o['iTwapInLimitPrice'], o['dailyVwap'],
            #     o['dailyOpenPrice'], o['dailyClosePrice'],
            #     o['arrivalPrice'], o['arrivalFarPrice'],
            #                                                 o['arrivalFarPriceStrict'], o['stockReturn'], o['arrivalFarBestPrice'], o['endOfDay']
            #                                                o['orderId'])), axis=1)
            ## UPDATE farBestPrice
            # with conn.cursor() as cursor:
            #     self.clientOrders.apply(lambda o: cursor.execute('update [ClientOrderAnalyze] set arrivalFarBestPrice=%s, endOfDay=%s where orderId=%s',\
            #     (o['arrivalFarBestPrice'], o['endOfDay'], o['orderId'])), axis=1)

            # INSERT
                self.clientOrders.apply(lambda o: cursor.execute('INSERT INTO [ClientOrderAnalyze] VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', \
                    (o['orderId'], o['iTwap'], o['iTwapInLimitPrice'], o['dailyVwap'], o['dailyOpenPrice'], o['dailyClosePrice'], \
                     o['arrivalPrice'], o['arrivalFarPrice'], o['arrivalFarPriceStrict'], o['stockReturn'], o['arrivalFarBestPrice'], o['endOfDay'])), axis = 1)
                conn.commit()
        except Exception as e:
            logging.error(e)
        finally:
            conn.close()
            logging.info("writeSql finished!")

    def updateClientOrder(self):
        logging.info("updateClientOrder started")
        conn = pymssql.connect(server=self.SqlServer['server'], user=self.SqlServer['user'],
                               password=self.SqlServer['password'], database=self.SqlServer['database'])
        try:
            ## UPDATE
            with conn.cursor() as cursor:
                #     self.clientOrders.apply(lambda o: cursor.execute('update [ClientOrderAnalyze] set iTwap=%s, iTwapInLimitPrice=%s, dailyVwap=%s, dailyOpenPrice=%s, \
                #     dailyClosePrice=%s, arrivalPrice=%s, arrivalFarPrice=%s, arrivalFarPriceStrict=%s, stockReturn=%s, arrivalFarBestPrice=%s, where orderId=%s',
                #     (o['iTwap'], o['iTwapInLimitPrice'], o['dailyVwap'],
                #     o['dailyOpenPrice'], o['dailyClosePrice'],
                #     o['arrivalPrice'], o['arrivalFarPrice'],
                #                                                 o['arrivalFarPriceStrict'], o['stockReturn'], o['arrivalFarBestPrice'], o['endOfDay']
                #                                                o['orderId'])), axis=1)
                # UPDATE slipageInBps & iVWP
                with conn.cursor() as cursor:
                    self.clientOrders.apply(lambda o: cursor.execute('update [ClientOrder] set slipageInBps=%s, iVWP=%s where orderId=%s',\
                    (o['slipageInBps'], o['iVWP'], o['orderId'])), axis=1)

                # INSERT
                # self.clientOrders.apply(lambda o: cursor.execute(
                #     'INSERT INTO [ClientOrderAnalyze] VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', \
                #     (o['orderId'], o['iTwap'], o['iTwapInLimitPrice'], o['dailyVwap'], o['dailyOpenPrice'],
                #      o['dailyClosePrice'], \
                #      o['arrivalPrice'], o['arrivalFarPrice'], o['arrivalFarPriceStrict'], o['stockReturn'],
                #      o['arrivalFarBestPrice'], o['endOfDay'])), axis=1)
                conn.commit()
        except Exception as e:
            logging.error(e)
        finally:
            conn.close()
            logging.info("writeSql finished!")


    def readTransaction(self):
        tr5 = h5py.File(self.transactionPath + time.strftime("%Y%m%d") + ".h5")


    def readTransactionBySymbol(self, symbol):
        if symbol not in self.transactionData:
            if symbol in self.tr5.keys():
                data = pd.DataFrame({'Time': self.tr5[symbol]['Time'][:],
                                     'Price': self.tr5[symbol]['Price'][:],
                                     'AskOrder': self.tr5[symbol]['AskOrder'][:],
                                     'BidOrder': self.tr5[symbol]['BidOrder'][:],
                                     'Volume': self.tr5[symbol]['Volume'][:],
                                     'FunctionCode': self.tr5[symbol]['FunctionCode'][:],
                                     'OrderKind': self.tr5[symbol]['OrderKind'][:]})
            else:
                logging.warning("there is no TransactionData (" + symbol + ") in h5 file, please check your data")


    def readTick(self):
        logging.info('readH5TickData start @' + self.tickPath + self.date + '.h5')
        self.ti5 = h5py.File(self.tickPath + self.date + ".h5")
        logging.info(self.ti5)

    def readTickBySymbol(self, symbol):
        if symbol not in self.tickData:
            if symbol in self.ti5.keys():
                if symbol.split('.')[1] == 'IF':
                    pass
                else:
                    self.tickData[symbol] = pd.DataFrame({'Time': self.ti5[symbol]['Time'][:],
                                                          'Price': self.ti5[symbol]['Price'][:],
                                                          'AccTurnover': self.ti5[symbol]['AccTurnover'][:],
                                                          'AccVolume': self.ti5[symbol]['AccVolume'][:],
                                                          'Volume': self.ti5[symbol]['Volume'][:],
                                                          'BSFlag': self.ti5[symbol]['BSFlag'][:],
                                                          'BidAvgPrice': self.ti5[symbol]['BidAvgPrice'][:],
                                                          'High': self.ti5[symbol]['High'][:],
                                                          'Low': self.ti5[symbol]['Low'][:],
                                                          'MatchItem': self.ti5[symbol]['MatchItem'][:],
                                                          'Open': self.ti5[symbol]['Open'][:],
                                                          'PreClose': self.ti5[symbol]['PreClose'][:],
                                                          'TotalAskVolume': self.ti5[symbol]['TotalAskVolume'][:],
                                                          'TotalBidVolume': self.ti5[symbol]['TotalBidVolume'][:],
                                                          'Turnover': self.ti5[symbol]['Turnover'][:],
                                                          'AskAvgPrice': self.ti5[symbol]['AskAvgPrice'][:]})
                    if self.tickData[symbol].size > 0:
                        bp = self.ti5[symbol]['BidPrice10'][:].T
                        ap = self.ti5[symbol]['AskPrice10'][:].T
                        bv = self.ti5[symbol]['BidVolume10'][:].T
                        av = self.ti5[symbol]['AskVolume10'][:].T
                        for i in range(10):
                            self.tickData[symbol]['BidPrice' + str(i + 1)] = bp[i, :]
                            self.tickData[symbol]['AskPrice' + str(i + 1)] = ap[i, :]
                            self.tickData[symbol]['BidVolume' + str(i + 1)] = bv[i, :]
                            self.tickData[symbol]['AskVolume' + str(i + 1)] = av[i, :]
            else:
                logging.warning("there is no TickData (" + symbol + ") in h5 file, please check your data")


    def getTickDataBySymbol(self, symbol, startTime=90000000, endTime=160000000, price=0, side='Buy'):
        self.readTickBySymbol(symbol)
        if symbol in self.tickData.keys():
            if price == 0:
                return self.tickData[symbol][
                    (self.tickData[symbol].Time >= startTime) & (self.tickData[symbol].Time <= endTime) & (
                        self.tickData[symbol].Volume > 0)]
            else:
                if side == 'Buy' or side == 1:
                    return self.tickData[symbol][
                        (self.tickData[symbol].Time >= startTime) & (self.tickData[symbol].Time <= endTime) & (
                            self.tickData[symbol].Volume > 0) & (self.tickData[symbol].Price <= price)]
                else:
                    return self.tickData[symbol][
                        (self.tickData[symbol].Time >= startTime) & (self.tickData[symbol].Time <= endTime) & (
                            self.tickData[symbol].Volume > 0) & (self.tickData[symbol].Price >= price)]
        return pd.DataFrame()


    def getTWAP(self, symbol, startTime=90000000, endTime=160000000, price=0, side='Buy'):
        ##stock/future/repo
        data = self.getTickDataBySymbol(symbol, startTime, endTime, price, side)
        if data.size > 0:
            return data.Price.sum() / data.Volume.count()
        else:
            return 0


    def getVWAP(self, symbol, startTime=90000000, endTime=160000000, price=0, side='Buy'):
        ##订单生命周期vwap,eodvwap,全天vwap
        ##stock
        data = self.getTickDataBySymbol(symbol, startTime, endTime, price, side)
        if data.size > 0:
            vwaPrice = (data.Price * data.Volume).sum() / data.Volume.sum()
        else:
            vwaPrice = 0
        return vwaPrice


    def getArrivalTick(self, symbol, startTime):
        data = self.getTickDataBySymbol(symbol, startTime)
        if data.size > 0:
            return data[:1].Price.item()
        else:
            logging.warning(
                "there is no TickData(" + symbol + " ) after " + str(startTime) + " , please check your data")
            return 0


    def getArrivalFarPrice(self, symbol, startTime, orderVolume, side='Buy', price=0, strict=False, level = 10):
        ##计算直接市价成交在对手盘的价格，考虑对手10档价格
        tick = self.getTickDataBySymbol(symbol, startTime)[:1]
        if tick.size <= 0:
            return 0
        p = v = ''
        if side == 'Buy' or side == 1:
            p = 'AskPrice'
            v = 'AskVolume'
            nearPrice = tick['BidPrice1'].item()
        else:
            p = 'BidPrice'
            v = 'BidVolume'
            nearPrice = tick['AskPrice1'].item()
        # print(nearPrice)
        cv = 0
        tv = 0
        lastPrice = 0
        for i in range(level):
            ##涨跌停时对手盘为0
            if tick[p + str(i + 1)].item() > 0:
                lastPrice = tick[p + str(i + 1)].item()
                if (tick[v + str(i + 1)].item() + cv) <= orderVolume:
                    cv += tick[v + str(i + 1)].item()
                    tv += tick[v + str(i + 1)].item() * lastPrice
                else:
                    tv += (orderVolume - cv) * lastPrice
                    cv = orderVolume
                    break
                if price > 0:
                    if side == 'Buy' or side == 1:
                        if lastPrice >= price:
                            break
                    else:
                        if lastPrice <= price:
                            break
            else:
                break

        if cv == orderVolume:
            ##全部成交
            avg = tv / orderVolume
        elif cv > 0:
            if strict:
                ##按照第十档计算剩余全部数量
                # logging.debug('not match all shares, use unStrict method')
                # print('not match all shares, use unStrict method')
                avg = (tv + lastPrice * (orderVolume - cv)) / orderVolume
            else:
                # logging.debug('not match all shares, use strict method')
                # print('not match all shares, use strict method')
                avg = tv / cv
        else:
            logging.debug('return nearPrice: ' + symbol + str(nearPrice))
            avg = nearPrice
        return avg


    def getClosePrice(self, symbol):
        ##stock/future/repo
        if symbol.split('.')[1] == 'IF':
            return 0
        else:
            if symbol.startswith('204') or symbol.startswith('131'):
                cp = self.getVWAP(symbol, startTime=152900000)
            else:
                cp = self.getVWAP(symbol, startTime=145900000)
            # logging.info('closePrice: ' + str(date) + ' | ' +  symbol + str(cp))
            return cp

    def calcSlipageInBps(self, avgPrice, ivwap, side):
        if (avgPrice == 0) | (ivwap == 0):
            return 0
        if side == 'Buy':
            return (ivwap - avgPrice)/ivwap * 10000
        else:
            return (avgPrice - ivwap)/ivwap * 10000


    def getStockReturn(self, symbol, startTime=90000000, endTime=150000000):
        data = self.getTickDataBySymbol(symbol, startTime, endTime)
        if data.size > 0:
            # logging.info(symbol + ' | ' + str(startTime) + ' | ' + str(data.Price[-1:].item()) + ' -> ' + str(endTime) + ' | ' + str(data.Price[:1].item()))
            return data.Price[-1:].item() / data.Price[:1].item() - 1
        else:
            return 0
    def logClientOrder(self, o):
        # logging.info(o['orderId']+','+ str(o['iTwap'])+','+ str(o['iTwapInLimitPrice']) +','+ str(o['dailyVwap'])+','+ str(o['dailyOpenPrice'])+','+ str(o['dailyClosePrice'])\
        #                                                   +','+ str(o['arrivalPrice']) + ','+ str(o['arrivalFarPrice']) + ',' + str(o['arrivalFarPriceStrict']) + ',' + str(o['stockReturn']))
        log = ''
        for key in o.keys():
            log += key + ": " + str(o[key]) + ','
        logging.info(log)

    def run(self):
        logging.info('start updateClientOrder: ' + self.date)
        self.readSql()
        if self.clientOrders.size > 0:
            self.readTick()
            self.clientOrders['startTime'] = self.clientOrders['effectiveTime'].apply(
                lambda d: 1000 * int(d.strftime('%H%M%S')))
            self.clientOrders['endTime'] = self.clientOrders['expireTime'].apply(
                lambda d: 1000 * int(d.strftime('%H%M%S')))
            logging.info('start calc vwap')
            self.clientOrders['iVWP'] = self.clientOrders.apply(
                    lambda o: self.getVWAP(o['symbol'], o['startTime'], o['endTime']), axis=1)
            logging.info('start calc slipageInBps')
            self.clientOrders['slipageInBps'] = self.clientOrders.apply(
                lambda o: self.calcSlipageInBps(o['avgPrice'], o['iVWP'], o['side']), axis=1)
            # logging.info('start calc iTwap')
            # self.clientOrders['iTwap'] = self.clientOrders.apply(
            #     lambda o: self.getTWAP(o['symbol'], o['startTime'], o['endTime']), axis=1)
            # logging.info('start calc iTwapInLimitPrice')
            # self.clientOrders['iTwapInLimitPrice'] = self.clientOrders.apply(
            #     lambda o: self.getTWAP(o['symbol'], o['startTime'], o['endTime'], o['price'], o['side']), axis=1)
            # logging.info('start calcDailyVWAP')
            # self.clientOrders['dailyVwap'] = self.clientOrders.apply(lambda o: self.getVWAP(o['symbol']), axis=1)
            # logging.info('start calc dailyOpenPrice')
            # self.clientOrders['dailyOpenPrice'] = self.clientOrders.apply(
            #     lambda o: self.getArrivalTick(o['symbol'], 92459000), axis=1)
            # logging.info('start calc dailyClosePrice')
            # self.clientOrders['dailyClosePrice'] = self.clientOrders.apply(lambda o: self.getClosePrice(o['symbol']),
            #                                                                axis=1)
            # logging.info('start calc arrivalPrice')
            # self.clientOrders['arrivalPrice'] = self.clientOrders.apply(
            #     lambda o: self.getArrivalTick(o['symbol'], o['startTime']), axis=1)
            # logging.info('start calc arrivalFarPrice')
            # self.clientOrders['arrivalFarPrice'] = self.clientOrders.apply(
            #     lambda o: self.getArrivalFarPrice(o['symbol'], o['startTime'], o['orderQty'], o['side']), axis=1)
            # logging.info('start calc arrivalFarBestPrice')
            # self.clientOrders['arrivalFarBestPrice'] = self.clientOrders.apply(
            #     lambda o: self.getArrivalFarPrice(o['symbol'], o['startTime'], o['orderQty'], o['side'], level = 1), axis=1)
            # logging.info('start calc arrivalFarPriceStrict')
            # self.clientOrders['arrivalFarPriceStrict'] = self.clientOrders.apply(
            #     lambda o: self.getArrivalFarPrice(o['symbol'], o['startTime'], o['orderQty'], o['side'], strict=True),
            #     axis=1)
            # logging.info('start calc stockReturn')
            # self.clientOrders['stockReturn'] = self.clientOrders.apply(
            #     lambda o: self.getStockReturn(o['symbol'], o['startTime'], o['endTime']), axis=1)
            # self.clientOrders['endOfDay'] = self.clientOrders.apply(
            #     lambda o: self.getVWAP(o['symbol'], startTime = o['startTime'], price= o['price'], side = o['side']), axis=1)
            self.clientOrders.apply(lambda o: self.logClientOrder(o), axis = 1)
            # self.writeSql()
            self.updateClientOrder()
            self.clientOrders.to_csv('h:\\clientorder_20200720.csv', mode='a', header=False)
            self.ti5.close()
        logging.info('finished updateClientOrder: ' + self.date)


if __name__ == '__main__':
    date = time.strftime("%Y%m%d")
    if len(sys.argv) == 2:
        date = sys.argv[1]
    # app = getClientOrders('20190430')
    # app.run()

    with open('\\\\nas_yjs_algo\\algo_share\\Data\\static\\tradingday.csv', 'rb') as f:
        for date in f:
            #app = getClientOrders("20181112")
            #print(str(date, encoding = "utf-8").replace('\r\n',''))
            if date != b'date\r\n':
                #updated @ 20181228 to 20181220's data
                if int(date) >= 20200720 and int(date) <= 20200720 :#==20200713，需要update farBest和endOfDay,目前到20200108(20190429 20190430)
                                # if int(date)  > 20140101 and int(date) < 20180101:#QA
                    app = getClientOrders(str(date, encoding="utf-8").replace('\r\n', ''))
                    app.run()