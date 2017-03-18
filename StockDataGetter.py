# data format:
# {
#   data: [
#           [ date, open price, close price, highest price, lowest price, volume, percent ],
#             ...
#         ],
#   symbol: "stockCode",
#   name: "stockName"
# }
# data fetched from NetEase finance

import http.client
import json
import pickle
from datetime import datetime
import zlib

CONST_DR = False                        # divide right or not
CONST_KLINETYPE = "day"                 # k-line type: day, month, week
CONST_OUTPUTTYPE = "zip"                # output type: text, binary, zip
CONST_SINGLEFILE = True                 # output all data in a single file
CONST_FILENAME = "StockHistoryData"     # output file name, if writing as single file

conn = http.client.HTTPConnection('img1.money.126.net')

# get current year
curYear = datetime.now().year

# 0,1,2 represent SH,SZ,CYB respectively
def formatStockCode(part, number):
    tail = str(number)
    if part == 0:
        code = "60"
    elif part == 1:
        code = "00"
    else:
        code = "30"
    for i in range(0, 4-len(tail)):
        code += "0"
    return code + tail

def formatPartCode(part):
    if part == 0:
        return "0"
    else:
        return "1"

def decompressData(data):
    return zlib.decompress(data, 16+zlib.MAX_WBITS)

def getResponseString(data):
    return str(decompressData(data), "utf-8")

def outputData(fileName, data, writeType):
    if writeType == "text":
        file = open(fileName,"w")
        json.dump(data, file)
        file.close()
    elif writeType == "binary":
        file = open(fileName,"bw")
        pickle.dump(data, file)
        file.close()
    elif writeType == "zip":
        file = open(fileName,"bw")
        file.write(zlib.compress(json.dumps(data).encode("utf-8")))
        file.close()

# init http header
header = {
    "Accept":"*/*",
    "Accept-Encoding":"gzip, deflate, sdch",
    "Accept-Language":"zh-CN,zh;q=0.8",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
    "Connection": "keep-alive"
}
dr = ""
if not CONST_DR:
    dr = "derc"
urlTemp = "/data/hs/kline" + dr + "/" + CONST_KLINETYPE + "/history/{0}/{1}{2}.json"
dataList = []

for part in range(0, 3):
    for code in range(0, 3000):
        stockCode = formatStockCode(0, code)
        url = urlTemp.format(curYear, formatPartCode(part), stockCode)
        conn.request("GET", url, None, header)
        r1 = conn.getresponse()
        if r1.status == 200:
            data = json.loads(getResponseString(r1.read()), "utf-8")
            year = curYear
            while True:
                year -= 1
                url = urlTemp.format(year, formatPartCode(part), stockCode)
                conn.request("GET", url, None, header)
                rr = conn.getresponse()
                if rr.status == 200:
                    newData = json.loads(getResponseString(rr.read()), "utf-8")
                    data["data"].extend(newData["data"])
                else:
                    rr.read()
                    break
            if not CONST_SINGLEFILE:
                outputData(stockCode, data, CONST_OUTPUTTYPE)
            else:
                dataList.append(data)
        else:
            r1.read()
        print(part, stockCode)

if CONST_SINGLEFILE:
    outputData(CONST_FILENAME, dataList, CONST_OUTPUTTYPE)

conn.close()