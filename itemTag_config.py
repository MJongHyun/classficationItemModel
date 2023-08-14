# 설명 : 분석연월 설정 및 저장경로 설정

import sys
import datetime
from dateutil.relativedelta import relativedelta

# 이번 분석연월 설정

# input = sys.argv[1]

def defineDate(dt):
    date = str(dt)
    # 전달 분석연월 설정
    nowTime = datetime.datetime.strptime(date, '%Y%m')
    delta = relativedelta(months = 1)
    diff = nowTime - delta
    bf1Mdate = diff.strftime("%Y%m")
    # 공통파일경로, 월별파일경로 설정

    return date, bf1Mdate

commonPath = ''
targetPath = ''

