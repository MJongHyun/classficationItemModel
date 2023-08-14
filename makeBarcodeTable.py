# 설명 : 수집한 바코드에 대해 검증 및 n_gram결과를 통해 예측한 결과 테이블 추출
# CHECK 구분 : Y - 수동으로 검증하고 사용해도 되는 데이터, N - 수동으로 검증하고 사용하면 안되는 데이터
# CHECK 구분 : Y1, Y2 - 수동으로 검증하지 않았고 사용할지 말지 판단해야하는 데이터, Q - 수동으로 검증하지 않았고 사용하면 안되는 데이터

import pandas as pd
import pymysql
import json
import re
import item0_ethTypeClassificationFileName
import itemTag_config
import sys
import operator

# 필요데이터 및 변수
# cumulData : 전달 누적사전 데이터
# barcodeData : 바코드를 수집한 데이터 - 신규 데이터만 추출할 수 있도록 바꾸던가 해야함
# YM : 바코드데이터 결과를 추가한 분석연월
def makeMostCumulData(cumulData, barcodeData, YM):
    # 바코드 수집데이터에서 사용하는 데이터만 추출 및 분석연월 추가
    barcodeDF = barcodeData[["BARCODE", "GS1_ITEM", "BP_SCN_ITEM"]].copy()
    barcodeDF["YM"] = YM
    # 누적사전을 통해 바코드대비 가장 많이나온 사이즈 값 추출
    mostCumulSizePre = cumulData.groupby(["BARCODE", "ITEM_SZ"]).count().\
        sort_values("ITEM", ascending = False).reset_index(drop = False)
    mostCumulSizeIdx = mostCumulSizePre[["BARCODE"]].drop_duplicates().index
    mostCumulSizeRes = mostCumulSizePre[mostCumulSizePre.index.isin(mostCumulSizeIdx)][["BARCODE", "ITEM_SZ"]].copy()
    mostCumulSizeRes.columns = ["BARCODE", "MOST_CUMUL_ITEM_SZ"]
    # 누적사전을 통해 바코드대비 가장 많이나온 아이템 값 추출
    mostCumulItemPre = cumulData.groupby(["BARCODE", "ITEM"]).count().\
        sort_values("ITEM_SZ", ascending = False).reset_index(drop = False)
    mostCumulItemIdx = mostCumulItemPre[["BARCODE"]].drop_duplicates().index
    mostCumulItemRes = mostCumulItemPre[mostCumulItemPre.index.isin(mostCumulItemIdx)][["BARCODE", "ITEM"]].copy()
    mostCumulItemRes.columns = ["BARCODE", "MOST_CUMUL_ITEM"]
    # 바코드 수집데이터에 위에서 추출한 사이즈 및 아이템을 붙여 데이터 추출
    barcodeMostCumulPre1 = pd.merge(barcodeDF, mostCumulSizeRes, on = ["BARCODE"], how = "left")
    barcodeMostCumul = pd.merge(barcodeMostCumulPre1, mostCumulItemRes, on = ["BARCODE"], how = "left")

    return barcodeMostCumul

# 필요데이터 및 변수
# barcodeMostCumul : makeMostCumulData 함수를 통해 결과가 나온 데이터
# targetCol : 바코드데이터 중 n_gram의 결과를 추출할 컬럼데이터
# resultCol : n_gram 결과의 컬럼
# model : n_gram을 통해 만든 모델 json 파일
def n_gram_ResultData(barcodeMostCumul, targetCol, resultCol, model):
    # barcodeMostCumul 데이터에서 Null값을 None으로 바꾸고, 누적사전 사이즈 값이 존재하지 않을 경우 500으로 값 추출
    barcodeMostCumulDF = barcodeMostCumul.where(pd.notnull(barcodeMostCumul), None).copy()
    barcodeMostCumulDF.loc[barcodeMostCumulDF.MOST_CUMUL_ITEM_SZ.isnull(), "MOST_CUMUL_ITEM_SZ"] = 500
    # 아이템이름에서 숫자만 추출할 정규표현식
    numberRegex = re.compile("[0-9]+\\.?[0-9]+")
    # 바코드에 대한 매칭 결과를 추가할 수 있는 리스트
    barcodeList = []
    resultList = []
    # 1. n_gram을 통해 token을 만든 후, model를 이용하여 아이템 매칭값에 가까운 값 추출
    print("Stage1: infer new Name's ITEM using (TOKEN, ITEM) pair's Score-Matrix Model.")
    for i in range(len(barcodeMostCumulDF)):
        barcode = barcodeMostCumulDF["BARCODE"][i]
        element = barcodeMostCumulDF[targetCol][i]
        # 수집한 데이터 값이 존재하지 않을 경우(None), "비주류"로 매칭
        if element is None:
            barcodeList.append(barcode)
            resultList.append("비주류")
            continue
        # 수집한 데이터 정제 및 바코드를 통해 추출한 누적사전 아이템 사이즈 추출
        element = str(barcodeMostCumulDF[targetCol][i])
        size = str(int(barcodeMostCumulDF["MOST_CUMUL_ITEM_SZ"][i]))
        rawItemPre1 = element.replace(" ", "")
        rawItemPre2 = rawItemPre1.upper()
        rawItemPre3 = rawItemPre2.replace("㎖", "ML")
        rawItemPre4 = rawItemPre3.replace("˚", "도")
        rawItemPre5 = rawItemPre4.replace("ℓ", "L")
        rawItemPre6 = re.sub('[가정|유흥|일반|할인|면세|가정용|유흥용|일반용|할인용|면세용]', "", rawItemPre5)
        rawItemPre7 = re.sub('^[-=+,#/\?:^;.@*\"※~ㆍ!』‘|\(\)\[\]`\'…》\”\“\’·_㈜&Ô%＇′]', "", rawItemPre6)
        rawItem = re.sub('[-=+,#/\?:^;@*\"※~ㆍ!』‘|\(\)\[\]`\'…》\”\“\’·_㈜&Ô＇′]', "", rawItemPre7)
        # 위의 정규표현식을 통해 추출한 rawItem의 길이가 1이하 또는 숫자로 변경이 가능할 경우, "비주류"로 매칭
        if (len(rawItem) <= 1) or rawItem.isdigit():
            barcodeList.append(barcode)
            resultList.append("비주류")
            continue
        # 수집한 아이템에서 숫자가 존재할 경우 "*"로 변경
        replaceItem = numberRegex.sub("*", rawItem)
        numberList = numberRegex.findall(rawItem)
        # token을 추가할 List 및 token값 추출
        resElement = []
        elementList = []
        if len(replaceItem) <= 3:
            resElement.append(rawItem)
        else:
            for character in replaceItem:
                if character == "*":
                    key = numberList.pop(0)
                    elementList.append(key)
                else:
                    elementList.append(character)
            # n_gram을 통해 3가지 케이스로 token적용
            firstElementList = list(elementList)
            secondElementList = list(elementList)
            thirdElementList = list(elementList)
            # 처음 2글자를 뺌
            firstElementList.pop()
            firstElementList.pop()
            # 처음과 마지막 글자 뺌
            secondElementList.pop()
            secondElementList.pop(0)
            # 마지막 2글자를 뺌
            thirdElementList.pop(0)
            thirdElementList.pop(0)
            # 3가지 token + size를 통해 token 설정
            resElement = list(map(lambda x: x[0] + x[1] + x[2] + size,
                                  list(zip(firstElementList, secondElementList, thirdElementList))))
        # 토큰으로 만든 값이 model에 존재할 경우, model.key에 Score를 붙힘, 존재하지 않을 경우 Score를 0으로 함
        resDict = {}
        for key in resElement:
            if key in model:
                for item in model[key]:
                    if item not in resDict:
                        resDict[item] = 0
                        resDict[item] = model[key][item]
                    resDict[item] += model[key][item]
        # score가 가장 높은 값에 대한 아이템을 매칭
        try:
            result, score = sorted(resDict.items(), key = operator.itemgetter(1), reverse=True)[0]
        except:
            # 존재하지 않을 경우, 비주류로 매칭
            result, score = "비주류", 0
        barcodeList.append(barcode)
        resultList.append(result)
    # barcodeList, resultList에 추가한 값으로 데이터프레임 추출
    print("Stage2: save the inferenced reuslt as 'parquet file'.")
    taggedDFRes = pd.DataFrame({
        'BARCODE': barcodeList,
        resultCol: resultList,
    })

    return taggedDFRes

# 필요데이터 및 변수
# barcodeMostCumul : makeMostCumulData 함수를 통해 결과가 나온 데이터
# model : n_gram을 통해 만든 모델 json 파일
def makeBarcodeData(barcodeMostCumul, model):
    # barcodeMostCumul에서 바코드에 따른 수집한 아이템에 대한 결과 값 추출
    taggedDF1 = n_gram_ResultData(barcodeMostCumul, "GS1_ITEM", "GS1_ITEM_N_GRAM_RES", model)
    taggedDF2 = n_gram_ResultData(barcodeMostCumul, "BP_SCN_ITEM", "GS1_ITEM_N_GRAM_RES", model)
    # barcodeData 결과에 적용할 컬럼
    resultCol = ['YM', 'BARCODE', 'GS1_ITEM', 'GS1_ITEM_N_GRAM_RES', 'BP_SCN_ITEM',
                 'BP_SCN_ITEM_N_GRAM_RES', 'MOST_CUMUL_ITEM', 'MOST_CUMUL_ITEM_SZ',
                 'ITEM', 'CHECK']
    # barcodeMostCumul에 n_gram_ResultData을 통해 매칭한 결과 붙임
    barcodeDataPre1 = pd.merge(barcodeMostCumul, taggedDF1, on = ["BARCODE"])
    barcodeDataPre2 = pd.merge(barcodeDataPre1, taggedDF2, on = ["BARCODE"])
    # 필터 조건에 따라 ITEM, CHECK 값을 붙이기 위한 컬럼 추가
    barcodeDataPre2["ITEM"] = ""
    barcodeDataPre2["CHECK"] = "Q"
    # 필터링을 적용할 데이터와 아닌데이터 추출
    # 기준 : 수집 아이템 n_gram 매칭 결과가 하나라도 "비주류" 가 아닌 경우
    check = ((barcodeDataPre2["BP_SCN_ITEM_N_GRAM_RES"] != "비주류") | (barcodeDataPre2["GS1_ITEM_N_GRAM_RES"] != "비주류"))
    barcodeCheckTarget = barcodeDataPre2[check].copy()
    barcodeCheckOther = barcodeDataPre2[check == False].copy()
    # 필터조건1 : 2개의 n_gram결과가 비주류가 아닌 바코드 누적 아이템 값과 같을 경우 ITEM값으로 적용, CHECK는 Y1으로 적용
    # (2021.09 기준 약 90% 정확도)
    y1Check = (barcodeCheckTarget["GS1_ITEM_N_GRAM_RES"] == barcodeCheckTarget["BP_SCN_ITEM_N_GRAM_RES"]) & (
                barcodeCheckTarget["GS1_ITEM_N_GRAM_RES"] == barcodeCheckTarget["MOST_CUMUL_ITEM"])
    barcodeCheckTarget.loc[y1Check, "ITEM"] = barcodeCheckTarget[y1Check]["MOST_CUMUL_ITEM"]
    barcodeCheckTarget.loc[y1Check, "CHECK"] = "Y1"
    y1CheckIdx = barcodeCheckTarget[barcodeCheckTarget["CHECK"] == "Y1"].index
    # 필터조건2 : 필터조건1에 해당하지 않으면서, 각 n_gram결과가 비주류가 아니고 누적사전 아이템결과와 같은 경우, ITEM값으로 적용, CHECK는 Y2로 적용
    # (2021.09 기준 약 96% 정확도)
    notmainTagCheck1 = (barcodeCheckTarget["GS1_ITEM_N_GRAM_RES"] != "비주류") & (
                barcodeCheckTarget.index.isin(y1CheckIdx) == False) & (
                                    barcodeCheckTarget["GS1_ITEM_N_GRAM_RES"] == barcodeCheckTarget["MOST_CUMUL_ITEM"])
    notmainTagCheck2 = (barcodeCheckTarget["BP_SCN_ITEM_N_GRAM_RES"] != "비주류") & (
                barcodeCheckTarget.index.isin(y1CheckIdx) == False) & (
                                    barcodeCheckTarget["BP_SCN_ITEM_N_GRAM_RES"] == barcodeCheckTarget["MOST_CUMUL_ITEM"])
    barcodeCheckTarget.loc[notmainTagCheck1, "ITEM"] = barcodeCheckTarget[notmainTagCheck1]["GS1_ITEM_N_GRAM_RES"]
    barcodeCheckTarget.loc[notmainTagCheck2, "ITEM"] = barcodeCheckTarget[notmainTagCheck2]["BP_SCN_ITEM_N_GRAM_RES"]
    barcodeCheckTarget.loc[notmainTagCheck1, "CHECK"] = "Y2"
    barcodeCheckTarget.loc[notmainTagCheck2, "CHECK"] = "Y2"
    # 필터 2개를 적용한 결과를 최종 데이터로 추출
    barcodeTableDF = pd.concat([barcodeCheckTarget, barcodeCheckOther], axis = 0)[resultCol]

    return barcodeTableDF

# 데이터 경로
cumulDataName = item0_ethTypeClassificationFileName.cumulDataName
modelName = item0_ethTypeClassificationFileName.modelName

# 필요한 변수 및 데이터 추출
date, bf1Mdate = itemTag_config.defineDate(sys.argv[1])
baseFilePath = itemTag_config.targetPath

cumul_nminfo_df = pd.read_parquet(baseFilePath + bf1Mdate + "/" + cumulDataName)
model = json.load(open(baseFilePath + bf1Mdate + "/" + modelName))
# 바코드 관련 결과 : DB로 값을 불러오거나 또는 함수를 통해 구분하여 값을 불러와야함

# 바코드 수집한 결과와 n_gram을 통해 바코드 테이블 추출
# barcode 데이터를 DB와 연동에서 불러야 함
barcodeMostCumul = makeMostCumulData(cumul_nminfo_df, barcodeData, date)
barcodeTableDF = makeBarcodeData(barcodeMostCumul, model)

# barcodeTableDF을 추가할 DB 함수 필요





