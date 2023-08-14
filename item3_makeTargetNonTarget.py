# 설명 : 태그된 결과를 바탕으로 NontargetData, targetData 추출

import pandas as pd
import item0_ethTypeClassificationFileName
import itemTag_config
import sys

# 필요데이터
# cumul_nminfo_df : 지금까지 분석에 적용된 누적사전
# taggedDF : 아이템 태깅 결과 데이터
# itemCategoryData : 아이템에 따른 주종 및 인덱스를 가져오는 데이터
def makeTargetNonTargetData(cumul_nminfo_df, taggedDF, itemCategoryData):

    # 필터링 조건을 위해 float값으로 변환
    taggedDF["ITEM_SZ"] = taggedDF["ITEM_SZ"].astype(float)
    taggedDF["AMT"] = taggedDF["AMT"].astype(float)
    taggedDF["ITEM_UP"] = taggedDF["ITEM_UP"].astype(float)

    cumul_nminfo_df["ITEM_SZ"] = cumul_nminfo_df["ITEM_SZ"].astype(float)
    cumul_nminfo_df["AMT"] = cumul_nminfo_df["AMT"].astype(float)
    cumul_nminfo_df["ITEM_UP"] = cumul_nminfo_df["ITEM_UP"].astype(float)
    # tagged 된 값 중 본값과 안본값을 나눈 후, 본 값은 이전 누적사전과 같은 값으로 추출
    verifySaw = taggedDF[taggedDF["AMT"].isnull()].copy()
    verifyNotSaw = taggedDF[taggedDF["AMT"].isnull() == False].copy()
    verifySawDF = pd.merge(verifySaw[["NAME", "ITEM_SZ"]], cumul_nminfo_df, on = ["NAME", "ITEM_SZ"])
    # CHECK 값 추출 전 임시데이터 추출
    iteminfoCol = ["WR_DT", "BARCODE", "TYPE", "NAME", "ITEM_SZ", "ITEM", "SAW", "AMT", "ITEM_UP"]
    itemInfoBefore = pd.concat([verifySawDF[iteminfoCol], verifyNotSaw[iteminfoCol]], axis = 0)
    itemInfoBefore["CHECK"] = ""
    itemInfoBefore["MEMO"] = ""
    # 검증한 데이터, 검증하지 않은 데이터 추출
    sawData = itemInfoBefore[itemInfoBefore["SAW"] != "안봄"].copy()
    nowSawData = itemInfoBefore[itemInfoBefore["SAW"] == "안봄"].copy()

    # targetItem에 해당하는 값을 추출하기 위한 데이터 추출
    # AMT 기준
    nowSawData.loc[abs(nowSawData["AMT"]) >= 500000, "CHECK"] = "Y"
    # ITEM_SZ 기준
    nowSawData.loc[nowSawData["ITEM_SZ"] <= 200, "CHECK"] = "Y"
    nowSawData.loc[(nowSawData["ITEM_SZ"] >= 330) & (nowSawData["ITEM_SZ"] <= 375), "CHECK"] = "Y"
    nowSawData.loc[(nowSawData["ITEM_SZ"] >= 473) & (nowSawData["ITEM_SZ"] <= 560), "CHECK"] = "Y"
    nowSawData.loc[nowSawData["ITEM_SZ"] == 640, "CHECK"] = "Y"
    nowSawData.loc[nowSawData["ITEM_SZ"] == 700, "CHECK"] = "Y"
    nowSawData.loc[nowSawData["ITEM_SZ"] >= 1000, "CHECK"] = "Y"
    # 주종이 위스키 주종인 경우
    nowSawData.loc[nowSawData["TYPE"].str.contains("09"), "CHECK"] = "Y"
    # 이름에 브랜드 포함된 경우
    nowSawData.loc[nowSawData["NAME"].str.contains("진로|하이네켄"), "CHECK"] = "Y"
    # 태깅결과가 위스키인 경우
    whikeyItem = list(itemCategoryData[itemCategoryData["CATEGORY"] == "위스키"]["ITEM"])
    nowSawData.loc[nowSawData["ITEM"].isin(whikeyItem), "CHECK"] = "Y"
    # 주종 사이즈 아닌 값
    nowSawData.loc[((nowSawData["TYPE"] != "06") | (nowSawData["ITEM_SZ"] != 750)) & (
                (nowSawData["TYPE"] != "04") | (nowSawData["ITEM_SZ"] != 720)) & (
                               (nowSawData["TYPE"] != "04") | (nowSawData["ITEM_SZ"] != 900)), "CHECK"] = "Y"
    # 최종컬럼값 적용 후, classification에 해당하는 값 추출
    finalCol = ["WR_DT", "BARCODE", "TYPE", "NAME", "ITEM_SZ", "ITEM", "SAW", "MEMO", "CHECK", "AMT", "ITEM_UP"]
    nonTargetNotSaw =  nowSawData[nowSawData["CHECK"] != "Y"][finalCol]
    nonTargetNotData = pd.concat([nonTargetNotSaw, sawData[finalCol]], axis = 0)
    targetData = nowSawData[nowSawData["CHECK"] == "Y"][finalCol]

    return nonTargetNotData, targetData

# 데이터 이름
cumulFileName = item0_ethTypeClassificationFileName.cumulDataName
itemCategoryFileName = item0_ethTypeClassificationFileName.itemCategoryDataName
taggedDFName = item0_ethTypeClassificationFileName.taggedDFName

# 필요한 변수 및 데이터 추출
date, bf1Mdate = itemTag_config.defineDate(sys.argv[1])
baseFilePath = itemTag_config.targetPath

cumul_nminfo_df = pd.read_parquet(baseFilePath + bf1Mdate + "/" + cumulFileName)
itemCategoryData = pd.read_parquet(baseFilePath + date + "/" + itemCategoryFileName)
taggedDFData = pd.read_parquet(baseFilePath + date + "/" + taggedDFName)

# nonTargetItem, targetItem 저장
nonTargetNotData, targetData = makeTargetNonTargetData(cumul_nminfo_df, taggedDFData, itemCategoryData)
nonTargetNotData.to_parquet(baseFilePath + date + "/"  + "nonTargetNotData.parquet", index = False)
targetData.to_parquet(baseFilePath + date + "/"  + "targetData.parquet", index = False)
