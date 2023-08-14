# 설명 : 이번 분석연월에 사용할 targetData 추출출

import pandas as pd
import item0_ethTypeClassificationFileName
import itemTag_config
import sys

# 필요데이터
# targetData: 이번달 분석에 사용되는 데이터
# typeIdxData : 주종의 정보 및 인덱스 값
# allBarIdxData: 이번달 신규 적용한 업체정보 인덱스
def makeTargetData(targetData, typeIdxData, allBarIdxData):
    # 검증한 결과에서 모델에 사용할 수 있도록 type 설정
    targetItemDataPre1 = targetData.copy()
    targetItemDataPre1["BARCODE"] = targetItemDataPre1["BARCODE"].astype(str)
    targetItemDataPre1["TYPE"] = targetItemDataPre1["TYPE"].astype(str)
    targetItemDataPre1["ITEM_UP"] = targetItemDataPre1["ITEM_UP"].astype(float)
    targetItemDataPre1["ITEM_SZ"] = targetItemDataPre1["ITEM_SZ"].astype(float)
    targetItemDataPre1["AMT"] = targetItemDataPre1["AMT"].astype(float)
    # 바코드에 적혀있는 업체정보 값 추출
    targetItemDataPre1["COMINFO"] = ""
    targetItemDataPre1.loc[targetItemDataPre1["BARCODE"] != "", "COMINFO"] = targetItemDataPre1.BARCODE.str.slice(4, 8)
    # 업체정보에 따른 인덱스 값, 주종에 따른 인덱스 값을 조인하여 분석모델에 사용할 targetData 추
    targetItemDataPre2 = pd.merge(targetItemDataPre1, allBarIdxData, on = ["COMINFO"])
    targetItemData = pd.merge(targetItemDataPre2, typeIdxData, on = ["TYPE"])

    return targetItemData

# 데이터 이름
targetDataName = item0_ethTypeClassificationFileName.targetDataName
typeIdxFileName = item0_ethTypeClassificationFileName.typeIdxDataName
newBarIdxDataName = item0_ethTypeClassificationFileName.newBarIdxDataName

# 필요한 변수 및 데이터 추출
date, bf1Mdate = itemTag_config.defineDate(sys.argv[1])
baseFilePath = itemTag_config.targetPath
commonFilePath = itemTag_config.commonPath

targetData = pd.read_parquet(baseFilePath + date + "/" + targetDataName)
typeIdxData = pd.read_parquet(commonFilePath + typeIdxFileName)
newBarIdxData = pd.read_parquet(baseFilePath + date + "/" + newBarIdxDataName)

# targetData 저장
targetItemData = makeTargetData(targetData, typeIdxData, newBarIdxData)
targetItemData.to_parquet(baseFilePath + date + "/" + "targetItemData.parquet", index = False)