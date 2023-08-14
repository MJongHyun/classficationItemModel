# 설명 : 분석모델에 적용할 TrainData 추출

import pandas as pd
import item0_ethTypeClassificationFileName
import itemTag_config
import sys

# 필요데이터
# cumulData : 지금까지 분석에 적용된 누적사전
# itemCategoryData : 분석에 사용하는 아이템의 카테고리 정보
# barIdxData : 바코드에 적혀있는 업체정보 및 인덱스 값
# typeIdxData : 주종의 정보 및 인덱스 값
def makeTrainData(cumulData, itemCategoryData, barIdxData, typeIdxData):
    # 누적사전에서 검증한 결과만 추출
    verifyItemData = cumulData[cumulData["SAW"] != "안봄"].copy()
    # 검증한 결과에서 모델에 사용할 수 있도록 type 설정
    verifyItemData["BARCODE"] = verifyItemData["BARCODE"].astype(str)
    verifyItemData["TYPE"] = verifyItemData["TYPE"].astype(str)
    verifyItemData["ITEM_UP"] = verifyItemData["ITEM_UP"].astype(float)
    verifyItemData["ITEM_SZ"] = verifyItemData["ITEM_SZ"].astype(float)
    verifyItemData["AMT"] = verifyItemData["AMT"].astype(float)
    # 아이템별 모집단/비모집단 분류 값 조인
    trainDataPre1 = pd.merge(verifyItemData, itemCategoryData, on = ["ITEM"])
    # 바코드에 적혀있는 업체정보 값 추출
    trainDataPre1["COMINFO"] = ""
    trainDataPre1.loc[trainDataPre1["BARCODE"] != "", "COMINFO"] = trainDataPre1.BARCODE.str.slice(4, 8)
    # 업체정보에 따른 인덱스 값, 주종에 따른 인덱스 값을 조인하여 분석모델에 적용할 TrainData 추출
    trainDataPre2 = pd.merge(trainDataPre1, barIdxData, on = ["COMINFO"])
    trainData = pd.merge(trainDataPre2, typeIdxData, on = ["TYPE"])

    return trainData

# 데이터 이름
cumulFileName = item0_ethTypeClassificationFileName.cumulDataName
itemCategoryFileName = item0_ethTypeClassificationFileName.itemCategoryDataName
barIdxFileName = item0_ethTypeClassificationFileName.barIdxDataName
typeIdxFileName = item0_ethTypeClassificationFileName.typeIdxDataName

# 필요한 변수 및 데이터 추출
date, bf1Mdate = itemTag_config.defineDate(sys.argv[1])
baseFilePath = itemTag_config.targetPath
commonFilePath = itemTag_config.commonPath

cumulData = pd.read_parquet(baseFilePath + bf1Mdate + "/" + cumulFileName)
itemCategoryData = pd.read_parquet(baseFilePath + date + "/" + itemCategoryFileName)
barIdxData = pd.read_parquet(baseFilePath + bf1Mdate + "/" + barIdxFileName)
typeIdxData = pd.read_parquet(commonFilePath + typeIdxFileName)

# trainData 저장
trainData = makeTrainData(cumulData, itemCategoryData, barIdxData, typeIdxData)
trainData.to_parquet(baseFilePath + date + "/" + "trainData.parquet", index = False)