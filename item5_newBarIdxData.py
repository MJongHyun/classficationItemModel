# 설명 : 신규로 추가된 업체정보 인덱스 추가 및 저장

import pandas as pd
import item0_ethTypeClassificationFileName
import itemTag_config
import sys

# 필요데이터
# targetData: 이번달 분석에 사용되는 데이터
# barIdxData: 전달 사용한 업체정보 인덱스
def newBarIdxData(targetData, barIdxData):
    # 바코드에 적혀있는 업체정보 값 추출
    targetCopy = targetData.copy()
    targetCopy["BARCODE"] = targetCopy["BARCODE"].astype(str)
    targetCopy["COMINFO"] = ""
    targetCopy.loc[targetCopy["BARCODE"] != "", "COMINFO"] = targetCopy.BARCODE.str.slice(4, 8)
    # 신규로 나타난 업체정보 값 추출
    targetComInfo = targetCopy[["COMINFO"]].drop_duplicates()
    targetBarIdx = pd.merge(targetComInfo, barIdxData, on = ["COMINFO"], how = "left").sort_values("COMINFO_IDX")
    # 신규 업체정보에 인덱스 값 적용
    newComInfo = targetBarIdx[targetBarIdx.COMINFO_IDX.isnull()].reset_index(drop = True)
    newStIdx = barIdxData.COMINFO_IDX.max() + 1
    newLastIdx = barIdxData.COMINFO_IDX.max() + len(newComInfo) + 1
    # 기존 업체정보와 합쳐서 이번달에 사용할 업체정보 값 추출
    newIdxDF = pd.DataFrame(range(newStIdx, newLastIdx), columns = ["COMINFO_IDX"])
    newComInfoIdx = pd.concat([newComInfo[["COMINFO"]], newIdxDF], axis = 1)
    allBarIdxData = pd.concat([barIdxData, newComInfoIdx], axis = 0).reset_index(drop = True)

    return allBarIdxData

# 데이터 이름
barIdxFileName = item0_ethTypeClassificationFileName.barIdxDataName
targetDataName = item0_ethTypeClassificationFileName.targetDataName

# 필요한 변수 및 데이터 추출
date, bf1Mdate = itemTag_config.defineDate(sys.argv[1])
baseFilePath = itemTag_config.targetPath

barIdxData = pd.read_parquet(baseFilePath + bf1Mdate + "/" + barIdxFileName)
targetData = pd.read_parquet(baseFilePath + date + "/" + targetDataName)

# 신규 업체정보 결과 저장
allBarIdxData = newBarIdxData(targetData, barIdxData)
allBarIdxData.to_parquet(baseFilePath + date + "/" + "barIdxData.parquet", index = False)