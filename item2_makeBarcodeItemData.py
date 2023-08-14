# 설명 : 누적사전에서 검증한 아이템을 바탕으로 바코드에 따르는 아이템 값 추출

import pandas as pd
import item0_ethTypeClassificationFileName
import itemTag_config
import sys
# [2022.04.04]
# 바코드 데이터에 누적사전에서 가장 많이 나온 아이템 값 추가
# 필요데이터
# cumul_nminfo_df : 지금까지 분석에 적용된 누적사전
# itemCategoryData : 아이템에 따른 주종 및 인덱스를 가져오는 데이터
def makeBarcodeItemData(cumul_nminfo_df, itemCategoryData):
    # 누적사전에서 검증된 아이템만 추출하여 바코드에 따른 아이템 결과 값 추출
    cumulSaw = cumul_nminfo_df[(cumul_nminfo_df["SAW"] != "안봄") & (cumul_nminfo_df["BARCODE"] != "")]
    barcodeItemResPre = cumulSaw.groupby(["BARCODE", "ITEM"]).count(). \
        reset_index(drop=False)[["BARCODE", "ITEM", "WR_DT"]]
    barcodeItemResPre.columns = ["BARCODE", "ITEM", "ITEM_CNT"]
    # 현재 검증한 누적사전에서 가장 많이 나온 아이템 값 추출
    barcodeItemKey = barcodeItemResPre.sort_values("ITEM_CNT", ascending=False)[["BARCODE"]]. \
        drop_duplicates().index
    barcodeItem = barcodeItemResPre[barcodeItemResPre.index.isin(barcodeItemKey)][["BARCODE", "ITEM"]].copy()
    # 바코드에 따른 분석주종/비분석주종에 대한 값 추출
    barcodeItemResPre2 = pd.merge(barcodeItemResPre, itemCategoryData[["ITEM", "CATEGORY", "CATEGORY_IDX"]])
    barcodeItemResPre3 = barcodeItemResPre2.groupby(["BARCODE", "CATEGORY_IDX"]). \
        sum("ITEM_CNT").reset_index(drop=False)
    barcodeItemResPre3.columns = ["BARCODE", "CATEGORY_IDX", "CATEGORY_CNT"]
    # 바코드에 따른 전체결과 합을 구한 후, 비율 값 추출
    barcodeItemResPre3Sum = barcodeItemResPre3.groupby("BARCODE").sum("CATEGORY_CNT"). \
        reset_index(drop=False)[["BARCODE", "CATEGORY_CNT"]]
    barcodeItemResPre3Sum.columns = ["BARCODE", "CNT_SUM"]
    barcodeItemRes = pd.merge(barcodeItemResPre3, barcodeItemResPre3Sum)
    barcodeItemRes["PERCENT"] = (barcodeItemRes["CATEGORY_CNT"] / barcodeItemRes["CNT_SUM"]) * 100
    # 바코드로 나온 아이템의 수가 평균이상이면서, 해당 주종의 결과가 90% 이상인 바코드와 CATOGORY_IDX 추출
    barcodeItemRes["CNT_TARGET"] = 0
    barcodeItemRes["PER_TARGET"] = 0
    barcodeItemRes.loc[barcodeItemRes.CATEGORY_CNT >= int(barcodeItemRes.CATEGORY_CNT.mean()), "CNT_TARGET"] = 1
    barcodeItemRes.loc[barcodeItemRes.PERCENT >= 90, "PER_TARGET"] = 1
    # 위의 조건에 해당되는 결과만 추출
    targetBarcodeItem = barcodeItemRes[(barcodeItemRes["CNT_TARGET"] == 1) & (
            barcodeItemRes["PER_TARGET"] == 1)][["BARCODE", "CATEGORY_IDX"]]
    targetBarcodeItem.columns = ["BARCODE", "BARCODE_IDX"]
    targetBarcodeItemResultPre = pd.merge(barcodeItem, targetBarcodeItem)
    targetBarcodeItemResult = pd.merge(targetBarcodeItemResultPre, itemCategoryData[["ITEM", "SIZE", "CATEGORY"]],
                                       on=["ITEM"])
    targetBarcodeItemResult.columns = ["BARCODE", "BARCODE_ITEM", "BARCODE_IDX", "SIZE", "CATEGORY"]

    return targetBarcodeItemResult

# 데이터 이름
cumulFileName = item0_ethTypeClassificationFileName.cumulDataName
itemCategoryFileName = item0_ethTypeClassificationFileName.itemCategoryDataName

# 필요한 변수 및 데이터 추출
date, bf1Mdate = itemTag_config.defineDate(sys.argv[1])
baseFilePath = itemTag_config.targetPath

cumul_nminfo_df = pd.read_parquet(baseFilePath + bf1Mdate + "/" + cumulFileName)
itemCategoryData = pd.read_parquet(baseFilePath + date + "/" + itemCategoryFileName)

# barcodeItemData 저장
barcodeItemData = makeBarcodeItemData(cumul_nminfo_df, itemCategoryData)
barcodeItemData.to_parquet(baseFilePath + date + "/" + "barcodeItemData.parquet", index = False)
