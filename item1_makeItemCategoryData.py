# 설명 : 이번 분석에 사용할 아이템별 주종 구분 인덱스 데이터 추출
import pandas as pd
import pymysql
import item0_ethTypeClassificationFileName
import itemTag_config
import sys

# 사용데이터
# bfItemCategoryData : 전달 분석에 사용한 아이템
def makeItemCategoryData(bfItemCategoryData):
    # 주류 DB 접속하여 각 아이템마스터에서 분석에 사용하는 아이템 추출
    soolDB = pymysql.connect(user = '',
                             passwd = '',
                             host = '',
                             port = ,
                             db = '',
                             charset = 'utf8')

    DKwhiskyCursor = soolDB.cursor(pymysql.cursors.DictCursor)
    DKBeerCursor = soolDB.cursor(pymysql.cursors.DictCursor)
    HJBeerCursor = soolDB.cursor(pymysql.cursors.DictCursor)
    HJSojuCursor = soolDB.cursor(pymysql.cursors.DictCursor)
    HKBeerCursor = soolDB.cursor(pymysql.cursors.DictCursor)
    # [2022.01.24] 사이즈 값 추가
    DKWhiskyItemSql = "SELECT WSK_NM, VSL_SIZE FROM DK_WSK_ITEM WHERE USE_YN = 'Y'"
    DKBeerItemSql = "SELECT BR_NM, VSL_SIZE FROM DK_BR_ITEM WHERE USE_YN = 'Y'"
    HJBeerItemSql = "SELECT BR_NM, VSL_SIZE FROM HJ_BR_ITEM WHERE USE_YN = 'Y'"
    HJSojuItemSql = "SELECT SJ_NM, VSL_SIZE FROM HJ_SJ_ITEM WHERE USE_YN = 'Y'"
    HKBeerItemSql = "SELECT ITEM_NM, VSL_SIZE FROM HK_AGRN_ITEM WHERE USE_YN = 'Y'"

    DKwhiskyCursor.execute(DKWhiskyItemSql)
    DKBeerCursor.execute(DKBeerItemSql)
    HJBeerCursor.execute(HJBeerItemSql)
    HJSojuCursor.execute(HJSojuItemSql)
    HKBeerCursor.execute(HKBeerItemSql)

    DKWResult = DKwhiskyCursor.fetchall()
    DKBResult = DKBeerCursor.fetchall()
    HJBResult = HJBeerCursor.fetchall()
    HJSResult = HJSojuCursor.fetchall()
    HKBResult = HKBeerCursor.fetchall()

    DKWItem = pd.DataFrame(DKWResult)
    DKBItem = pd.DataFrame(DKBResult)
    HJBItem = pd.DataFrame(HJBResult)
    HJSItem = pd.DataFrame(HJSResult)
    HKBItem = pd.DataFrame(HKBResult)
    # [2022.01.24] 사이즈 컬럼 추가
    DKWItem.columns = ["ITEM", "SIZE"]
    DKBItem.columns = ["ITEM", "SIZE"]
    HJBItem.columns = ["ITEM", "SIZE"]
    HJSItem.columns = ["ITEM", "SIZE"]
    HKBItem.columns = ["ITEM", "SIZE"]

    # DB에 존재하지 않는 비모집단아이템 및 미분류아이템 추출
    nonTargetCase1 = bfItemCategoryData[bfItemCategoryData["CATEGORY_IDX"] == 0]
    nonTargetCase2 = bfItemCategoryData[bfItemCategoryData.ITEM.str.contains("비모집단")]
    nonTargetCase3 = bfItemCategoryData[bfItemCategoryData.ITEM.str.contains("미분류")]
    totNonTarget = pd.concat([nonTargetCase1, nonTargetCase2, nonTargetCase3], axis=0).drop_duplicates()
    # 각 주종 아이템 별로 CATEGORY, CATEGORY_IDX 추출
    totBeerItem = pd.concat([DKBItem, HJBItem, HKBItem], axis=0).drop_duplicates()
    totBeerItem["CATEGORY"] = "맥주"
    totBeerItem["CATEGORY_IDX"] = 1
    HJSItem["CATEGORY"] = "소주"
    HJSItem["CATEGORY_IDX"] = 1
    DKWItem["CATEGORY"] = "위스키"
    DKWItem["CATEGORY_IDX"] = 1
    # 이번달에 사용할 주종 구분 인덱스 데이터 추출
    itemCategoryData = pd.concat([totBeerItem, HJSItem, DKWItem, totNonTarget], axis=0).drop_duplicates().fillna(0)
    soolDB.close()

    return itemCategoryData

# 데이터 이름
itemCategoryFileName = item0_ethTypeClassificationFileName.itemCategoryDataName

# 필요한 변수 및 데이터 추출
date, bf1Mdate = itemTag_config.defineDate(sys.argv[1])
baseFilePath = itemTag_config.targetPath
bfItemCategoryData = pd.read_parquet(baseFilePath + bf1Mdate + "/" + itemCategoryFileName)

# itemCategoryData 저장
itemCategoryData = makeItemCategoryData(bfItemCategoryData)
itemCategoryData.to_parquet(baseFilePath + date + "/" + itemCategoryFileName, index = False)
