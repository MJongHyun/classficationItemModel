# 설명 : 필요한 데이터를 이름 정렬

# 바코드 수집을 통해 만든 바코드 검증 데이터
barcodeTableName = "barcodeTableData.parquet"
# 아이템 태깅 결과 데이터
taggedDFName = "taggedDF.parquet"
# 누적사전데이터
cumulDataName = "cumul_nminfo_df.parquet"
# 주종별 인덱스 데이터
ethCategoryDataName = "ethCategoryData.parquet"
# 아이템별 주종 구분 인덱스 데이터
itemCategoryDataName = "itemCategoryData.parquet"
# xgboost에 사용하는 parameter 정보 데이터
bestModelParameterDataName = "bestModelParameterData.parquet"
# 누적사전을 통한 바코드에 따른 아이템 데이터
barcodeItemDataName = "barcodeItemData.parquet"
# 이번달 검증에 사용하는 아이템
targetDataName = "targetData.parquet"
# 이번달 검증하지 않는 아이템
nonTargetDataName = "nonTargetNotData.parquet"
# 주세법에 적용된 주종구분 데이터
typeIdxDataName = "typeIdxData.parquet"
# 바코드에 작성된 업체정보 인덱스 데이터
barIdxDataName = "barIdxData.parquet"
# 신규 업체정보를 업데이트한 데이터
newBarIdxDataName = "barIdxData.parquet"
# 학습데이터 (누적사전에서 검증한 값)
trainDataName = "trainData.parquet"
# 1차로 검증해야할 데이터로 추출한 데이터
targetItemDataName = "targetItemData.parquet"
# 검증한 아이템 중 한번더 검증한 결과 데이터
verifyDataName = "verifyData.parquet"
# 태깅 모델 데이터
modelName = "model.json"
