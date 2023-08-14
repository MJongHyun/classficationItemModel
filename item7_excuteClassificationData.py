# 설명 : 머신러닝을 사용하여 분석에 사용하는 아이템을 추출한 후,  BARCODE 및 SCORE를 통해 매칭결과가 신뢰성이 높은 아이템들을 구분한 후, 검증데이터 저장
# 설명 추가 : 검증한 데이터 중, 값이 의심되는 값은 이번달에 재 검증
# ITEM_CATEGORY : 0 - 비분석주류아이템 (검증비적용), 1 - 분석주류아이템 (검증적용), 2 - 분석주류아이템 중 핉터링에 해당하는 아이템 (검증적용), 3 - 검증제외아이템 (검증비적용),
# 4 - SCORE를 통해 태깅결과를 신뢰하는 아이템 (검증비적용), 5 - 바코드 테이블을 통해 신뢰하는 아이템 (검증비적용), 6 - 검증한 아이템 중, 오류로 판단되는 아이템 (검증적용)
# ITEM_CATOEGORY에서 1,2,6 을 필터링해서 수동검증 진행함 / 나머지 (0, 3, 4, 5)는 봄으로 바꿔서 검증

from xgboost import XGBClassifier
import pandas as pd
import numpy as np
import item0_ethTypeClassificationFileName
import itemTag_config
import sys
import requests
import datetime

# 필요데이터
# trainData : 누적 사전을 통해 생성한 모델 훈련 데이터
# targetItemData : 이번달 검증에 사용하는 결과 데이터
# bestModelParameterData : xgboost에 사용하는 parameter 정보 데이터
# itemCategoryData : 아이템에 따른 주종 및 인덱스를 가져오는 데이터
# barcodeItemData : 누적사전에서 검증된 아이템 중 바코드에 따른 아이템을 추출한 데이터
def excuteClassification(trainData, targetItemData, bestModelParameterData, itemCategoryData, barcodeItemData):
    # feature에 사용하는 컬럼과 label 결과에 사용하는 컬럼 구분
    featureColumn = ["COMINFO_IDX", "TYPE_IDX", "ITEM_SZ", "ITEM_UP"]
    labelColumn = ["CATEGORY_IDX"]
    # 훈련데이터에 feaure, label적용
    trainFeature = trainData[featureColumn]
    trainLabel = trainData[labelColumn]
    # 결과데이터에 feature 적용
    targetFeature = targetItemData[featureColumn]
    # 모델 값에 최적의 파라미터 값 적용 후 실행
    xgbModel = XGBClassifier(learning_rate = bestModelParameterData["learning_rate"].iloc[0],
                             max_depth = int(bestModelParameterData["max_depth"].iloc[0]),
                             n_estimators = int(bestModelParameterData["n_estimators"].iloc[0]))
    xgbModel.fit(trainFeature, trainLabel.values.ravel())
    # 모델에 따른 예측 값과 각 분류의 percent 값 추출
    predictLabel = xgbModel.predict(targetFeature)
    predictScore = xgbModel.predict_proba(np.array(targetFeature))
    predictLabelDF = pd.DataFrame(predictLabel, columns = ["CATEGORY_IDX"])
    predictScoreDF = pd.DataFrame(predictScore, columns = ["NON_TARGET_PER", "TARGET_PER"])
    predictResultPre1 = pd.concat([predictScoreDF, predictLabelDF], axis = 1)
    # 위에서 추출한 값과 검증에 사용하는 데이터에 조인
    predictIdxLabel = pd.concat([targetFeature.reset_index(drop = False), predictResultPre1], axis = 1)[
        ["index", "NON_TARGET_PER", "TARGET_PER", "CATEGORY_IDX"]]
    predictResultPre2 = pd.merge(targetItemData.reset_index(drop = False), predictIdxLabel, on = ["index"])
    predictResultPre3 = pd.merge(predictResultPre2, itemCategoryData[["CATEGORY_IDX"]].drop_duplicates(),
                                 on  = ["CATEGORY_IDX"]).drop(columns = ["index"])
    # 비모집단과 모집단 구분하여 비모집단에서 추가적으로 검증할 값 추가
    predictResultPre3Target = predictResultPre3[predictResultPre3.CATEGORY_IDX == 1]
    predictResultPre3Nontarget = predictResultPre3[predictResultPre3.CATEGORY_IDX == 0]

    # 검증 추가 기준
    # 제외 조건으로 반드시 비주류 분석에 해당되는 값 제거
    # 1. 매칭결과가 "비모집단와인" 이면서 주종이 "06", 아이템사이즈가 "750"인 경우 제거
    wineCase = predictResultPre3Nontarget[(predictResultPre3Nontarget["TYPE"] == "06") & (
            predictResultPre3Nontarget["ITEM_SZ"] == 750) & (
            predictResultPre3Nontarget["ITEM"] == "비모집단와인")]

    wineCaseIdx = wineCase.index

    # [2022.04.04 수정] - 비주류인 값은 수동검증 확인
    # 2. 매칭결과가 누적사전 바코드 아이템과 비교하여 비주류 분석에 해당되는 값 제거
    trueCase1 = predictResultPre3Nontarget[predictResultPre3Nontarget.index.isin(wineCaseIdx) == False]
    barcodeCasePre = pd.merge(trueCase1, barcodeItemData, on = ["BARCODE"], how = "left")

    barcodeCase1 = barcodeCasePre[(barcodeCasePre.BARCODE_IDX == 1) | (barcodeCasePre.ITEM == "비주류")].copy()
    barcodeCase1["CATEGORY_IDX"] = 2
    barcodeCase2 = barcodeCasePre[(barcodeCasePre.BARCODE_IDX == 0) & (barcodeCasePre.ITEM.str.contains("비모집단"))].copy()
    barcodeCase = pd.concat([barcodeCase1, barcodeCase2], axis = 0).drop(columns = ["BARCODE_IDX"])
    barcodeItemKey = barcodeCase[["NAME", "ITEM_SZ"]].copy()
    barcodeItemKey["YN"] = "Y"

    trueCase2 = pd.merge(trueCase1, barcodeItemKey, on = ["NAME", "ITEM_SZ"], how = "left")
    filteringTarget = trueCase2[trueCase2["YN"].isnull()].drop(columns = ["YN"])

    # 필터링 조건을 통해 분석주류로 의심되는 값 추출
    # 1. classResNonCase1 : 비모집단으로 구분된 값들 중 주종이 ["", "05", "07", "08", "09"] 이면서 모집단일 확률이 평균+표준편차 이상인 경우
    typeList = ["", "05", "07", "08", "09"]
    meanRes = filteringTarget.TARGET_PER.describe()["mean"]
    stdRes = filteringTarget.TARGET_PER.describe()["std"]
    valueRes = meanRes + stdRes
    classResNonCase1 = filteringTarget[
        (filteringTarget.TYPE.isin(typeList)) | (filteringTarget.TARGET_PER >= valueRes)]
    case1Idx = classResNonCase1.index

    # 2. classResNonCase2 : classResNonCase1을 제외하고 바코드가 없는 값 중, 비모집단에서 NAME, ITEM_SZ 기준 매입액이 퍙균이상인 아이템
    classResNonCase2 = filteringTarget[
        (filteringTarget.index.isin(case1Idx) == False) & (
                filteringTarget.COMINFO == "")].sort_values("AMT", ascending = False)

    # case2에 해당하는 금액 및 key값 추출
    case2Value = int(classResNonCase2.AMT.mean())
    case2Idx = classResNonCase2[classResNonCase2.AMT >= case2Value].index

    # 3. classResNonCase3 : classResNonCase1, classResNonCase2를 제외하고 비모집단에서 NAME, ITEM_SZ 기준 매입액이 평균이상인 아이템
    classResNonCase3 = filteringTarget[
        (filteringTarget.index.isin(case1Idx) == False) & (filteringTarget.index.isin(case2Idx) == False)]

    # case3에 해당하는 금액 및 key값 추출
    case3Value = int(classResNonCase3.AMT.mean())
    case3Idx = classResNonCase3[classResNonCase3.AMT >= case3Value].index

    # 4. classResNonCase4 : classResNonCase1, classResNonCase2, classResNonCase3을 제외하고 태깅결과가 주류분석 아이템, 비주류인 경우
    classResNonCase4 = filteringTarget[
        (filteringTarget.index.isin(case1Idx) == False) & (
                filteringTarget.index.isin(case2Idx) == False) & (
            filteringTarget.index.isin(case3Idx) == False)]

    # case4에 해당하는 아이템 및 key값 추출
    case4ValueCase1 =  itemCategoryData[itemCategoryData["CATEGORY_IDX"] == 1][["ITEM"]]
    case4ValueCase2 = itemCategoryData[itemCategoryData["ITEM"] == "비주류"][["ITEM"]]
    case4Value = pd.concat([case4ValueCase1, case4ValueCase2], axis=0)["ITEM"].tolist()
    case4Idx = classResNonCase4[classResNonCase4.ITEM.isin(case4Value)].index

    # case1, case2, case3, case4에 해당하는 값을 분석주류 인덱스 값으로 변환
    totalIdx = case4Idx.append(case3Idx).append(case2Idx).append(case1Idx).unique()
    filteringTarget.loc[filteringTarget.index.isin(totalIdx), "CATEGORY_IDX"] = 2

    # 변환한 값을 다시 합친 후 결과에 필요한 컬럼만 적용하여 추출
    classResNonTargetResult = pd.concat([wineCase, barcodeCase, filteringTarget], axis = 0)
    resCol = ["WR_DT", "BARCODE", "TYPE", "NAME", "ITEM_SZ", "ITEM", "SAW", "MEMO", "AMT", "ITEM_UP", "CHECK",
              "CATEGORY_IDX"]

    predictResult = pd.concat([predictResultPre3Target, classResNonTargetResult], axis = 0).reset_index(drop = True)

    return predictResult[resCol]

# 필요데이터
# [2022.04.04] 누적바코드에서 추출한 바코드 데이터를 비검증데이터에 포함
# 필요데이터
# predictResult : 1차 excuteClassification를 통해 검증/비검증을 추출한 데이터
# barcodeTable : 바코드 수집을 통해 만든 바코드 검증 데이터
# barcodeItemData : 누적사전에서 만든 바코드 데이터
def excuteBarcodeTrustClassification(predictResult, barcodeTableData, barcodeItemData):
    # 바코드 검증데이터에서 검증한 데이터만 추출
    barcodeTableTrue = barcodeTableData[barcodeTableData.CHECK == "Y"][["BARCODE", "ITEM"]].copy()
    barcodeTableTrue.columns = ["BARCODE", "BAR_ITEM"]
    # predictResult에서 검증해야할 아이템이면서 바코드가 존재하는 데이터 추출
    verifyDataPre = predictResult[predictResult.CATEGORY_IDX.isin([1, 2])]
    targetBarcodeCheckDF = verifyDataPre[verifyDataPre.BARCODE != ""].copy()
    # 바코드 값이 바코드 테이블에 존재하면서 매칭된 아이템과 바코드 아이템이 같은 값 추출
    barCodeTrueDFPre = pd.merge(barcodeTableTrue, targetBarcodeCheckDF, on=["BARCODE"])[
        ["NAME", "ITEM_SZ", "ITEM", "BAR_ITEM"]].drop_duplicates()
    barCodeTrueKey = barCodeTrueDFPre[barCodeTrueDFPre["ITEM"] == barCodeTrueDFPre["BAR_ITEM"]][
        ["NAME", "ITEM_SZ"]].copy()
    barCodeTrueKey["BARCODE_TRUE"] = "Y"
    # 같은 값에 대해 CATEGORY_IDX를 변경하여 predictResultFinalPre값 추출
    barCodeTrueDF = pd.merge(predictResult, barCodeTrueKey, on=["NAME", "ITEM_SZ"], how="left")
    barCodeTrueDFRes = barCodeTrueDF[barCodeTrueDF.BARCODE_TRUE == "Y"].copy()
    barCodeTrueDFRes["CATEGORY_IDX"] = 5
    resultCol = predictResult.columns
    predictResultFinalPre1 = barCodeTrueDFRes[resultCol]
    # 누적바코드에서 나온 결과 적용
    barCodeFalseDF = barCodeTrueDF[barCodeTrueDF.BARCODE_TRUE != "Y"].copy()
    cumulBarCodeCheck = pd.merge(barCodeFalseDF, barcodeItemData, on=["BARCODE"], how="left")

    cumulmask1 = cumulBarCodeCheck["ITEM"] != cumulBarCodeCheck["BARCODE_ITEM"]
    cumulmask2 = cumulBarCodeCheck["ITEM_SZ"] != cumulBarCodeCheck["SIZE"]
    cumulmask3 = (cumulBarCodeCheck.CATEGORY == "맥주") & (cumulBarCodeCheck.NAME.str.contains("병|BTL")) & (
                cumulBarCodeCheck.ITEM.str.contains("병") == False)
    cumulmask4 = (cumulBarCodeCheck.CATEGORY == "맥주") & (cumulBarCodeCheck.NAME.str.contains("캔|CAN")) & (
                cumulBarCodeCheck.ITEM.str.contains("캔") == False)
    cumulmask5 = (cumulBarCodeCheck.CATEGORY == "맥주") & (cumulBarCodeCheck.NAME.str.contains("케그")) & (
                cumulBarCodeCheck.ITEM.str.contains("케그") == False)

    cumulBarCodeFalseIdx = cumulBarCodeCheck[
        cumulmask1 | cumulmask2 | cumulmask3 | cumulmask4 | cumulmask5].index.unique()
    cumulBarCodeCheck.loc[cumulBarCodeCheck.index.isin(cumulBarCodeFalseIdx) == False, "CATEGORY_IDX"] = 5
    predictResultFinalPre2 = cumulBarCodeCheck[resultCol]
    predictResultFinalPre = pd.concat([predictResultFinalPre1, predictResultFinalPre2], axis=0)

    return predictResultFinalPre

# 필요데이터
# predictResultFinalPre : 바코드테이블을 통해 1차 검증을 한 데이터
# taggedDFData : 아이템 태깅 결과 데이터
def excuteScoreTrustClassification(predictResultFinalPre, taggedDFData):
    # predictResult에서 태깅결과가 "비주류"가 아니면서 분석주종으로 나온 데이터 추출
    trustItemResPre = predictResultFinalPre[(predictResultFinalPre.CATEGORY_IDX.isin([1, 2])) &
                                            (predictResultFinalPre.ITEM != "비주류")].copy()
    # 아이템 태깅결과와 Join하여 Score값 추출
    taggedDFData["ITEM_SZ"] = taggedDFData.ITEM_SZ.astype(float)
    taggedDF = taggedDFData[["NAME", "ITEM_SZ", "SCORE"]].copy()
    trustItemScoreDF = pd.merge(trustItemResPre, taggedDF, on = ["NAME", "ITEM_SZ"])
    # 추출한 데이터에서 매칭아이템 결과의 용량 추출
    trustItemScoreDF["SIZE_LIST"] = trustItemScoreDF.ITEM.str.findall('(\d+)')
    trustItemScoreDF["SIZE"] = trustItemScoreDF.SIZE_LIST.str.get(-1).fillna(0).astype(float)
    # 1. 추출용량과 세금계산서 용량이 같은 경우, 2. 용량이 다르면서 태깅결과가 "비모집단맥주"가 아닌 경우
    sameSizeDF = trustItemScoreDF[trustItemScoreDF["SIZE"] == trustItemScoreDF["ITEM_SZ"]].copy()
    otherSizeDF = trustItemScoreDF[(trustItemScoreDF["SIZE"] != trustItemScoreDF["ITEM_SZ"]) & (
                trustItemScoreDF["ITEM"].str.contains("비모집단맥주") == False)].copy()
    # 각 추출한 결과에서 평균 Score값 추출 / [2021.11.19] OtherSizeMeanScore에 표준편차 추가
    sameSizeMeanScore = round(sameSizeDF.SCORE.mean(), 6)
    otherSizeMeanScore = round(otherSizeDF.SCORE.mean(), 6) + int(otherSizeDF.SCORE.std())
    # 추출한 SCORE보다 높은 값들 추출
    sameSizeDFResDF = sameSizeDF[sameSizeDF.SCORE >= sameSizeMeanScore]
    otherSizeResDF = otherSizeDF[otherSizeDF.SCORE >= otherSizeMeanScore]
    trustScoreDFPre = pd.concat([sameSizeDFResDF, otherSizeResDF], axis = 0)

    # 필터링 조건을 통해 태깅결과가 의심스러운 값 추출
    # 1. amtValue : 매입금액이 평균 매입금액보다 높은 경우
    amtValue = predictResultFinalPre.AMT.mean()
    nonCase1DF = trustScoreDFPre[trustScoreDFPre.AMT >= amtValue]
    case1Idx = nonCase1DF.index
    # case1Value에 해당하지 않는 값들 제거
    case1DFRes = trustScoreDFPre[trustScoreDFPre.index.isin(case1Idx) == False].copy()
    trustItemKey = case1DFRes[["NAME", "ITEM_SZ", "SCORE"]].copy()

    # 필터링조건에 해당하지 않는 데이터는 CATEGORY_IDX값을 4로 바꾸고 결과추출
    resultCol = predictResultFinalPre.columns
    trustDF = pd.merge(predictResultFinalPre, trustItemKey, on = ["NAME", "ITEM_SZ"])[resultCol]
    trustDF["CATEGORY_IDX"] = 4
    notTrustDFPre = pd.merge(predictResultFinalPre, trustItemKey, on = ["NAME", "ITEM_SZ"], how = "left")
    notTrustDF = notTrustDFPre[notTrustDFPre["SCORE"].isnull()][resultCol]

    predictResFinalData = pd.concat([trustDF, notTrustDF], axis = 0)

    return predictResFinalData

# [2022.03.29] 이미 검증한 데이터 중, 용량/용기가 이슈가 있을만한 데이터는 이번달 검증
# 필요데이터
# nonTargetItemData : 지난번까지 누적사전에 검증한 값으로 적용되었던 데이터
# bfVerifyData: 한번 더 검증한 데이터
# itemCategoryData : 분석에 적용되는 매칭 결과 데이터
# nowDT : 이번분석연월
def makeIssueTagItem(bfVerifyData, nonTargetItemData, itemCategoryData, nowDT):
    dataResCol = nonTargetItemData.columns
    matchMissPre = pd.merge(nonTargetItemData, itemCategoryData[["ITEM", "SIZE", "CATEGORY"]], on = ["ITEM"])
    # 비모집단/비주류 매칭 값 제거 및 아이템 사이즈가 0인값은 제거 - 이름으로 매칭
    nonTargetIdx1 = matchMissPre[(matchMissPre["SIZE"] != 0) & (matchMissPre["ITEM_SZ"] != 0)].index.tolist()
    matchMissPre2 = matchMissPre[matchMissPre.index.isin(nonTargetIdx1)]
    # 병/캔/케그로 적혀 있으나 매칭 값이 다른 경우 추출
    targetMatch1 = (matchMissPre2.NAME.str.contains("병|BTL")) & (matchMissPre2.ITEM.str.contains("병") == False)
    targetMatch2 = (matchMissPre2.NAME.str.contains("캔|CAN")) & (matchMissPre2.ITEM.str.contains("캔") == False)
    targetMatch3 = (matchMissPre2.NAME.str.contains("케그|생|KEG")) & (matchMissPre2.ITEM.str.contains("케그") == False)
    # 사이즈가 다른경우 추출
    targetMatch4 = (matchMissPre2.ITEM_SZ != matchMissPre2.SIZE)
    matchMissRes1 = matchMissPre2[targetMatch1 | targetMatch2 | targetMatch3].drop_duplicates().copy()
    idxAllpre = matchMissRes1.index
    # 병/캔/케그는 필수로 보고 사이즈가 다른값만 추출
    matchMissPre3 = matchMissPre2[(matchMissPre2.index.isin(idxAllpre) == False) & targetMatch4].copy()
    # 세금계산서 사이즈에 따른 매칭값 사이즈의 비율 값이 0.95 밑인값은 검증확인
    matchMissPre3["TF_SIZE"] = matchMissPre3["ITEM_SZ"] > matchMissPre3["SIZE"]
    matchMissPre3["SIZE_PER"] = 0
    matchMissPre3.loc[matchMissPre3.TF_SIZE == True, "SIZE_PER"] = matchMissPre3["SIZE"] / matchMissPre3["ITEM_SZ"]
    matchMissPre3.loc[matchMissPre3.TF_SIZE == False, "SIZE_PER"] = matchMissPre3["ITEM_SZ"] / matchMissPre3["SIZE"]
    matchMissPre4 = matchMissPre3[matchMissPre3.SIZE_PER < 0.95].copy()
    # 세금계산서 사이즈에 따른 매칭값 사이즈의 비율값에서 로그값을 취했을 때, 정수값이 아닌 경우 검증확인
    matchMissPre4["LOG_PER"] = np.log10(matchMissPre4["SIZE_PER"])
    matchMissPre4["LOG_FALSE"] = matchMissPre4.LOG_PER.apply(lambda x: x == int(x))
    matchMissPre5 = matchMissPre4[matchMissPre4["LOG_FALSE"] == False].copy()
    # 매칭값이 위스키인 값중에서 50ml 차이가 나지않으면 검증에서 제외
    matchMissPre5["SIZE_DEL"] = abs(matchMissPre5["ITEM_SZ"] - matchMissPre5["SIZE"])
    targetMatch5 = (matchMissPre5["CATEGORY"] != "위스키") | (matchMissPre5["SIZE_DEL"] > 50)
    matchMissRes2 = matchMissPre5[targetMatch5]
    # 전달까지 검증한 데이터가 존재할 경우, 검증값에서 제외
    mathMissRes3 = pd.concat([matchMissRes1, matchMissRes2], axis = 0)[dataResCol]
    bfVerifyDF = bfVerifyData[["NAME", "ITEM_SZ"]].copy()
    bfVerifyDF["YN"] = "Y"
    mathMissRes4 = pd.merge(mathMissRes3, bfVerifyDF, on = ["NAME", "ITEM_SZ"], how = "left")
    # 위 모든필터링을 제거하고 남은 데이터는 검증데이터로 적용
    matchMissResult = mathMissRes4[mathMissRes4.YN.isnull()][dataResCol]
    matchMissResult["CATEGORY_IDX"] = 6
    matchMissResult["DESC"] = "검증필요데이터_검증"
    # 나머지 검증한 결과는 비검증데이터로 추출
    sawDFResult = pd.merge(nonTargetItemData, matchMissResult[["NAME", "ITEM_SZ", "CATEGORY_IDX", "DESC"]],
                           on = ["NAME", "ITEM_SZ"], how = "left")
    sawDFResult.loc[sawDFResult.CATEGORY_IDX == 6, "SAW"] = "안봄"
    sawDFResult.loc[sawDFResult.CATEGORY_IDX.isnull(), "CATEGORY_IDX"] = 3
    sawDFResult.loc[sawDFResult.CATEGORY_IDX.isnull(), "DESC"] = "검증제외데이터_비검증"
    # 지금까지 한번더 검증한 데이터와 이번달에 나온 신규데이터를 총합하여 이번 분석연월에 저장
    nowVerifyDF = matchMissResult[["WR_DT", "NAME", "ITEM_SZ", "ITEM"]].copy()
    nowVerifyDF["VERIFY_DT"] = str(nowDT)
    allVerifyDF = pd.concat([bfVerifyData, nowVerifyDF], axis = 0)

    return allVerifyDF, sawDFResult

# 필요데이터
# predictResFinalData : 최종 검증데이터
# sawDFResult : 검증한 아이템 필터링 적용한 데이터
# [2022.03.28] sawDFResult 검증안된 데이터 중 비주류 또는 모집단아이템으로 n_gram결과가 나왔을 경우 수동검증에 추가
# [2022.04.22] n_gram맥주 값에 대해 이상있는 값은 수동검증에 추가
def makeItemInfoAfterData(predictResFinalData, sawDFResult, itemCategoryData):
    # 이번달 전체 데이터 추출할 수 있도록 성분 변경
    predictResFinalData["DESC"] = ""
    sawDFResult["ITEM_UP"] = sawDFResult["ITEM_UP"].astype(float)
    sawMask = (sawDFResult.SAW == "안봄") & \
              (~sawDFResult.ITEM.str.contains("비모집단")) & \
              (sawDFResult.CATEGORY_IDX != 6)

    sawDFResult.loc[sawMask, "CATEGORY_IDX"] = 2

    # 최종 검증데이터 추출 전, 맥주로 n_gram결과가 나온 값들 중 용기값을 잘못 적은 경우 CATEGORY_IDX를 2번으로 변경
    itemInfoAfterDataPre = pd.concat([sawDFResult, predictResFinalData], axis = 0)
    itemInfoCol = itemInfoAfterDataPre.columns

    itemInfoAfterDataPre2 = pd.merge(itemInfoAfterDataPre, itemCategoryData[["ITEM", "CATEGORY"]])
    targetMask = (itemInfoAfterDataPre2["CATEGORY"] == "맥주") & (itemInfoAfterDataPre2["SAW"] == "안봄")
    itemInfoAfterDataPreBeer = itemInfoAfterDataPre2[targetMask].copy()
    itemInfoAfterDataPreElse = itemInfoAfterDataPre2[~targetMask].copy()

    beerMaskCan = (itemInfoAfterDataPreBeer.NAME.str.contains("캔|CAN")) & \
                  (~itemInfoAfterDataPreBeer.ITEM.str.contains("캔"))
    beerMaskKeg = (itemInfoAfterDataPreBeer.NAME.str.contains("케그")) & \
                  (~itemInfoAfterDataPreBeer.ITEM.str.contains("케그"))
    beerMaskBot = (itemInfoAfterDataPreBeer.NAME.str.contains("병")) & \
                  (~itemInfoAfterDataPreBeer.ITEM.str.contains("병"))
    itemInfoAfterDataPreBeer.loc[beerMaskCan | beerMaskKeg | beerMaskBot, "CATEGORY_IDX"] = 2
    itemInfoAfterData = pd.concat([itemInfoAfterDataPreBeer, itemInfoAfterDataPreElse], axis=0)[itemInfoCol]

    itemInfoAfterData.loc[itemInfoAfterData.CATEGORY_IDX == 0, "DESC"] = "비분석주류아이템_비검증"
    itemInfoAfterData.loc[itemInfoAfterData.CATEGORY_IDX == 1, "DESC"] = "분석아이템_검증"
    itemInfoAfterData.loc[itemInfoAfterData.CATEGORY_IDX == 2, "DESC"] = "필터기준아이템_검증"
    itemInfoAfterData.loc[itemInfoAfterData.CATEGORY_IDX == 3, "DESC"] = "검증제외아이템_비검증"
    itemInfoAfterData.loc[itemInfoAfterData.CATEGORY_IDX == 4, "DESC"] = "SCORE기준아이템_비검증"
    itemInfoAfterData.loc[itemInfoAfterData.CATEGORY_IDX == 5, "DESC"] = "바코드테이블기준아이템_비검증"

    itemInfoAfterData.loc[itemInfoAfterData.CATEGORY_IDX == 4, "SAW"] = "N_GRAM을 통한 검증"
    itemInfoAfterData.loc[itemInfoAfterData.CATEGORY_IDX == 5, "SAW"] = "바코드를 통한 검증"

    return itemInfoAfterData

# 데이터 경로
trainDataName = item0_ethTypeClassificationFileName.trainDataName
targetItemDataName = item0_ethTypeClassificationFileName.targetItemDataName
bestModelParameterDataName = item0_ethTypeClassificationFileName.bestModelParameterDataName
itemCategoryFileName = item0_ethTypeClassificationFileName.itemCategoryDataName
nonTargetItemDataName = item0_ethTypeClassificationFileName.nonTargetDataName
barcodeItemDataName = item0_ethTypeClassificationFileName.barcodeItemDataName
taggedDFName = item0_ethTypeClassificationFileName.taggedDFName
barcodeTableName = item0_ethTypeClassificationFileName.barcodeTableName
verifyDataName = item0_ethTypeClassificationFileName.verifyDataName

# 필요한 변수 및 데이터 추출
date, bf1Mdate = itemTag_config.defineDate(sys.argv[1])
baseFilePath = itemTag_config.targetPath
commonFilePath = itemTag_config.commonPath

trainData = pd.read_parquet(baseFilePath + date + "/" + trainDataName)
targetItemData = pd.read_parquet(baseFilePath + date + "/" + targetItemDataName)
bestModelParameterData = pd.read_parquet(commonFilePath + bestModelParameterDataName)
itemCategoryData = pd.read_parquet(baseFilePath + date + "/" + itemCategoryFileName)
nonTargetItemData = pd.read_parquet(baseFilePath + date + "/" + nonTargetItemDataName)
barcodeItemData = pd.read_parquet(baseFilePath + date + "/" + barcodeItemDataName)
taggedDFData = pd.read_parquet(baseFilePath + date + "/" + taggedDFName)
barcodeTableData = pd.read_parquet(commonFilePath + "/" + barcodeTableName)
bfVerifyData = pd.read_parquet(baseFilePath + bf1Mdate + "/" + verifyDataName)

# 예측값 추출 후 검증해야할 데이터 저장
predictResult = excuteClassification(trainData, targetItemData, bestModelParameterData, itemCategoryData, barcodeItemData)
predictResultFinalPre = excuteBarcodeTrustClassification(predictResult, barcodeTableData, barcodeItemData)
predictResFinalData = excuteScoreTrustClassification(predictResultFinalPre, taggedDFData)
verifyData, sawDFResult = makeIssueTagItem(bfVerifyData, nonTargetItemData, itemCategoryData, date)
verifyData.to_parquet(baseFilePath + date + "/" + "verifyData.parquet", index = False)

itemInfoAfterData = makeItemInfoAfterData(predictResFinalData, sawDFResult, itemCategoryData)
itemInfoAfterData.to_parquet(baseFilePath + date + "/" + "item_info_AFTER.parquet", index = False)
itemInfoAfterData.to_excel(baseFilePath + date + "/" + "item_info_AFTER.xlsx", index = False)


def send_msg_fun(flag, task, file):
    startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    level = "INFO"
    url = "https://hooks.slack.com/services/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"
    payload = {"blocks": [
        {"type": "section",
         "text": {
             "type": "mrkdwn",
             "text": "[{} : {}] \n{}  `{}`  {}".format(flag, task, startTime, level, file)}
         }
    ]}
    return requests.post(url, json=payload)



def send_slack(flag, task, file):
    rst = send_msg_fun(flag, task, file)
    if rst.status_code != 200:
        rst = send_msg_fun(flag, task, file)
        if rst.status_code != 200:
            print("슬랙 전송 오류")

send_slack("dti", "아이템 매칭 카테고리 생성 완료", "run_verify_ItemModel")
