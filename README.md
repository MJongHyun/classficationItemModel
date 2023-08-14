# classficationItemModel

### 모델 기능

    + 분석에 사용하는 주종들만 추출하여 진행

### 코드설명

    + item0_ethTypeClassficationFileName.py : 모델 사용시 필요한 데이터 이름
    + item1_makeItemCategoryData.py : 이번 분석에 사용할 아이템별 주종 구분 인덱스 데이터 추출
    + item2_makeBarcodeItemData.py : 누적사전에서 검증한 아이템을 바탕으로 바코드에 따르는 아이템 값 추출
    + item3_makeTargetNonTarget.py : 태그된 결과를 바탕으로 NontargetData, targetData 추출
    + item4_makeTrainData.py : 분석모델에 적용할 TrainData 생성
    + item5_newBarIdxData.py : 신규 업체정보 업데이트 한 업체정보 인덱스 데이터 추출
    + item6_makeTargetData.py : 분석모델에 적용할 TargetData(이번 분석연월 아이템 데이터) 추출
    + item7_excuteClassficationData.py : 분류모델 실행하여 검증해야할 아이템 추출

        - 자세한 사항은 코드별로 작성해놓음

### 실행 방법

    1. Data Directory에 최신 누적사전(cumul_nminfo_df), 최신 업체정보 인덱스 데이터(barIdxData), 이번 분석에서 태깅된 데이터(taggedDF), 
    신규 아이템이 추가된 아이템, 주종, 인덱스 데이터(itemCategoryData) COMMON 폴더에 저장
    
    2. item1_makeItemCategoryData.py 실행
    
    2. item2_makeBarcodeIemData.py 실행
    
    3. item3_makeTargetNonTarget.py 실행

    4. item4_makeTrainData.py 실행

    5. item5_newBarIdxData.py 실행

    6. item6_makeTargetData.py 실행

    7. item7_excuteClassficationData.py 실행 후 Result Directory에 추출된 item_info_AFTER 추출하여 검증 진행

### 버전관리

    + ver 0.0.1 : Local에 저장되어 있음
    + ver 0.1 : Filtering 내용 변경 및 바코드아이템 관련 코드 추가
    + ver 0.1.1 : 신규아이템 추가시 자동으로 itemCategoryData 추가하는 코드 추가 (서버 적용코드)
    + ver 0.2 : SCORE 기반으로 검증 수를 줄이는 코드 추가 
    + ver 0.3 : 바코드 기반으로 검증 수를 줄이는 코드 추가
    + ver 0.4 : 자동검증값에서 정확하지 않은 값 수동검증으로 확인하는 코드 추가 및 검증한 데이터 중 의심되는 값 추출하는 코드 추가
     
