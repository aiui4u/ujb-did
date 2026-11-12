# 의정부시 통합 DID JSON 수집기

Node.js 24에서 저장소 최상위 위치를 기준으로 실행합니다.

```bash
node collector/collect.mjs --self-test
node collector/collect.mjs
```

추가 패키지 설치가 필요하지 않습니다.

필수 GitHub Actions Secrets:
- KMA_API_KEY
- AIRKOREA_API_KEY
- POPULATION_API_KEY
- BIRTH_API_KEY
- COMMERCE_API_KEY
- TRAFFIC_API_KEY

세대원수 API는 POPULATION_API_KEY를 함께 사용합니다. 해당 서비스의 키가 다를 때만 선택적으로 HOUSEHOLD_SIZE_API_KEY를 등록합니다. 같은 키를 쓰더라도 이용하는 각 API의 활용승인이 필요합니다.

데이터: 기존 web/data.json, web/news.json을 읽고 갱신합니다.
진행 상태: collector/state.json에 수집 예정시각, 실행 잠금, 페이지 진행, 캐시를 JSON으로 저장합니다. 인증키는 저장하지 않습니다.
실행 순서: 특보, 시청 대표 날씨·대기질, 인구, 출생, 월별 인구 부가통계, 기존 대기 통반 상세, 상권, 홈·교통 신규연도 점검, Google RSS.

- 특보 최소 30분 간격과 항목별 next_due 및 lease를 준수합니다.
- 날씨는 시청 대표 격자만 수집합니다. 인구 비교의 15개 동은 유지합니다.
- 인구는 최대 120단계/10분, 출생 최대 12단계, 기존 통반 대기작업 최대 8단계, 상권 최대 120단계/5분으로 제한합니다.
- 인구·출생 완료 월과 기존 교통 연도 자료는 재사용합니다.
- 교통 신규연도는 분기별 확인이며 강제 재수집하지 않습니다.
- 뉴스는 Google RSS만 사용하고 최대 150개를 유지합니다. 시청 보도자료 게시판 HTML 및 RSS 수집은 제외합니다.
- 민원 수집과 예약은 포함하지 않습니다.
- 일부 API가 실패해도 다른 작업을 계속하며 마지막 정상자료를 유지합니다.
- 자동 실행은 별도 collect.yml 워크플로를 등록해야 시작됩니다. 이 폴더 업로드만으로는 자동 수집이 시작되지 않습니다.

수집 간격은 collect.yml에서 정합니다. 계획된 30분 실행에서 날씨와 뉴스는 내부 상태에 따라 대략 시간 단위, 월별 통계는 일 단위로 확인합니다. GitHub 예약 실행은 지연될 수 있습니다.

source.zip은 유지보수용 소스입니다. 풀어서 npm install 후 npm run build를 실행하면 collector 실행 파일을 다시 생성합니다.
