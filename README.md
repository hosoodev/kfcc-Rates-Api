# 새마을금고 정적 금리 API

`kfcc-Rates-Api`는 새마을금고 금리·금고·경영실태평가의 **생성된 정적 JSON 데이터만 공개하는 저장소**입니다. `main`의 `v2/`를 GitHub Pages에 배포하며, 데이터 수집과 가공은 별도 비공개 저장소 `hosoodev/kfcc-Rates-Crawler`에서 수행합니다.

API 기본 주소는 **https://api.mgija.com**입니다. 저장소의 `v2/`가 사이트 루트로 배포되므로 요청 URL에는 `/v2`를 붙이지 않습니다.

| 데이터 | URL |
| --- | --- |
| 메인 데이터 | [main.json](https://api.mgija.com/main.json) |
| 갱신 상태 | [status.json](https://api.mgija.com/status.json) |
| 금고 목록 | [meta/banks.json](https://api.mgija.com/meta/banks.json) |
| 금리 요약 | [rates/summary.json](https://api.mgija.com/rates/summary.json) |
| 예금 금리 | [rates/deposit/all.json](https://api.mgija.com/rates/deposit/all.json) |
| 적금 금리 | [rates/saving/all.json](https://api.mgija.com/rates/saving/all.json) |
| 입출금 금리 | [rates/demand/all.json](https://api.mgija.com/rates/demand/all.json) |
| 금고 상세 | `https://api.mgija.com/branches/{gmgoCd}.json` |
| 경영실태평가 목록 | [grades/index.json](https://api.mgija.com/grades/index.json) |

비공개 크롤러가 생성한 `v2/`를 이 저장소의 `main`에 반영하면, 공개 워크플로우가 필수 엔드포인트와 JSON·gzip 파일을 검증한 뒤 Pages를 배포합니다. PR에서는 검증만 실행됩니다. 배포 성공 후 `SITE_URL`과 `REVALIDATE_SECRET`이 설정되어 있으면 프론트엔드 캐시 갱신을 요청합니다.

기존 `api-data` 및 `og-images` 브랜치는 호환성을 위해 유지합니다. API 배포 원본은 이제 `main`입니다. 기존 공개 커밋 이력에는 이전 크롤러 소스가 남아 있으며, 이번 분리는 현재 `main`의 파일 구성을 변경합니다. 기존 이력은 다시 쓰지 않습니다.
