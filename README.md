# Local Lakehouse (k3s)

`local-lakehouse-course`(docker-compose 실습)를 홈랩 k3s 클러스터로 옮긴 버전.
MinIO(스토리지) + Nessie(카탈로그) + Trino(쿼리엔진) + dbt(변환) + Airflow(오케스트레이션)

## 레포 구성

이 프로젝트는 레포 **2개**로 나뉜다. 로컬에서는 `local-lakehouse-k3s/` 폴더 안에 나란히 둔다.

| 폴더 | 레포 | 내용 |
|---|---|---|
| `gitops/` | [lakehouse-k3s-gitops](https://github.com/kkw855/lakehouse-k3s-gitops) | ArgoCD Application, Helm values, SealedSecret |
| `airflow/` | [lakehouse-k3s-airflow](https://github.com/kkw855/lakehouse-k3s-airflow) | Dockerfile, DAG, dbt 프로젝트, GitHub Actions |

> ⚠️ 상위 폴더 `local-lakehouse-k3s/` 에는 **`git init` 하지 말 것.** 안쪽 두 레포가 embedded repository로 잡혀서 커밋이 꼬인다.

레포를 둘로 나눈 이유: Airflow가 DAG과 dbt 프로젝트를 **git-sync sidecar로 당겨오기** 때문에 원격 git URL이 필요하고, dbt 커스텀 이미지도 GitHub Actions에서 빌드해야 한다. 매니페스트와 같은 레포에 두면 dbt 모델 한 줄 고칠 때마다 ArgoCD가 반응해서 시끄럽다.

## 아키텍처

```
postgres ns (기존 CNPG 재사용)
  ├─ Database CR: nessie    ← Nessie version store (JDBC)
  └─ Database CR: airflow   ← Airflow 메타DB
     ※ infra-k3s 레포의 apps/postgres/manifests/database-{nessie,airflow}.yaml

minio ns (기존 재사용)
  └─ 버킷 local-lakehouse   ← s3://local-lakehouse
     ※ infra-k3s 레포의 apps/minio/values-minio.yaml 의 buckets:

lakehouse-k3s ns (이 레포가 관리)
  ├─ common   SealedSecret 7개 (wave 1)
  ├─ nessie   0.108.4, JDBC → postgres-rw (wave 2)
  ├─ trino    476, coordinator 1 + worker 2 (wave 3)
  └─ airflow  3.0.6, KubernetesExecutor (wave 4)
```

강의의 컨테이너 12개가 **파드 6개**로 줄었다. Redis / 내장 Postgres / Celery worker가 통째로 사라지고, MinIO·Postgres는 클러스터에 이미 있던 것을 재사용한다.

## 전제 조건 (infra-k3s 레포)

이 레포만으로는 안 뜬다. `infra-k3s` 쪽에 아래가 먼저 있어야 한다.

- ArgoCD, Sealed Secrets 컨트롤러(`kube-system` ns, 서비스명 `sealed-secrets`), MetalLB, Traefik
- CloudNativePG 클러스터 `postgres` (`postgres-rw.postgres.svc:5432`)
- MinIO (`minio.minio.svc:9000`) + `local-lakehouse` 버킷
- `apps/postgres/manifests/database-nessie.yaml`, `database-airflow.yaml`

## 최초 설치

```bash
# 1. ArgoCD에 app-of-apps 등록 (최초 1회만 수동)
kubectl apply -f lakehouse-k3s-app.yaml
```

이후로는 `apps/` 아래에 폴더 + `*-app.yaml`을 추가하면 자식 앱이 자동 생성된다.

SealedSecret 7개는 클러스터를 새로 깔면 **다시 봉인해야 한다** (sealed-secrets 키를 백업/복원했다면 그대로 재사용 가능). 아래 "SealedSecret 재봉인" 참고.

## 접속 정보

telepresence 연결 상태에서 `서비스명.네임스페이스:포트`로 바로 접속된다.

| 서비스 | 주소 | 계정 |
|---|---|---|
| Airflow UI | http://airflow-api-server.lakehouse-k3s:8080 | admin / admin |
| Trino UI | http://trino.lakehouse-k3s:8080 | - |
| Nessie UI | http://nessie.lakehouse-k3s:19120 | - |
| MinIO 콘솔 | http://minio-console.minio:9001 | `minio-auth` Secret 참조 |

```bash
telepresence connect
```

telepresence 없이 쓰려면 port-forward. 이 경우 강의와 같은 주소가 된다.

```bash
kubectl -n lakehouse-k3s port-forward svc/trino 8080:8080
```

## 파이프라인 실행

Airflow UI에서 `dbt_pipeline` DAG을 켜고 트리거. `dbt_seed` → `dbt_run` 순으로 돌고, task마다 Pod이 하나씩 뜬다.

```
seeds/*.csv → (dbt seed) → landing   5개 테이블
            → (dbt run)  → staging   5개
            → (dbt run)  → curated   dim_country / dim_product / fact_sale
```

결과 확인:

```bash
kubectl -n lakehouse-k3s exec deploy/trino-coordinator -- trino --server localhost:8080 --user batman --execute "SHOW SCHEMAS FROM iceberg"
```

기준값 — `fact_sale` 843행, `dim_product` 293행, `dim_country` 6행.

## 개발 흐름

| 무엇을 고쳤나 | 무슨 일이 일어나나 |
|---|---|
| `airflow` 레포의 `dags/**` | git-sync가 수십 초 내 반영. **이미지 빌드 없음** |
| `airflow` 레포의 `Dockerfile` / `requirements-dbt.txt` | GH Actions → ghcr 푸시 → gitops 레포에 태그 커밋 → ArgoCD 롤아웃 (전자동) |
| `gitops` 레포의 values | ArgoCD가 3분 내 반영 |

CI가 `values-airflow.yaml`의 이미지 태그를 커밋 SHA로 갱신하므로, **워크플로가 돈 뒤에는 gitops 레포를 `git pull` 해야 로컬이 최신이 된다.** 안 하면 다음 push에서 충돌한다.

---

# 알아두면 좋은 것들 (삽질 방지)

## ArgoCD

**`operationState.phase: Running`에 갇히면 새 커밋이 반영되지 않는다.**
자동 동기화는 진행 중인 작업이 끝나야 다음을 시작한다. 절대 Healthy가 될 수 없는 리소스를 만들면 영원히 갇히고, 잘못된 값을 고쳐 push해도 소용없다. 증상이 이상하면 여기부터 본다.

```bash
kubectl -n argocd get app lakehouse-k3s-airflow -o jsonpath='{.status.operationState.phase}'
kubectl -n argocd patch app lakehouse-k3s-airflow --type merge -p '{"operation":null}'   # 또는 UI의 TERMINATE
```

**`Unknown` 상태는 "동기화 실패"가 아니라 "비교 자체를 못 함"이다.** 진짜 이유는 항상 conditions에 있다.

```bash
kubectl -n argocd get app <앱> -o jsonpath='{.status.conditions[0].message}'
```

**`directory.include: "*.yaml"` 은 `.yml`을 잡지 않는다.** 파일을 추가했는데 아무 일도 안 일어나고 `Synced`만 유지되면 확장자부터 확인. 에러가 안 나서 제일 찾기 어렵다.

**`chart:` 와 `path:` 는 상호배타적이다.**

| 필드 | 의미 | `targetRevision` |
|---|---|---|
| `chart:` | Helm 레포의 차트 이름 | 차트 버전 (`1.40.0`) |
| `path:` | git 레포 안의 디렉터리 | git 레퍼런스 (`main`) |

`chart:`가 남아 있으면 `repoURL`을 git이 아니라 Helm 레포로 해석해서 `helm pull`을 시도한다.

**Application 이름과 Helm 릴리스 이름은 별개다.** 지정하지 않으면 릴리스명이 앱 이름이 되어 `lakehouse-k3s-trino-trino-coordinator` 같은 이름이 나온다. `helm.releaseName`으로 고정한다 (Trino는 `trino`, Nessie는 `nessie`). **나중에 바꾸면 리소스가 전부 재생성**되므로 처음에 정해야 한다.

## SealedSecret

**빈 값을 봉인해도 아무도 에러를 내지 않는다.** 컨트롤러는 `SYNCED=True`로 정상 복호화하고, 문제는 최종 소비자(psycopg2, Flask)가 죽고 나서야 드러난다. 이 프로젝트에서 두 번 겪었다 (`airflow-metadata`, `airflow-api-secret-key`).

봉인 **전에** 값 길이를 반드시 찍어볼 것. 변수 선언과 `kubeseal`을 **한 명령으로 이어서** 실행해야 한다 (셸이 나뉘면 변수가 안 넘어간다).

```bash
KEY=$(openssl rand -hex 32) && echo "길이=${#KEY}" && \
kubectl create secret generic ... --from-literal=key="$KEY" --dry-run=client -o yaml | kubeseal ...
```

값이 들어갔는지 사후 확인:

```bash
kubectl -n lakehouse-k3s get secret <이름> -o jsonpath='{.data.<키>}' | base64 -d | wc -c
```

**Secret을 고쳐도 파드는 자동으로 안 바뀐다.** 환경변수는 파드 생성 시점에 주입된다.

```bash
kubectl -n lakehouse-k3s rollout restart deploy/airflow-api-server deploy/airflow-scheduler deploy/airflow-dag-processor
```

**scope는 strict(기본)라 네임스페이스가 암호문에 묶인다.** `metadata.namespace`만 고쳐선 안 되고 다시 봉인해야 한다. `kubeseal`에 `--controller-name sealed-secrets --controller-namespace kube-system`이 필요하다 (기본값은 `sealed-secrets-controller`인데 이 클러스터의 서비스명은 `sealed-secrets`).

## k3s / 스토리지

**`local-path`는 ReadWriteOnce만 지원한다.** Airflow 차트는 로그 PVC를 ReadWriteMany로 요청하므로 PVC가 영원히 Pending이 되고 파드가 전부 스케줄 불가에 빠진다. → `logs.persistence.enabled: false`

Airflow 3은 kubelet에서 직접 로그를 읽어오므로 공유 볼륨 없이도 UI에서 로그가 보인다. 다만 **task Pod이 삭제된 뒤에는 과거 로그가 사라진다.** 영속화가 필요해지면 MinIO 원격 로깅으로 올린다.

**StatefulSet은 in-place 수정이 안 되는 필드가 있다.** 볼륨 구성을 바꾸면 `Forbidden: updates to statefulset`이 난다. 삭제 후 재생성해야 하는데, **`--cascade=orphan`으로 지우면 남은 파드를 새 StatefulSet이 구 스펙째로 재입양한다.** 파드까지 함께 지워야 한다.

**롤링 업데이트는 새 파드가 Ready 돼야 구 파드를 내린다.** 구 파드가 없어진 PVC를 기다려 Pending이면 서로 물려 영원히 안 끝난다. 구 파드를 직접 삭제.

## Airflow + dbt

**git-sync 볼륨은 읽기 전용이다.** dbt는 프로젝트 디렉터리에 `target/`과 `logs/`를 쓰므로 그대로 실행하면 **출력 한 줄 없이 exit 2로 죽는다.** 게다가 task Pod은 쿠버네티스 레벨에서 `Succeeded`로 보여서 원인 파악이 어렵다.

`dags/custom_operator/dbt_operator.py`에서 경로를 `/tmp`로 돌려둔 이유가 이것이다.

```python
"--target-path", "/tmp/dbt-target",
"--log-path", "/tmp/dbt-logs",
```

> `dbt debug`는 `--target-path`를 받지 않는다 (`--log-path`만 있음). 명령을 추가할 때 주의.

**ArgoCD + Helm hook 교착.** 차트 기본값 `useHelmHooks: true`면 마이그레이션 Job이 PostSync가 되는데, init 컨테이너가 그 Job을 기다리느라 Sync가 안 끝나고, 그래서 PostSync가 영원히 실행되지 않는다. `Job 수: 0`이 계속 나오면 이것.

```yaml
migrateDatabaseJob:
  useHelmHooks: false
  applyCustomEnv: false
createUserJob:
  useHelmHooks: false
  applyCustomEnv: false
```

**차트가 fernet / jwt / api secret을 렌더마다 난수로 새로 만든다.** ArgoCD `selfHeal`이 3분마다 이걸 교체해서 세션이 끊기고 파드가 재시작한다. 넷 다 SealedSecret으로 고정해야 한다.

| values 키 | Secret | 키 이름 |
|---|---|---|
| `data.metadataSecretName` | `airflow-metadata` | `connection` |
| `fernetKeySecretName` | `airflow-fernet-key` | `fernet-key` |
| `jwtSecretName` | `airflow-jwt-secret` | `jwt-secret` |
| `apiSecretKeySecretName` | `airflow-api-secret-key` | `api-secret-key` |

**키 이름이 정확히 저것이어야 한다.** 차트 `templates/_helpers.yaml`이 그 키로 `secretKeyRef`를 만든다.

**FAB auth manager는 `[api] secret_key`를 읽는다** (`providers/fab/www/app.py`). Airflow 3에서 `[webserver] secret_key`는 쓰이지 않는다. 로그인 시 `'NoneType' object has no attribute 'sign'` 500 에러가 나면 `AIRFLOW__API__SECRET_KEY` 값이 비었다는 뜻.

**차트 appVersion(3.0.2)과 `airflowVersion`(3.0.6)이 달라도 된다.** 템플릿 분기가 전부 major.minor 경계(`>=3.0.0`, `<3.0.0`)라 patch 차이는 렌더 결과가 완전히 동일하다. 단 **minor를 건너뛰면(3.2.x 등) 안 된다.**

**스키마 생성(`init.sql` 대응물)은 필요 없다.** dbt가 `dbt_project.yml`의 `seeds.schema` / `models.+schema`를 보고 `landing`/`staging`/`curated`를 알아서 만든다. 검증 완료 — 스키마 3개를 지우고 DAG을 돌리면 행 수까지 동일하게 복구된다.

> 참고로 강의의 `trino_config/coordinator/init.sql`도 **원래 자동 실행되지 않았다.** 파일만 마운트돼 있었을 뿐이다.

## Trino

**카탈로그 values는 평문 ConfigMap으로 렌더링된다.** `s3.aws-secret-key`를 그대로 쓰면 public 레포에 MinIO 비밀번호가 박힌다. Trino의 `${ENV:VAR}` 치환을 쓴다.

```yaml
catalogs:
  iceberg: |
    s3.aws-access-key=${ENV:MINIO_USER}
    s3.aws-secret-key=${ENV:MINIO_PASSWORD}
env:
  - name: MINIO_USER
    valueFrom: { secretKeyRef: { name: minio-secret, key: minio-user } }
```

`envFrom` + `secretRef`는 **쓸 수 없다.** 키 이름 `minio-user`에 하이픈이 있어 유효한 C 식별자가 아니고, 쿠버네티스가 조용히 건너뛴다. `env` + `valueFrom`으로 이름을 바꿔 매핑해야 한다.

**차트 기본 JVM 힙이 coordinator 8G + worker 8G × 2 = 24GB다.** 실습 데이터엔 과하므로 2G로 낮춰뒀다.

**`DROP SCHEMA ... CASCADE`는 MinIO 데이터 파일까지 정리한다.** 반면 `DROP TABLE`은 카탈로그 참조만 끊고 파일을 고아로 남긴다 (강의 README에 적어둔 그 현상). dbt의 table materialization도 매번 새 UUID 폴더를 만들고 이전 것을 남긴다.

## 기타

**Nessie Helm 레포 도메인(`charts.projectnessie.org`)이 소멸했다.** `.org` 레지스트리가 NXDOMAIN을 반환한다(2026-08-26 확인). 차트 실물은 GitHub Releases에 있고 컨테이너 이미지도 ghcr.io에서 멀쩡하다. 그래서 차트를 `apps/nessie/chart/`에 **vendoring**해뒀다.

```bash
curl -sLO https://github.com/projectnessie/nessie/releases/download/nessie-0.108.4/nessie-helm-0.108.4.tgz
tar xzf nessie-helm-0.108.4.tgz && mv nessie chart
```

도메인이 복구되면 `nessie-app.yaml`의 `sources[0]`을 원격 차트로 되돌리면 된다. `values-nessie.yaml`은 손댈 필요 없다.

**Nessie는 RocksDB 대신 JDBC를 쓴다.** 강의는 RocksDB + 볼륨이라 `user: "0:0"` 권한 우회가 필요했는데, CNPG를 쓰면서 PVC와 그 삽질이 통째로 사라졌다. 상태가 Postgres에 있으니 `replicaCount`를 늘리는 것도 가능하다.

**telepresence는 연결 시점 이후에 생긴 서비스 이름을 못 잡는다.** 부정 응답(NXDOMAIN)이 캐시된다. 새 네임스페이스나 서비스를 만든 뒤 이름이 안 잡히면 재연결.

```bash
telepresence quit && telepresence connect
```

급하면 ClusterIP로 직접 접속해도 된다 (`kubectl -n lakehouse-k3s get svc`).

**GHCR 패키지는 첫 push 시 private으로 생성된다.** 레포가 public이어도 그렇다. 이 프로젝트는 private을 유지하고 `ghcr-secret`(dockerconfigjson) + `registry.secretName`으로 붙였다. Secret 타입이 `Opaque`면 kubelet이 무시하니 반드시 `kubernetes.io/dockerconfigjson`이어야 한다.

**`zsh`에서 `jsonpath` 출력 끝의 `%`는 값이 아니다.** 개행 없이 끝난 출력을 zsh가 표시하는 것. `; echo`를 붙이면 사라진다.
