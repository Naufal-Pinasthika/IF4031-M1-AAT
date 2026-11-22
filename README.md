# Hazard Coordination Platform

Sistem integrasi data kebencanaan BNPB untuk IF4031, melanjutkan Milestone 1
ke Milestone 2. Sumber BMKG, PVMBG, Feed Satelit, dan Sensor Pantai dipetakan
ke `HazardEvent`, disimpan lintas node, lalu dibaca melalui Client API dan
diterbitkan ke consumer Kafka.

README ini menjelaskan konfigurasi, deployment, dan pengujian. Keberadaan
implementasi atau pod berstatus Ready **bukan** pernyataan seluruh kriteria
lulus; hasil setiap kampanye harus diperiksa beserta versi image dan buktinya.

## Daftar isi

- [Arsitektur dan topologi](#arsitektur-dan-topologi)
- [Menjalankan sistem dari awal](#menjalankan-sistem-dari-awal)
- [Konfigurasi](#konfigurasi)
- [Akses dan API](#akses-dan-api)
- [Observabilitas](#observabilitas)
- [Pengujian P1–P5 dan Tester Agent](#pengujian-p1p5-dan-tester-agent)
- [Pemeriksaan pengembangan](#pemeriksaan-pengembangan)
- [Menghentikan sistem](#menghentikan-sistem)

## Arsitektur dan topologi

```text
BMKG / PVMBG / Sensor Pantai ---- polling pemilik lease ----+
Feed Satelit ------------------- SSE + checkpoint ---------+--> Aggregator
Tim Lapangan --> Client API --> gateway --> durable inbox -+       |
                                                                +--> canonical PostgreSQL lintas node
Client --> Auth --> JWT --> Client API --> gateway --> Aggregator |
                                                                +--> outbox --> Kafka
                                                                                |--> dashboard-consumer
                                                                                +--> notifier-consumer
```

| Problem M2 | Mekanisme utama dan lokasi implementasi |
| --- | --- |
| P1: penyimpanan terdistribusi | Partisi kanonik, dua salinan lintas node, pembacaan/pagination global, pemulihan, dan agregasi analitik di [store](services/aggregator/internal/store/) |
| P2: koordinasi dan kapasitas | Kepemilikan polling per sumber, waktu coordinator, fencing, polling ledger, cache/proyeksi baca, serta gateway di [Aggregator](services/aggregator/) |
| P3: perubahan skema dan laporan | Raw inbox PostgreSQL, klasifikasi `retrying`/`parked`, pemeriksaan Ops, replay berurutan, dan penerimaan laporan durabel di [inbox.go](services/aggregator/internal/aggregator/inbox.go) dan [ingress.go](services/aggregator/internal/store/ingress.go) |
| P4: aliran hotspot | SSE yang dapat dilanjutkan, state durabel, pengelompokan berdasarkan waktu deteksi, update/retract, dan proyeksi idempoten di [Aggregator](services/aggregator/) |
| P5: ketahanan Sensor Pantai | Timeout/backoff, circuit breaker, isolasi sumber, pemeriksaan kesegaran, prioritas Tim Lapangan, dan jalur Kafka kritis menuju [notifier](services/notifier-consumer/) |

Inbox/parked messages P3 bukan topic DLQ Kafka. Kafka digunakan untuk publikasi
event ke consumer; state consumer bukan sumber kebenaran kanonik.

### Deklarasi topologi lab

Deployment VPS menggunakan **tiga VM Ubuntu 24.04 dengan kernel terpisah**:

| Node | Jenis dan peran | Alokasi CPU/RAM VM | Jaringan privat |
| --- | --- | --- | --- |
| A / `aat-vps-a` | VPS Biznet; k3s control plane sekaligus worker | 16 vCPU / 16 GB | `10.77.0.1` |
| B / `aat-vps-b` | VPS Biznet; worker | 8 vCPU / 8 GB | `10.77.0.2` |
| C / `aat-vps-c` | VPS Biznet; worker | 8 vCPU / 8 GB | `10.77.0.3` |

Host fisik/hypervisor A, B, dan C dikelola Biznet; identitas serta spesifikasi
CPU/RAM host fisiknya tidak diberikan kepada kelompok. Angka di atas adalah
alokasi **VM**, bukan spesifikasi host fisik. Penempatan pada host fisik berbeda
tidak diklaim atau dibuktikan hanya dari tiga IP/VM. Keterangan penyedia masih
diperlukan jika identitas host fisik diminta untuk deklarasi atau bonus.

VPS D adalah generator k6 opsional di luar cluster (`10.77.0.10`), bukan node
aplikasi. Laptop bertindak sebagai controller SSH. Endpoint publik, pengguna
SSH, dan lokasi private key berada di inventory privat, bukan README.

Antar-node memakai WireGuard `aatwg0`; k3s memakai jaringan tersebut.
SSH menuju IP publik tidak mengharuskan laptop bergabung ke VPN. API cluster,
database, dan Kafka tidak perlu dibuka ke internet; akses operator melalui
tunnel SSH. Konfigurasi VPN node berada di `/etc/wireguard/aatwg0.conf` dan
mengandung private key, sehingga tidak boleh dibagikan.

### Kontrak deployment M2

- Semua komponen BNPB berada di namespace
  `sistem-informasi-kebencanaan-bnpb`, dengan label `role` sesuai kelas komponen.
- Manifest dasar menjalankan tiga Aggregator lintas node. Gateway per node
  berupa DaemonSet, tetapi client menggunakan satu endpoint Service, bukan
  memilih pod Aggregator. Probe/readiness dan discovery mengeluarkan backend
  yang tidak siap dari jalur layanan.
- Tiga unit kanonik PostgreSQL memakai disk lokal node dan anti-affinity wajib.
  Nama unit `postgres-a` tidak menjamin pod tersebut ditempatkan pada node A.
- Secret dan ConfigMap dibentuk oleh [k8s_config.py](scripts/k8s_config.py).
  [NetworkPolicy](k8s/base/platform/network-policies.yaml) membatasi koneksi
  penyimpanan menurut pemiliknya; pemisahan tidak hanya berupa konvensi kode.
- Baseline [k8s/base](k8s/base/) masih memiliki Kafka dan coordinator satu
  instance. [k8s/ha](k8s/ha/) adalah konfigurasi tambahan, bukan bukti bahwa
  semua layanan produksi sudah HA. Jumlah Aggregator tidak membuktikan HA
  seluruh dependensinya.

Memori per pod mengikuti Lampiran B; CPU dapat dideklarasikan dalam manifest:

| Kelas komponen | requests | limits |
| --- | --- | --- |
| Aggregator | 256Mi | 512Mi |
| Penyimpanan | 512Mi | 1Gi |
| Broker/pemroses stream | 512Mi | 1Gi |
| Consumer/pemroses event | 128Mi | 256Mi |
| Auth/Client API | 128Mi | 256Mi |
| Mock BMKG/PVMBG | 64Mi | 128Mi |
| Komponen lain | 128Mi | 256Mi |

Sensor Pantai dan Tester Agent tetap menggunakan image resmi yang dipin;
kontrak vendor tidak diganti untuk melewati pengujian. Pengujian beban berjalan
di luar pod komponen aplikasi.

## Menjalankan sistem dari awal

Semua perintah berikut dijalankan dari root repositori ini dalam **Bash di
Linux/WSL**. Jangan menjalankan skrip VPS langsung dari PowerShell: beberapa
skrip memakai fasilitas Linux seperti `resource` dan socket SSH.

### 1. Persiapan controller dan kredensial

Siapkan Git, Python 3, OpenSSH client, rsync, cURL, OpenSSL, dan utilitas Bash.
Go 1.25+ diperlukan untuk pemeriksaan kode; Node.js diperlukan untuk tes UI.
Docker Compose diperlukan hanya untuk pengembangan lokal. k6 diperlukan pada
mesin yang benar-benar mengirim beban, bukan pada setiap node aplikasi.

```bash
git clone https://github.com/Naufal-Pinasthika/IF4031-M1-AAT.git
cd IF4031-M1-AAT
bash scripts/setup-env.sh
cp infra/vps/inventory.example.json infra/vps/inventory.json
chmod 600 .env infra/vps/inventory.json
```

Sunting `.env` dan `inventory.json` sebelum melanjutkan:

- `setup-env.sh` membuat secret lokal unik dan kunci penandatangan JWT. Isi
  `SATELLITE_TOKEN` dengan token kelompok dari asisten. Token ini tidak
  diganti dengan kunci SSH atau token client.
- Isi `key` dengan path private key **Linux/WSL**, lalu isi tiga entri `nodes`
  berurutan server A, agent B, agent C. Ganti IP publik dan pengguna sesuai VPS.
  Hapus entri `load_generator` jika D tidak digunakan.
- Setiap VPS harus dapat diakses melalui SSH key dan `sudo -n`, serta dapat
  mengunduh paket/image. Verifikasi fingerprint host SSH melalui kanal tepercaya
  sebelum menyimpan host di `known_hosts`; tooling memakai pemeriksaan host ketat.
- Untuk cluster yang sudah ada, gunakan `.env` yang cocok dengan kredensial
  deployment tersebut. Jangan membuat secret baru hanya untuk menjalankan tes.

`setup-env.sh` menolak menimpa `.env` yang ada. `--merge` menambahkan variabel
yang belum ada; `--force` mengganti konfigurasi dan bukan prosedur upgrade biasa.
`.env`, inventory, private key, backup, dan artefak pengujian tidak boleh masuk Git.

### 2. Provision dan deploy VPS dengan satu perintah

Setelah tiga VPS tersedia dan inventory diisi:

```bash
python3 infra/vps/deploy.py --provision
```

Perintah ini memasang kebutuhan host, WireGuard dan k3s, membangun image di A,
mengimpornya ke B/C, menerapkan platform/aplikasi, lalu memasang Tester Agent
resmi. Tidak perlu Docker/Kubernetes lokal pada laptop. Tool ini memprovision
**VPS yang sudah dibeli**, bukan membuat VM melalui API penyedia/Terraform.

Provisioner memilih versi k3s stable satu kali untuk ketiga node. Untuk
reproduksibilitas versi, gunakan versi yang telah divalidasi oleh kelompok:

```bash
python3 infra/vps/provision.py --version <versi-k3s-tervalidasi> provision
python3 infra/vps/deploy.py
```

Cluster harus mendukung Service `trafficDistribution: PreferSameNode` yang
dipakai manifest. StorageClass harus node-local, bukan penyimpanan jaringan.
Provisioner saat ini mengharapkan tepat tiga node; batas minimum spesifikasi
dua node tidak berarti manifest ini otomatis dapat dipasang pada dua node.

Untuk memperbarui source/image tanpa memprovision ulang host:

```bash
python3 infra/vps/deploy.py
python3 infra/vps/access.py status
python3 infra/vps/access.py smoke
```

Build ulang tanpa cache layer: `python3 infra/vps/deploy.py --no-cache`.
Ini **tidak menghapus database/PVC, offset Kafka, atau state Sensor Pantai**.
`--tag <tag>` menentukan versi image; `--skip-build --tag <tag>` menggunakan
image yang sudah dibangun. Simpan commit, tag, dan konfigurasi setiap pengujian.
Redeploy juga bukan pembenaran untuk menurunkan versi database/broker yang
sudah dimigrasikan: periksa kompatibilitas live sebelum mengganti image platform.

### 3. Alternatif cluster Kubernetes yang sudah tersedia

Dengan kubeconfig yang benar, node-local StorageClass, dan seluruh image
`aat/<service>:<tag>` sudah diimpor pada node yang membutuhkannya:

```bash
bash scripts/deploy-k8s.sh --k3s --image-tag <tag>
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
kubectl -n sistem-informasi-kebencanaan-bnpb get pods -o wide
```

Entrypoint `deploy.py` mengurus impor image, NodePort privat, dan pemasangan
Agent; `deploy-k8s.sh` saja tidak menyediakan seluruh akses VPS tersebut.

### Pengembangan lokal dengan Compose

Isi `SATELLITE_TOKEN` dahulu; Compose mengharuskannya terisi. Pada Kubernetes,
token kosong menonaktifkan feed, tetapi berarti jalur live P4 belum dijalankan.

```bash
docker compose config --quiet
docker compose up -d --build
docker compose ps
curl -sS --max-time 5 -w '\nHTTP %{http_code}; time_total=%{time_total}s\n' http://localhost:8080/health
```

Compose menggunakan satu lingkungan lokal, bukan topologi demo M2. kind/k3d
atau multi-node container berkernel bersama juga bukan bukti node berkernel
terpisah. Sensor pada Compose menyimpan `/data` pada named volume; deployment
Kubernetes memakai state sesuai manifestnya. Jangan mengasumsikan restart
container selalu mengatur ulang waktu virtual sensor.

## Konfigurasi

Daftar variabel dan nilai bawaan ada di [`.env.example`](.env.example).
Berikut pengelompokannya; jangan menaruh nilai rahasia nyata dalam README.

| Kegunaan | Variabel |
| --- | --- |
| Kredensial sumber | `BMKG_API_KEY`, `PVMBG_TOKEN`, `PANTAI_AUTH_TOKEN`, `SATELLITE_TOKEN` |
| Endpoint sumber | `BMKG_URL`, `PVMBG_URL`, `COASTAL_URL`, `SATELLITE_URL` |
| Mock | `BMKG_EVENT_INTERVAL_MS`, `PVMBG_EVENT_INTERVAL_MS`, `PVMBG_DELAY_MIN_MS`, `PVMBG_DELAY_MAX_MS`, `PVMBG_ADMIN_LISTEN_ADDRESS`, `PVMBG_ADAPTER_VERSION` |
| Konfigurasi resmi sensor | `PANTAI_PROFILE`, `PANTAI_SEED`, `PANTAI_SPEED` |
| Akses Aggregator | `AGGREGATOR_API_KEY`, `AGGREGATOR_LISTEN_ADDRESS`, `AGGREGATOR_URL` |
| Polling dan koordinasi | `AGGREGATOR_POLL_INTERVAL_MS`, `AGGREGATOR_HTTP_TIMEOUT_MS`, `AGGREGATOR_START_SINCE`, `AGGREGATOR_LEASE_TTL_MS`, `COORDINATION_TIMEOUT_MS` |
| Kapasitas dan baca | `AGGREGATOR_MAX_IN_FLIGHT`, `AGGREGATOR_RESPONSE_SLOTS`, `AGGREGATOR_FIELD_RESERVED_SLOTS`, `AGGREGATOR_SHARED_READ_PROJECTION`, `AGGREGATOR_READ_CACHE_TTL_MS` |
| Ketahanan sensor | `COASTAL_POLL_INTERVAL_MS`, `COASTAL_HTTP_TIMEOUT_MS`, `COASTAL_LEASE_TTL_MS`, `COASTAL_MAX_STALENESS_MS`, `COASTAL_CIRCUIT_FAILURES`, `COASTAL_CIRCUIT_OPEN_MS` |
| Penyimpanan Aggregator | `POSTGRES_ADMIN_PASSWORD`, `POSTGRES_APP_USER`, `POSTGRES_APP_PASSWORD`, `DATABASE_URL`, `COORDINATOR_POSTGRES_PASSWORD`, `COORDINATION_DATABASE_URL` |
| Partisi/Audit P1 | `P1_STORAGE_A_URL`, `P1_STORAGE_B_URL`, `P1_STORAGE_C_URL`, `P1_STORE_TIMEOUT_MS`, `P1_AUDIT_TOKEN`, `P1_IMPORT_ONLY` |
| Kafka | `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_CRITICAL_TOPIC` |
| Dashboard consumer | `DASHBOARD_CONSUMER_GROUP`, `DASHBOARD_LISTEN_ADDRESS`, `DASHBOARD_STORE_PATH` |
| Notifier | `NOTIFIER_CONSUMER_GROUP`, `NOTIFIER_LISTEN_ADDRESS`, `NOTIFIER_STORE_PATH` |
| Penyimpanan Auth | `AUTH_POSTGRES_PASSWORD`, `AUTH_DATABASE_URL` |
| JWT/JWKS | `AUTH_LISTEN_ADDRESS`, `AUTH_JWKS_URL`, `AUTH_ACCESS_TOKEN_TTL_SECONDS`, `AUTH_REFRESH_TOKEN_TTL_SECONDS`, `AUTH_JWT_ISSUER`, `AUTH_JWT_AUDIENCE`, `AUTH_JWT_KEY_ID`, `AUTH_JWT_PRIVATE_KEY_B64` |
| Kapasitas Auth/Client API | `AUTH_MAX_IN_FLIGHT`, `AUTH_FIELD_RESERVED_SLOTS`, `CLIENT_API_MAX_IN_FLIGHT`, `CLIENT_API_FIELD_RESERVED_SLOTS` |
| Identitas client | `MEDIA_CLIENT_ID`, `MEDIA_CLIENT_SECRET`, `FIELD_CLIENT_ID`, `FIELD_CLIENT_SECRET`, `OPS_CLIENT_ID`, `OPS_CLIENT_SECRET` |

Nilai dengan akhiran `_MS` dalam milidetik; TTL Auth berakhiran `_SECONDS`
dalam detik. `*_PASSWORD`, `*_SECRET`, token, private key JWT, dan URL database
berkredensial adalah rahasia. `P1_AUDIT_TOKEN` adalah token Audit API, bukan JWT
client; gunakan token asisten saat pengujian resmi tersedia.

Renderer Kubernetes membentuk URL database dan DNS service internal sendiri,
mengaktifkan listener admin PVMBG `:8089` khusus Agent, serta memisahkan Secret
dari ConfigMap. Karena itu, tidak semua nilai endpoint lokal `.env` diteruskan
apa adanya ke cluster. Konfigurasi gateway dan penempatan juga terdapat pada
[manifest gateway](k8s/base/apps/gateway.yaml) dan [workloads](k8s/base/apps/workloads.yaml).
Jangan mengubah seed/speed/profil untuk mencocokkan pola tes; aplikasi harus
menangani Sensor Pantai sebagai black box.

## Akses dan API

Pada dua terminal terpisah, biarkan tunnel berikut tetap berjalan:

```bash
python3 infra/vps/access.py tunnel
```

```bash
python3 infra/vps/access.py observe
```

| Layanan | Akses VPS dari laptop melalui tunnel | Compose lokal |
| --- | --- | --- |
| Client API / dashboard analitik | `http://localhost:30080` / `/dashboard` | `http://localhost:8080` / `/dashboard` |
| Auth | `http://localhost:30084` | `http://localhost:8084` |
| Aggregator melalui gateway | `http://localhost:30083` | Internal jaringan Compose, tidak dipublikasikan ke host |
| Notifier | `http://localhost:30086` | `http://localhost:8086` |
| Sensor Pantai | `http://localhost:30090` | Internal jaringan Compose |
| Dashboard operasi/metrik | `http://localhost:8090` melalui `observe` | Tidak dipasang oleh Compose |

Port Kubernetes tersebut bukan port yang perlu dibuka ke internet. Default
namespace dan pemetaan port ada di [vps_config.py](infra/vps/vps_config.py).
Perintah kubectl dapat dijalankan tanpa kubeconfig lokal:

```bash
python3 infra/vps/access.py kube -- -n sistem-informasi-kebencanaan-bnpb get pods -o wide
```

### Token client dan permintaan terukur

Auth menerbitkan JWT melalui `POST /v1/token` dengan HTTP Basic client ID/secret.
Media memperoleh ringkasan, Tim Lapangan memperoleh akses sesuai scope-nya
termasuk laporan, dan Internal Ops memperoleh pemeriksaan/replay pesan gagal.
Client API memverifikasi JWT menggunakan JWKS; `POST /v1/token/refresh`
memutar refresh token. Gunakan pasangan baru setelah refresh, bukan token lama.

Contoh berikut meminta kredensial tanpa menuliskannya sebagai literal di shell
history. Respons Auth berisi token rahasia; jangan merekamnya ke bukti publik.

```bash
read -r -p 'Client ID dari .env: ' CLIENT_ID
read -r -s -p 'Client secret dari .env: ' CLIENT_SECRET; printf '\n'
curl -sS --max-time 10 --user "$CLIENT_ID:$CLIENT_SECRET" \
  -X POST http://localhost:30084/v1/token \
  -w '\nHTTP %{http_code}; time_total=%{time_total}s\n'
unset CLIENT_SECRET
read -r -s -p 'Access token dari respons Auth: ' ACCESS_TOKEN; printf '\n'
curl -sS --max-time 10 http://localhost:30080/events?limit=20 \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'X-Correlation-ID: readme-events-001' \
  -w '\nHTTP %{http_code}; time_total=%{time_total}s\n'
unset ACCESS_TOKEN
```

Untuk Compose, ganti port `30084`/`30080` menjadi `8084`/`8080`.
Dashboard `/dashboard` menerima **access token Client API**, bukan
`AGGREGATOR_API_KEY`, token feed, atau refresh token.

| Antarmuka | Endpoint penting |
| --- | --- |
| Client API | `GET /events`, `GET /events/{id}`, `GET /source-status`, `GET /analytics`, `GET /dashboard` |
| Tim Lapangan | `POST /field-reports`, `GET /field-reports/{id}`; balasan 202 berarti diterima durabel, bukan selesai diproses |
| Internal Ops | `GET /unprocessed`, `GET /unprocessed/{id}`, `POST /unprocessed/replay`, `GET /polling`, `GET /satellite-status` |
| Auth | `POST /v1/token`, `POST /v1/token/refresh`, `GET /.well-known/jwks.json` |
| Audit melalui Aggregator | `GET /internal/audit/events`, `/internal/audit/checksum`, `/internal/audit/location`, `/internal/audit/unprocessed` |
| Sensor resmi | `GET /healthz` tanpa token; `GET /v1/stations` dan `/v1/observations` dengan Bearer `PANTAI_AUTH_TOKEN` |

Audit memakai `Authorization: Bearer <P1_AUDIT_TOKEN>`. Ikuti `next_cursor`
sampai `null`; periksa `partial` dan `unavailable_units`, bukan hanya HTTP 200.
Checksum dihitung atas `source_ref_id` yang diurutkan, bukan atas body HTTP.
Contoh dengan token yang dimasukkan secara privat:

```bash
read -r -s -p 'Audit token: ' AUDIT_TOKEN; printf '\n'
curl -sS --max-time 10 'http://localhost:30083/internal/audit/events?limit=50' \
  -H "Authorization: Bearer $AUDIT_TOKEN" \
  -H 'X-Correlation-ID: readme-audit-001' \
  -w '\nHTTP %{http_code}; time_total=%{time_total}s\n'
unset AUDIT_TOKEN
```

Jika tes P3 menerima `401` dari `/admin/schema-version`, periksa `PVMBG_TOKEN`
pada `.env`/`--env-file` terhadap kredensial deployment. SSH yang berhasil tidak
membuktikan token aplikasi benar. Port data PVMBG `8082` tetap memerlukan Bearer
token; listener Agent `8089` adalah jalur kontrol terpisah dan dibatasi platform.

## Observabilitas

Dashboard analitik P1 (`/dashboard`) menampilkan jumlah kejadian terhadap waktu,
severity, dan wilayah dari seluruh lokasi penyimpanan; status tidak lengkap
ditampilkan saat datanya parsial. Ini berbeda dari dashboard operasi `:8090`.

Dashboard operasi menggunakan Prometheus untuk throughput, latensi, error 5xx,
429, kesehatan scrape, serta metrik sumber/consumer. Masukkan correlation ID
permintaan contoh di panel trace untuk melihat log yang tersedia dan durasi
langkah yang tercatat. Log juga dapat diperiksa langsung:

```bash
python3 infra/vps/access.py kube -- -n sistem-informasi-kebencanaan-bnpb logs -l app=client-api --all-containers=true --prefix --tail=100
python3 infra/vps/access.py kube -- -n sistem-informasi-kebencanaan-bnpb logs -l app=aggregator --all-containers=true --prefix --tail=100
```

Trace explorer adalah pencarian log terbatas (15 menit/5.000 baris per pod),
bukan penyimpanan distributed tracing lengkap. Span internal sensor/broker
black box tidak dibuat-buat. Seri scrape aplikasi yang tersedia tidak berarti
metrik setiap database/vendor sudah tercakup; kelengkapan observabilitas wajib
dibuktikan per layanan saat demo. HTTP 429 dicatat terpisah dari 5xx dan tidak
dihitung sebagai throughput permintaan yang berhasil dilayani.

## Pengujian P1–P5 dan Tester Agent

Panduan setiap kasus, efek samping, parameter, dan interpretasi hasil ada di
[`test/README.md`](test/README.md). Pengujian laporan berada di `test/`,
sedangkan unit/integration test berada bersama service. Nomor problem M1 dan
M2 tidak dapat dipertukarkan.

Jalankan satu kampanye mutasi pada satu waktu; jangan menjalankan P2 penuh
bersamaan dengan P5 pada cluster yang sama. `--allow-changes` menyatakan tes
boleh melakukan mutasi yang dijelaskan dalam panduan, bukan mengizinkan
penghapusan semua data. Semua script memiliki `--help`; `--env-file <path>`
memilih kredensial deployment yang sesuai.

### P1: partisi, kueri, pemulihan, dan analitik

```bash
python3 test/p1/00-preflight.py --vps
python3 test/p1/01-sharding.py --vps --allow-changes --repeat 3 --records 300
python3 test/p1/02-queries.py --vps --allow-changes --repeat 3
python3 test/p1/03-recovery.py --vps --allow-changes --repeat 3
python3 test/p1/05-analytics.py --vps --repeat 5
```

Kasus sandbox memakai volume milik run, bukan dataset asisten. Varian
`03-recovery.py --node-fault` menginterupsi worker; kasus `04-live-recovery.py`
menghapus satu PVC kanonik **asli** yang dipilih eksplisit. Jangan menjalankan
varian destruktif tanpa memeriksa penempatan, cadangan, dan persetujuan operator.
Parameter worker/claim serta prosedur verifikasi ulang ada dalam panduan tes.

### P2: failover polling dan kapasitas

Untuk generator D, isi `load_generator` di inventory lalu provision terpisah:

```bash
python3 infra/vps/provision-load.py --apply
python3 test/tester-agent/02-polling-failover.py --vps --allow-changes --repeat 3
python3 test/p2/01-capacity.py --vps --generator-vps --allow-changes --mode throughput --vus 1000 --seconds 60 --repeat 2 --max-rss-mib 6144
python3 test/p2/01-capacity.py --vps --generator-vps --allow-changes --mode scale --vus 10000 --seconds 120 --repeat 1 --max-rss-mib 6144
```

Tanpa `--generator-vps`, k6 berjalan di laptop; pastikan k6 tersedia dan RAM
mencukupi. Catat bahwa jalur SSH/WAN dapat membatasi pengukuran. Throughput
membandingkan respons lengkap 1 versus 3 instance; scale menguji perubahan
3 → 5 → 2 selama beban. `02-network.py`, `03-profile.py`, dan
`04-generator.py` adalah diagnosis jaringan/profil/generator, bukan bukti
pengganti target throughput ≥2×. Gunakan `--help` masing-masing sebelum berjalan.
Pemicu rolling versi Aggregator tersedia di
[05-rolling-update.sh](scripts/m2/p2/05-rolling-update.sh), dengan argumen image
pengganti yang sudah tersedia pada semua node. Skrip tersebut hanya memicu
rollout; jalankan sambil beban dan pengamat independen aktif untuk membuktikan
tidak ada request gagal. Jangan menyamakan rollout selesai atau rolling
Client API dengan pembuktian P2.8.

### P3: skema, pesan rusak, replay, dan laporan durabel

```bash
python3 test/p3/00-preflight.py --vps
python3 test/p3/01-schema.py --vps --allow-changes
python3 test/p3/02-poison.py --vps --allow-changes
python3 test/p3/03-ops-access.py --vps
python3 test/p3/04-replay.py --vps --allow-changes
python3 test/p3/05-delivery.py --vps --allow-changes --count 20 --repeat 3
```

### P4: feed live dan pemulihan stream

```bash
python3 test/p4/00-preflight.py --vps
python3 test/p4/01-live-stream.py --vps --seconds 180 --interval 5
python3 test/p4/02-recovery.py --vps --allow-changes --repeat 3
python3 test/p4/03-audit.py --vps
python3 test/p4/04-dashboard.py --vps --repeat 3
```

`02-recovery` memakai sumber uji terkontrol dan oracle independen untuk
membandingkan baseline/gangguan; itu bukan hasil referensi asisten.

### P5: kesehatan, alert kritis, isolasi, dan prioritas

```bash
python3 test/p5/00-health.py --vps --repeat 3 --seconds 60
python3 test/p5/01-critical.py --vps --repeat 3 --seconds 60
python3 test/p5/02-load.py --vps --mode isolation --repeat 3
python3 test/p5/02-load.py --vps --mode priority --repeat 3
python3 test/p5/03-recovery.py --vps --allow-changes --repeat 3
python3 test/p5/04-circuit.py --vps --repeat 3 --seconds 30
python3 test/p5/05-observability.py --vps --repeat 3 --under-load
```

Kasus lanjutan `06-resilience`, `07-source-age`, `08-critical-lane`,
`09-notifier-failover`, dan `10-contract` mencakup gangguan sumber, usia sumber,
backlog, crash notifier redundan, dan pesan tidak valid. Perintah lengkap serta
kebutuhan staging `notifier-ha` untuk kasus `09` ada di panduan tes. Kasus
terisolasi tidak membuktikan bahwa produksi sudah memakai notifier redundan.
Tidak ada sampel kritis baru berarti **inconclusive**, bukan lulus p95 ≤5 detik.
Latensi dihitung dari ketersediaan di sumber sampai durabel di notifier,
bukan sekadar setelah event masuk Aggregator.

### Tester Agent resmi

`deploy.py` memasang manifest resmi tanpa mengganti image atau hak istimewanya.
Agent berjalan satu per node dalam namespace `tester-agent`. Image yang dipakai:

```text
sustuna/tester-agent@sha256:b3143da159cf23a8744ede12b847da6208353754234d53a5a7b46d071cf277ec
sustuna/sensor-pantai@sha256:51fbd90d3891ad6e9f5768ad330ffb65205644cb29e50da65c7538f2e0b4397d
```

Temukan skenario resmi dan periksa kesiapan dahulu:

```bash
python3 test/tester-agent/00-preflight.py --vps
python3 infra/vps/access.py kube -- -n tester-agent exec daemonset/tester-agent -- /app list
```

Pemicu langsung, hanya pada jendela pengujian yang disepakati:

```bash
python3 infra/vps/access.py kube -- -n tester-agent exec daemonset/tester-agent -- /app run practice <nama-skenario>
python3 infra/vps/access.py kube -- -n tester-agent logs daemonset/tester-agent --tail=200
```

CLI berhasil bukan bukti gangguan sudah terjadi atau sistem pulih. Runner berikut
menambahkan pengamatan dan bukti; pemulihan diamati tanpa perbaikan manual:

```bash
python3 test/tester-agent/01-kill-leader.py --vps --allow-changes --repeat 3
python3 test/tester-agent/03-schema.py --vps --allow-changes --repeat 3
python3 test/tester-agent/04-kill-store.py --vps --allow-changes --repeat 3
python3 test/tester-agent/05-clock-skew.py --vps --allow-changes --repeat 3
python3 test/tester-agent/06-partition.py --vps --allow-changes --repeat 3
```

Panduan tes juga memuat `07-backup`/`08-wipe-node` (destruktif; backup terverifikasi
dan operator siap menyalakan worker), `09-gateway-failover`,
`10-audit-recovery`, `11-api-rollout`, `12-fencing`, dan `13-partition-load`.
Kasus `12` membekukan proses; pembekuan/resume **seluruh VM** tetap memerlukan
pengujian terpisah. Jangan memblokir Agent melalui RBAC, NetworkPolicy, label,
resource, atau perubahan image. Jangan menjalankan `wipe-node` sebagai tes rutin.

### Bukti dan status kelulusan

Hasil tersimpan secara privat di
`.e2e/report-tests/<problem>/<case>/<run-id>/`: `result.json`, snapshot
deployment, request cURL dengan `time_total`, correlation ID, dan observasi.
Periksa status `passed`, `failed`, `inconclusive`, atau pengamatan tanpa klaim
lulus; baca seluruh checks, bukan hanya exit code atau daftar criteria.
Arsip pesan gagal/backup dapat berisi data sensitif dan harus disunting sebelum
dimasukkan ke laporan.

Pembuktian throughput ≥2×, kehilangan nol/p95 kritis ≤5 detik, kelengkapan trace,
serta gangguan gabungan harus memakai bukti kampanye yang sesuai. README ini
tidak mengubah kegagalan atau hasil tidak konklusif menjadi lulus. Dataset,
checksum referensi, kredensial Audit khusus, dan kunci exam asisten diverifikasi
terpisah saat tersedia setelah tenggat; data latihan tidak dinamai ulang sebagai
referensi resmi.

## Pemeriksaan pengembangan

```bash
go test ./services/aggregator/... ./services/auth/... ./services/bmkg/... ./services/client-api/... ./services/dashboard-consumer/... ./services/notifier-consumer/... ./services/pvmbg/...
node --test services/client-api/cmd/server/dashboard_ui.test.cjs
python3 -m unittest discover -s services/observability -p 'test_*.py'
```

Integration test PostgreSQL memerlukan `P2_TEST_DATABASE_URL` menuju database
**sementara** yang boleh membuat schema. Tanpa variabel tersebut sebagian tes
akan skip; jangan menyatakan semuanya lulus tanpa memeriksa output. Jangan
mengarahkan integration test ke database produksi/coordinator.

## Struktur repositori

```text
services/       Service Go, mock, database bootstrap, dan observabilitas
k8s/base/       Workload, penyimpanan, job, dan NetworkPolicy baseline
k8s/ha/         Konfigurasi HA tambahan; tidak otomatis aktif di baseline
scripts/        Setup, renderer manifest, dan demo/lab
infra/vps/      Provision, build, deployment, dan akses server
test/           Skenario laporan P1–P5, Tester Agent, dan harness bersama
docs/           Artefak laporan yang masih dilacak Git
.env.example    Daftar konfigurasi tanpa secret nyata
go.work         Workspace Go
```

## Menghentikan sistem

Untuk pengembangan lokal: `docker compose stop` menghentikan container;
`docker compose down` juga melepas container/network proyek dan mempertahankan
named volume. `docker compose down -v` **menghapus data volume proyek** dan
bukan langkah wajib redeploy.

Menutup tunnel hanya menghentikan akses laptop, bukan cluster VPS. Tidak ada
perintah wipe/reset VPS otomatis dalam panduan startup ini. Penghapusan PVC,
namespace, atau data node harus dilakukan hanya setelah target, dampak, dan
cadangan disepakati. Saat skenario P5 sedang mengukur pemulihan, jangan
melakukan restart atau perbaikan manual agar hasil terlihat lulus.

## Authors

<div align="center">
  <table>
    <tr>
      <th>NIM</th>
      <th>Name</th>
      <th>GitHub</th>
    </tr>
    <tr align="center">
      <td>13523090</td>
      <td>Nayaka Ghana Subrata</td>
      <td>
        <a href="https://github.com/Nayekah">
          <img src="https://github.com/Nayekah.png" width="48" alt="Nayekah" /><br/>
          <sub><b>@Nayekah</b></sub>
        </a>
      </td>
    </tr>
    <tr align="center">
      <td>13524013</td>
      <td>Anindya Naufal Pinasthika</td>
      <td>
        <a href="https://github.com/Naufal-Pinasthika/">
          <img src="https://github.com/Naufal-Pinasthika.png" width="48" alt="Naufal-Pinasthika" /><br/>
          <sub><b>@Naufal-Pinasthika</b></sub>
        </a>
      </td>
    </tr>
    <tr align="center">
      <td>13524003</td>
      <td>Faiq Azzam Nafidz</td>
      <td>
        <a href="https://github.com/Shui7/">
          <img src="https://github.com/Shui7.png" width="48" alt="Shui7" /><br/>
          <sub><b>@Shui7</b></sub>
        </a>
      </td>
    </tr>
  </table>
</div>
