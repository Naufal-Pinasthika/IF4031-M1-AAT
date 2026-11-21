# Pengujian E2E P2/P5

Current design and verification results are summarized in
[the published status](../../../docs/status/README.md). The dated local retest journal
is ignored by Git. The table below retains historical, not current, results.

Lab ini khusus pengujian lokal, bukan pengganti seluruh bukti demo. Status
**lulus**, **gagal**, dan **belum diuji** harus dibedakan. Bukti mentah berada
di `.e2e/`, yang sengaja diabaikan Git karena juga memuat kubeconfig.

## Topologi yang benar-benar dijalankan

Pengujian tanggal 18 November 2026 memakai satu host Windows dengan RAM 16 GB
dan 12 logical CPU. Distro WSL `AAT-M2-E2E` serta disk lab berada di drive D:.
WSL hanya menjadi host QEMU dan alat pengujian; aplikasi BNPB berjalan di k3s.

| Node QEMU | Peran | vCPU | RAM | Alamat |
|---|---|---:|---:|---|
| `aat-node-0` | Control plane dan worker | 4 | 4.096 MiB | `192.168.93.10` |
| `aat-node-1` | Worker | 2 | 1.280 MiB | `192.168.93.11` |
| `aat-node-2` | Worker | 2 | 1.280 MiB | `192.168.93.12` |

Tabel menunjukkan alokasi lab terbaru. Hasil historis di bawah dan hasil beban
sebelum perubahan topologi memakai control plane 2 vCPU. Alokasi dinaikkan
menjadi 4 vCPU setelah tekanan CPU tetap tinggi ketika tidak ada generator.
Hasil lama tidak dianggap validasi kapasitas topologi baru. `lab.sh up`
memakai 4 vCPU secara bawaan; `AAT_LAB_CONTROL_PLANE_CPUS` dapat diatur untuk
perbandingan terpisah. VM yang sudah berjalan tidak diubah oleh perintah `up`.

Ketiga VM menjalankan kernel Linux sendiri, dengan boot ID berbeda. k3s yang
terpasang saat pengujian adalah `v1.37.2+k3s1`. Ini bukan kind/k3d dengan kernel
bersama. Semua VM berada pada host fisik yang sama; bonus lintas host tidak
diklaim. Disk PVC bersifat lokal pada masing-masing VM.

## Hasil dan batas pembuktian

| Pemeriksaan | Hasil yang terukur | Batas klaim |
|---|---|---|
| Regresi kode terakhir | Seluruh `go test ./...` pada tujuh layanan dan 11 tes PostgreSQL lulus | PostgreSQL dijalankan terisolasi setelah VM berhenti; bukan bukti E2E penuh |
| Instalasi/deployment | Tiga node `Ready`; seluruh workload berhasil dijalankan | Ada retry/restart saat startup pada host yang sibuk |
| Batas akses storage | Pemilik diizinkan; Client API/mock tidak dapat mengakses DB Aggregator; Aggregator tidak dapat mengakses DB Auth | Probe dari identitas pod nyata, bukan hanya pemeriksaan YAML |
| Pemimpin BMKG dimatikan | Pengambilalihan 15,99 detik | Satu skenario penghapusan pod |
| VM pemilik BMKG dibekukan | Pengambilalihan 31,23 detik; stale admission = 0 | Worker VM 2; intent lama yang telah sah tetap boleh diputar ulang |
| HTTP/autentikasi/aliran data | Ketiga sumber masuk kanonik, identitas unik, hak Ops ditegakkan, notifikasi diterima | `smoke-results.json`; bukan seluruh profil ujian |
| Alert kritis setelah pengulangan bersih | Tiga alert kanonik diterima notifier; p95 admission-ke-notifier 1.826 ms | Sampel kecil; **bukan** p95 penuh sejak alert tersedia di sumber |
| Prioritas awal, cadangan 16 slot | Gagal: 15 dari 876 request Field menerima 429 | Bukti disimpan di `priority-with-16-slots/` |
| Cadangan 32 slot, uji awal | Field 0 error, tetapi 14 arrival tidak dikirim generator | Belum lulus penuh; tidak boleh menyamakan dropped arrival dengan request sukses |
| Gangguan gabungan dan prioritas dengan 50 VU Field siap pakai | Gagal: 65 dari 476 request Field gagal; 425 arrival terlewat | Beban dan gangguan bersamaan; `priority-summary.json` dipertahankan sebagai kegagalan |
| Circuit breaker/gangguan gabungan | Harness tersedia; hasil awal belum lulus | Transport `kubectl exec`, koneksi lama, dan TTL token pada harness diperbaiki; perlu hasil akhir yang lulus |
| 10.000 VU selama 120 detik, skala 3→5→2 | **Belum diuji** | C: tersisa sekitar 1,1 GB; tidak aman memaksa alokasi besar |
| Throughput sukses tiga instance ≥ 2× satu instance | **Belum dibuktikan** | Gunakan jumlah HTTP 200, bukan seluruh request termasuk 429 |
| Restart bergulir, 10 VU/60 detik | 3.882 respons HTTP 200, tetapi semuanya `partial:true`: **gagal** | HTTP 200 saja tidak membuktikan data lengkap; upgrade image/10.000 VU juga belum terbukti |
| Profil ujian/game day final dan dashboard lintas layanan | **Belum selesai** | Tidak ada kunci profil ujian; dashboard/metrik seluruh layanan masih perlu divalidasi |

Hasil lama tidak dihapus untuk membuat angka tampak lulus. Reset fixture hanya
menghapus data sintetis milik lab ini; snapshot hasil sebelumnya disimpan di
`.e2e/before-reset-*/`. Data fixture yang dihapus tidak merupakan backup database.
Tester Agent, RBAC, namespace, dan kredensial tidak dihapus saat reset.

### Perbaikan yang ditemukan lewat pengujian

1. Jam Sensor Pantai ternyata merupakan jam simulasi. Freshness sekarang memakai
   watermark persisten serta durasi stagnasi berdasarkan jam PostgreSQL. Offset
   awal yang tidak diketahui tidak diperlakukan sebagai umur data beberapa hari.
2. Field wajib yang hilang/null tidak boleh berubah diam-diam menjadi angka nol
   atau `critical=false`.
3. Halaman sensor diterima melalui transaksi batch berpagar lease. Event kritis
   didahulukan dalam antrean pengiriman, tanpa melewati versi lama untuk identitas
   yang sama. Cursor tetap maju setelah seluruh halaman diterima secara tahan
   crash.
4. Penambahan Aggregator tidak lagi menarik storage sehat ke status `recovering`.
   Pemulihan nonempty hanya dikerjakan worker dengan lease `P1_RECOVERY`.
   Probe skema juga menghindari DDL berulang pada skema yang sudah lengkap:
   `ALTER TABLE ... IF NOT EXISTS` tetap mengambil lock eksklusif dan dapat
   menghambat pembaca. Perbaikan terakhir ini memerlukan validasi runtime ulang.
5. Sensor baru dimulai setelah penerima notifikasi siap pada deployment awal.
6. Cadangan Field dinaikkan dari 16 menjadi 32 slot setelah penolakan Field
   benar-benar muncul pada uji beban. Konsekuensinya, kapasitas bersama Media
   berkurang dari 48 menjadi 32 slot.

Regresi PostgreSQL terakhir juga menahan read lock saat instance baru mulai,
sehingga migrasi DDL yang tidak perlu tidak dapat lolos tanpa terdeteksi.
Tag build lab berikutnya dibuat unik dan dicatat di `.e2e/image-tag`; jangan
mengandalkan retag image yang sudah di-cache saat memvalidasi kode terbaru.

## Menjalankan ulang

Prasyarat di WSL: QEMU/KVM, `qemu-utils`, `cloud-image-utils`, OpenSSH client,
Docker dengan BuildKit, Python 3, curl, jq, dan iptables. Gunakan distro pada
disk yang cukup longgar. `/dev/kvm` harus tersedia; tidak ada fallback ke node
container untuk pembuktian kernel terpisah.

Jalankan sebagai root dalam distro lab. `lab.sh` hanya memakai bridge
`aatbr0`, tap `aattap0..2`, dan disk di `/var/lib/aat-m2-e2e`.

```bash
mkdir -p /var/lib/aat-m2-e2e
curl -fL https://cloud-images.ubuntu.com/jammy/current/jammy-server-cloudimg-amd64.img \
  -o /var/lib/aat-m2-e2e/jammy.img
curl -fL https://cloud-images.ubuntu.com/jammy/current/SHA256SUMS \
  -o /var/lib/aat-m2-e2e/SHA256SUMS

# Dari root repository, terminal WSL pertama:
bash scripts/m2/e2e/lab.sh keepalive

# Terminal WSL kedua; pastikan Docker daemon berjalan:
bash scripts/m2/e2e/lab.sh up
bash scripts/m2/e2e/build.sh
bash scripts/m2/e2e/deploy.sh
python3 scripts/m2/e2e/smoke.py
```

Image Ubuntu diperiksa terhadap SHA-256 sebelum boot. Image Sensor Pantai dan
Tester Agent tetap memakai digest resmi, tanpa modifikasi. Sebelum menguji
ulang dari kondisi bersih, **hanya jika seluruh data cluster ini disposable**:

```bash
python3 scripts/m2/e2e/reset-fixtures.py --confirm-disposable-lab-data
```

Skrip reset menolak endpoint, nama node, atau daftar resource yang berbeda.
Jangan dipakai pada cluster bersama atau cluster yang menyimpan data penting.

### Jalankan kasus satu per satu

```bash
python3 scripts/m2/e2e/resilience.py network-policy
python3 scripts/m2/e2e/resilience.py failover
python3 scripts/m2/e2e/resilience.py freeze
python3 scripts/m2/e2e/resilience.py sensor-outage
python3 scripts/m2/e2e/coastal-window.py
```

Kasus `sensor-outage` menggabungkan pemutusan akses ke sensor dan penghentian
pod pemilik untuk menutup koneksi TCP lama. Akses Tester Agent tetap diizinkan.
Fault dihapus otomatis pada akhir jendela; jangan menjalankan rollout/config
update bersamaan dengan kasus ini kecuali memang menguji gangguan gabungan.

`coastal-window.py` membaca black box resmi dan membandingkan ID kritis valid
dengan notifier. Untuk alert baru, batas latensi berasal dari waktu polling
sebelumnya pada jam host, bukan `observed_at` simulasi. Jika tidak ada alert
baru, atau polling terputus, angka ini tidak boleh dianggap pembuktian penuh
SLO sumber-ke-notifier. Sampel historis tetap dihitung untuk rekonsiliasi ID.

Untuk k6 Windows, jalankan relay loopback dalam WSL pada terminal tersendiri:

```bash
python3 scripts/m2/e2e/forward.py
```

Lalu, dari PowerShell pada root repository:

```powershell
./scripts/m2/e2e/run-load.ps1 -Case priority -K6Path D:\lokasi\k6.exe
./scripts/m2/e2e/run-load.ps1 -Case rolling -K6Path D:\lokasi\k6.exe
./scripts/m2/e2e/run-load.ps1 -Case throughput -K6Path D:\lokasi\k6.exe
```

Konfigurasi lab dibatasi: prioritas memakai 50 VU Media, Field 20 request/detik,
dan 45 detik; restart bergulir memakai 10 VU; perbandingan throughput memakai
50 VU selama 30 detik per konfigurasi. Ini **tidak menggantikan** konfigurasi
wajib 10.000 VU/120 detik di `scripts/m2/p2/load.js`. Relay TCP juga dapat
menjadi bottleneck; benchmark final sebaiknya memiliki jalur langsung ke Service.

Autentikasi awal k6 dilakukan pada `setup()` agar satu principal tidak membuat
puluhan sesi hanya karena ada banyak VU. Uji refresh/Auth storm merupakan uji
berbeda. `field_failures` harus dibaca bersama `dropped_iterations`: request yang
tidak berhasil dikirim load generator bukan request Field yang berhasil dilayani.

### Mengakhiri lab

```bash
bash scripts/m2/e2e/lab.sh down
```

Perintah ini mematikan tiga VM secara tertib, bukan menghapus disk. Distro WSL
lain dan container/proyek lain tidak dibersihkan. Setelah semua VM mati dan
relay/keepalive ditutup, distro khusus ini dapat dihentikan dari PowerShell:

```powershell
wsl --terminate AAT-M2-E2E
```
