# Project Watchtower

Dokumen ini berisi langkah-langkah untuk menjalankan Project Watchtower dari awal. Sistem ini adalah pipeline data real-time untuk Network Operations Center (NOC) yang dibangun menggunakan Go. Panduan ini dibuat untuk memverifikasi semua fitur tanpa asumsi.

## Prerequisite

Mesin harus memiliki software berikut:
1. Golang (Versi 1.20 atau lebih baru).
2. Docker dan Docker Compose (Untuk menjalankan server MinIO lokal).

## Langkah 1: Persiapan Infrastruktur

MinIO diperlukan sebagai object storage. Nyalakan di latar belakang.

```bash
docker compose up -d
```

Output: Container MinIO berjalan. Web console dapat diakses di `http://localhost:9001` dengan kredensial default `watchtower_admin` / `watchtower_password`.

## Langkah 2: Menjalankan Aplikasi

Jalankan pipeline utama Watchtower.

```bash
go run ./...
```

Output di terminal:
```text
Kebijakan berhasil diperbarui dari MinIO!
Policy berhasil disinkronisasi ke MinIO.
Berhasil memuat X event IDs dari dedup window
Berhasil memuat Y event IDs yang masih valid (TTL: 60s)
Memulai Project Watchtower: Ingestion & Screening Pipeline!
[Worker 1] Aktif.
[Worker 2] Aktif.
[Worker 3] Aktif.
[Worker 4] Aktif.
Watchtower SSE Server listening on http://localhost:8080/stream
```
Angka X dan Y dapat berbeda tergantung sisa state di MinIO.

## Langkah 3: Verifikasi Dashboard

Akses dashboard di browser melalui `http://localhost:8080`.

Struktur Dashboard:
1. Event Rate: 4 kartu sumber (Dynatrace, Prometheus, Splunk, Riverbed) dengan angka events/second yang berubah.
2. Event Feed: Daftar 50 event terakhir. Badge P-Tier terletak di sebelah kiri setiap event:
   - P1 (Merah) untuk event kritis.
   - P2 (Oranye) untuk warning.
   - P3 (Kuning) untuk info.
   - P4 (Biru) untuk event normal.
3. Anomaly Alerts & Forecasts: Panel akan terisi otomatis saat ML mendeteksi anomali atau memprediksi tren.
4. Drop Counter: Angka di pojok kanan atas yang mencatat event yang dibuang karena buffer penuh.

## Langkah 4: Menguji Hot-Reload Policy

Fitur ini mengubah aturan screening tanpa mematikan server.

1. Buka file `policy/screening.json`.
2. Ubah `"drop_keywords"` dari `["DEBUG"]` menjadi `["INFO"]`. Simpan file.
3. Log terminal akan menampilkan dalam waktu sekitar 2 detik:
   ```text
   Perubahan policy terdeteksi di MinIO (ETag berubah). Memuat ulang...
   Kebijakan berhasil diperbarui dari MinIO!
   ```
4. Event dengan keyword INFO akan berhenti muncul di Event Feed.

## Langkah 5: Menguji Graceful Shutdown

Memastikan aplikasi mati dengan rapi dan menyimpan state terakhir.

1. Tekan `Ctrl + C` di terminal tempat aplikasi berjalan.
2. Output yang diharapkan:
   ```text
   Received shutdown signal, initiating graceful shutdown...
   Final dedup state berhasil disimpan.
   Dashboard snapshot berhasil disimpan ke /state/dashboard.json
   All workers finished gracefully.
   Watchtower shutdown complete.
   ```
3. Buka MinIO UI (`localhost:9001`), cek bucket `watchtower`, dan pastikan terdapat file di folder `/state/dashboard.json`.

## Langkah 6: Menjalankan Test Suite

Memastikan kode aman dari data race. Jalankan di terminal baru.

```bash
go test -race ./...
```

Output yang diharapkan:
```text
ok      watchtower/ml       (cached)
ok      watchtower/policy   (waktu eksekusi)
ok      watchtower/screening(waktu eksekusi)
?       watchtower/sse      [no test files]
```
Pastikan tidak ada kata FAIL di output.

## Struktur Direktori

| Path | Deskripsi |
| --- | --- |
| `main.go` | Titik masuk aplikasi, perangkaian channel, dan HTTP server. |
| `config/` | Modul parsing `config.json`. |
| `mocks/` | Generator data simulasi (Dynatrace, Prometheus, dll). |
| `models/` | Definisi struct data (EventEnvelope, AnomalyResult, dll). |
| `policy/` | Engine screening dan hot-reload berbasis ETag MinIO. |
| `screening/` | Logika Worker Pool, deduplikasi, dan signature noise filter. |
| `storage/` | Integrasi MinIO (Arsip, State, Policy). |
| `ml/` | Algoritma EMA Anomaly Detection & Linear Regression Forecast. |
| `sse/` | Server-Sent Events Hub untuk streaming real-time ke dashboard. |
| `state/` | In-memory state untuk dashboard (dengan thread-safe mutex). |

Untuk penjelasan mendalam mengenai arsitektur dan evolusi keputusan desain, baca [Dokumen Desain Sistem (DESIGN.md)](./DESIGN.md).
