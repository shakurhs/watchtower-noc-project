# Project Watchtower - Architecture & Design Document

## 1. Data Structures: EventEnvelope
Semua data yang dihasilkan oleh empat mock source (Dynatrace, Splunk, Riverbed, dan Prometheus) dibungkus dalam struktur standar `EventEnvelope` sebelum dikirim ke channel ingestion. Hal ini memastikan pipeline pemrosesan yang seragam.
Struktur ini didefinisikan dengan tipe data ketat sebagai berikut:
- **Version**: Field berjenis string untuk mencatat versi data.
- **ID**: Identifier unik berjenis string yang bertindak sebagai UUID.
- **Source**: Field string untuk menandai sumber asal data.
- **Timestamp**: Waktu kejadian log dalam format integer 64-bit (Unix timestamp).
- **Payload**: Payload dinamis menggunakan `map[string]interface{}`.
- **Priority**: Field string untuk klasifikasi severity (P1, P2, P3, P4).

## 2. Object Storage Key Schema
Seluruh state dan data disimpan secara flat file ke MinIO menggunakan skema path. Konsep path folder `YYYY/MM/DD` bertindak sebagai skema partisi waktu untuk efisiensi pembacaan data.
- **Raw Events**: Data log mentah diarsipkan di `/events/raw/YYYY/MM/DD/HH/<uuid>.json`.
- **Screened Events**: Data bersih setelah screening disimpan di `/events/screened/YYYY/MM/DD/HH/<uuid>.json`.
- **Anomaly Outputs**: Keluaran deteksi ML disimpan di `/ml/anomalies/YYYY/MM/DD/<uuid>.json`.
- **Forecasts**: Hasil prediksi trend disimpan di `/ml/forecasts/YYYY/MM/DD/<metric>.json`.
- **State & Policy**: State operasional disimpan di `/policy/screening.json`, snapshot dashboard di `/state/dashboard.json`, dan data deduplikasi di `/dedup/window/<bucket>.json`.

## 3. Channel Buffer Strategy & Backpressure
Mekanisme aliran data menggunakan antrean tersentralisasi untuk mencegah sistem crash akibat lonjakan data.
- **Buffer Size**: Menggunakan satu buffered channel dengan ukuran antrean 2048, diambil dari nilai `ingestion.channel_buffer_size`.
- **Backpressure Drop**: Bila channel penuh, sistem menggunakan operator `select` dengan opsi `default` untuk membuang paket log.
- **Monitoring Drop**: Peristiwa pembuangan dicatat ke log terminal dan diekspos ke web dashboard UI.

## 4. Goroutine Ownership Map
Batas tugas setiap concurrent goroutine dipetakan secara absolut untuk mematuhi aturan bebas race condition:
- **Producers (4 Goroutines)**: Memproduksi mock data dan menjadi satu-satunya penulis ke ingestion channel.
- **Consumers / Worker Pool**: 4 goroutine worker paralel yang membaca data dan mengeksekusi pipeline screening.
- **Storage Archiver (2 Goroutines)**: Satu untuk raw events, satu untuk screened events, mendengarkan secondary channel.
- **Policy Poller (1 Goroutine)**: Mengecek status policy di MinIO via ETag untuk hot-reload.
- **ML & SSE Goroutines**: Memproses deteksi anomali, forecasting, dan broadcasting ke dashboard.

## 5. Graceful Shutdown Sequence
Aplikasi diatur untuk selesai menutup dirinya dalam tenggat waktu maksimum 5 detik.
- **Signal Capture**: Menangkap `SIGINT` dan `SIGTERM`.
- **Context Cancellation**: Mengirim `context.CancelFunc` ke semua produsen.
- **Channel Closure**: Menjalankan `close()` pada channel ingestion.
- **Drain & Process**: Menggunakan `sync.WaitGroup` untuk menunggu consumer menyelesaikan pemrosesan.
- **State Snapshot**: Menyimpan state deduplikasi dan dashboard ke MinIO sebelum keluar.

## 6. Worker Pool Architecture (Screening Layer)
Sistem mengimplementasikan pola arsitektur Worker Pool pada tahap pembersihan data. Sejumlah goroutine diluncurkan secara paralel untuk memvalidasi dan menyaring paket log secara independen dari antrean utama.

## 7. Stateful Deduplication (Concurrent Map & TTL)
Sistem mencegah data ganda dengan melacak `ID` dari setiap log.
- **Thread-Safe Storage**: Menggunakan `sync.Map` untuk operasi Read/Write atomik.
- **Time-To-Live (TTL) Eviction**: ID terikat pada batas waktu kedaluwarsa berdasarkan `screening.dedup_ttl_seconds`. Jika log masuk setelah TTL terlewati, stempel waktu diperbarui.

## 8. Mekanisme Filter Noise & Validasi
Setiap paket data dievaluasi melalui serangkaian aturan bisnis.
- **Staleness Check**: Menghitung selisih waktu antara waktu saat ini dengan `Timestamp`. Jika melebihi `noise_window_seconds`, data dibuang.
- **Signature Sliding Window**: Membuat hash SHA256 dari payload. Jika hash yang sama muncul melebihi `signature_threshold` dalam window waktu, event dianggap noise dan dibuang.
- **Severity Filtering**: Membuang data berdasarkan keyword yang didefinisikan dalam policy.

## 9. Two-Tier Channel Orchestration
- **Ingestion Channel**: Buffer primer yang menerima data kotor dari produsen.
- **Screened Channel**: Buffer sekunder yang menerima data valid setelah screening.

## 10. Automated Infrastructure Provisioning
Sistem memiliki pengecekan absolut (`EnsureBucketExists`) yang dieksekusi sebelum pipeline berjalan. Jika bucket target belum eksis di MinIO, sistem akan membuatnya secara otomatis.

## 11. Machine Learning & Predictions
- **Anomaly Detection**: Menggunakan Exponential Moving Average (EMA) untuk menghitung baseline metrik. Deviasi yang melebihi ambang batas sigma (`anomaly_sigma_threshold`) memicu alert anomali.
- **Forecasting**: Menggunakan Linear Regression pada window data terakhir (`regression_window_size`) untuk memprediksi nilai metrik di masa depan (`forecast_horizon_minutes`).

## 12. Server-Sent Events (SSE) & Dashboard
- **SSE Hub**: Menggunakan pola Hub-and-Spoke untuk mendistribusikan event, anomaly, dan forecast ke semua client yang terhubung via `/stream`.
- **Dashboard UI**: Dibangun dengan HTML/CSS/JS murni tanpa framework. Menggunakan desain monochrome dengan aksen warna hanya untuk indikator status.
- **P-Tier Classification**: Event diklasifikasikan secara dinamis menjadi P1 (Kritis), P2 (Warning), P3 (Info), dan P4 (Normal) berdasarkan keyword payload dan sumber data.

## 13. Screened Event Archiving
Event yang lolos screening tidak hanya dikirim ke dashboard dan ML, tetapi juga diarsipkan secara asinkron ke path `/events/screened/` di MinIO untuk keperluan audit trail.

## 14. Apa yang salah (Evolusi Keputusan Desain)

Bagian ini mendokumentasikan keputusan desain yang berubah selama proses pengembangan beserta alasannya.

**Keputusan 1: Mekanisme Hot-Reload Policy**
- **Desain Awal**: Policy engine membaca file `screening.json` dari sistem file lokal dan melakukan polling menggunakan `os.Stat` untuk mengecek `ModTime` (waktu modifikasi file).
- **Perubahan**: Mekanisme diubah total untuk membaca policy dari object storage MinIO dan melakukan polling menggunakan `StatObject` untuk mengecek `ETag`.
- **Alasan**: Brief proyek secara eksplisit melarang penggunaan state lokal ("Penyimpanan hanya menggunakan object storage"). Polling file lokal melanggar constraint ini dan akan gagal bekerja jika aplikasi di-deploy di lingkungan terdistribusi atau containerized di mana file lokal tidak tersinkronisasi antar instance. ETag polling di MinIO lebih andal dan sesuai dengan arsitektur stateless.

**Keputusan 2: Logika Filter Noise**
- **Desain Awal**: Filter noise hanya mengandalkan pengecekan timestamp usang (staleness check) dan pembuangan keyword sederhana.
- **Perubahan**: Ditambahkan mekanisme Signature Sliding Window. Payload di-hash menggunakan SHA256, dan frekuensi hash yang sama dilacak dalam window waktu tertentu.
- **Alasan**: Pengecekan timestamp saja gagal menangkap "stuck metrics", yaitu kondisi di mana sumber data terus-menerus mengirimkan nilai metrik yang persis sama (misalnya status "OK" yang tidak berubah) dengan ID yang berbeda. Signature hashing secara efektif menekan redundansi payload yang tidak memberikan nilai analitik baru.

**Keputusan 3: Klasifikasi Prioritas Event (P-Tier)**
- **Desain Awal**: Semua event yang lolos screening ditampilkan secara seragam di dashboard tanpa perbedaan visual tingkat urgensi.
- **Perubahan**: Menambahkan field `Priority` pada `EventEnvelope` dan mengimplementasikan engine klasifikasi yang memetakan keyword payload dan sumber data ke tingkat P1 hingga P4.
- **Alasan**: Dashboard NOC (Network Operations Center) membutuhkan triase visual yang cepat. Menampilkan semua event dengan format yang sama menyebabkan alert fatigue dan menyulitkan operator untuk mengidentifikasi insiden kritis secara instan. Badge berwarna pada P-tier memberikan konteks urgensi secara langsung.