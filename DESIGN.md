# Project Watchtower - Architecture & Design Document (Day 1)

## 1. Data Structures: EventEnvelope
Semua data yang dihasilkan oleh empat mock source (Dynatrace, Splunk, Riverbed, dan Prometheus) akan dibungkus dalam struktur standar `EventEnvelope` sebelum dikirim ke channel ingestion. Hal ini memastikan pipeline pemrosesan yang seragam.

Struktur ini didefinisikan dengan tipe data ketat sebagai berikut:
* **Version**: Field berjenis string yang wajib diisi untuk mencatat versi data yang masuk.
* **ID**: Identifier unik berjenis string yang bertindak sebagai UUID.
* **Source**: Field string untuk menandai sumber asal data (misalnya: "dynatrace" atau "splunk").
* **Timestamp**: Waktu kejadian log yang dicatat dalam format integer 64-bit sebagai Unix timestamp.
* **Payload**: Payload dinamis yang disesuaikan dengan sumber aslinya, menggunakan `map[string]interface{}` (setara dengan struktur JSON / Dictionary campuran di Python).

## 2. Object Storage Key Schema
Karena penggunaan database tidak diperbolehkan, seluruh state dan data akan disimpan secara *flat file* ke MinIO menggunakan skema path. Konsep path folder `YYYY/MM/DD` ini bertindak sebagai skema partisi waktu (mirip dengan partisi data di BigQuery) untuk efisiensi pembacaan data.

* **Raw Events**: Data log mentah yang baru masuk akan diarsipkan secara asinkron di `/events/raw/YYYY/MM/DD/HH/<uuid>.json`.
* **Screened Events**: Data bersih setelah melewati deduplikasi dan filter noise disimpan di `/events/screened/YYYY/MM/DD/HH/<uuid>.json`.
* **Anomaly Outputs**: Keluaran hasil deteksi model Machine Learning terkait anomali disimpan di `/ml/anomalies/YYYY/MM/DD/<uuid>.json`.
* **Forecasts**: Hasil dari algoritma prediksi *trend* metrik akan ditimpa (overwritten) setiap kali ada prediksi baru, dan disimpan di `/ml/forecasts/YYYY/MM/DD/<metric>.json`.
* **State & Policy**: State operasional sistem seperti aturan aktif disematkan di `/policy/screening.json`, snapshot dashboard di `/state/dashboard.json`, dan data riwayat deduplikasi di `/dedup/window/<bucket>.json`.

## 3. Channel Buffer Strategy & Backpressure
Mekanisme aliran data (Ingestion) dari sumber sistem menggunakan antrean tersentralisasi untuk mencegah sistem *crash* akibat lonjakan data mendadak.

* **Buffer Size**: Sistem menggunakan satu buffered channel secara terpusat dengan ukuran antrean sebesar `2048`, angka ini diambil secara dinamis dari nilai `ingestion.channel_buffer_size`.
* **Backpressure Drop**: Bila MinIO atau CPU lambat yang menyebabkan channel penuh, baris kode akan menggunakan operator `select` dengan opsi `default` untuk membuang paket log (dropped) dengan sengaja.
* **Monitoring Drop**: Setiap peristiwa pembuangan paket ini harus dicatat ke dalam log terminal, menambahkan matriks metrik pembuangan (drop counter), dan wajib diekspos datanya ke web dashboard UI. Interval log pembuangan ini dieksekusi secara agregat setiap `5000` milidetik (`drop_log_interval_ms`).

## 4. Goroutine Ownership Map
Untuk mematuhi aturan tidak ada bentrokan memori (sebagai lulus kriteria `go test -race`), batas tugas setiap concurrent goroutine dipetakan secara absolut:

* **Producers (4 Goroutines)**: Terdiri dari empat goroutine independen yang memproduksi mock data dari masing-masing sistem dan menjadi satu-satunya penulis (*writer*) mutlak ke ingestion channel.
* **Consumers / Worker Pool**: Sistem menyiapkan `4` buah goroutine worker paralel (`screening.worker_count`) yang berjalan bersamaan. Mereka membaca data dari channel lalu mengeksekusi pipeline screening seperti *deduplication* dan *noise filtering*.
* **Storage Archiver (1 Goroutine)**: Satu buah goroutine terpisah yang mendengarkan *secondary channel* agar latensi penulisan HTTP/Network ke *storage* MinIO tidak memblokir antrean ingestion utama.
* **Policy Poller (1 Goroutine)**: Satu goroutine yang terus-menerus mengecek status `screening.json` via ETag untuk keperluan *hot-reload* pengaturan aturan *screening* tanpa menghentikan pipeline.

## 5. Graceful Shutdown Sequence
Aplikasi Golang tidak boleh berhenti secara kasar saat dimatikan. Sistem diatur untuk selesai menutup dirinya (*clean exit*) dalam tenggat waktu maksimum `5` detik (`server.shutdown_timeout_seconds`). 

Tahapan *Graceful Shutdown* yang dieksekusi:
1.  **Signal Capture**: Menangkap peringatan interupsi sistem operasi dengan memantau `SIGINT` dan `SIGTERM`.
2.  **Context Cancellation**: Menggunakan perintah `context.CancelFunc` yang dikirim ke semua produsen untuk menghentikan generator data mock secara bersamaan.
3.  **Channel Closure**: Menjalankan fungsi `close(ingestionChannel)` untuk mengunci pintu masuk channel.
4.  **Drain & Process**: Memerintahkan modul `sync.WaitGroup` untuk menahan program tetap hidup sambil menunggu kumpulan consumer menyelesaikan pemrosesan log yang masih terjebak di dalam antrean channel.
5.  **State Snapshot**: Meminta sistem menyimpan seluruh memori sementara (state deduplikasi dan dashboard) menjadi file flat-JSON ke dalam MinIO sebelum keluar ke OS.