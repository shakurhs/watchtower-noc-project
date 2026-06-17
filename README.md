# Project Watchtower

**Project Watchtower** adalah sebuah sistem *pipeline* data berbasis **Go (Golang)** yang dirancang untuk mensimulasikan pemrosesan data *backend* untuk Network Operations Center (NOC). Sistem ini menerima, memfilter, dan menyimpan log atau metrik dari berbagai sumber secara *real-time* dengan konkurensi tinggi.

## Fitur Utama

- **High-Concurrency Ingestion:** Menerima data secara simultan dari berbagai produsen data (Dynatrace, Prometheus, Splunk, Riverbed) menggunakan Goroutines.
- **Screening Pipeline (Worker Pool):** Mencegah *bottleneck* dengan menggunakan *worker pool* paralel untuk memfilter data *noise* dan melakukan deduplikasi (menggunakan `sync.Map`) sebelum data disimpan.
- **S3-Compatible Storage:** Menyimpan data yang sudah divalidasi ke dalam *bucket* **MinIO** dengan format JSON yang terstruktur.
- **Graceful Shutdown:** Memastikan tidak ada antrean data yang hilang atau korup saat aplikasi dihentikan secara sengaja.

## Prasyarat (Prerequisites)

Sebelum menjalankan proyek ini, pastikan sistem Anda telah memiliki perangkat lunak berikut:

- **Go** (Versi 1.20 atau lebih baru)
- **Docker** dan **Docker Compose** (Digunakan untuk menjalankan server MinIO lokal)


## Cara Menjalankan Aplikasi

**1. Persiapkan Infrastruktur (MinIO):**

Nyalakan container MinIO di latar belakang menggunakan Docker Compose.

```bash
docker compose up -d
```

**2. Jalankan Aplikasi Pipeline:**

Aplikasi Go akan membaca konfigurasi, memastikan *bucket* tersedia, dan mulai memproduksi serta menyaring data.

```bash
go run main.go
```

Untuk menghentikan aplikasi dengan aman, tekan `Ctrl + C` di terminal.

**3. Melihat Hasil Penyimpanan Data:**

Buka browser dan akses Web Console MinIO di `http://localhost:9001`, lalu masuk menggunakan kredensial yang telah Anda definisikan di file `.env`.

Masuk ke menu **Buckets** -> **watchtower** -> **Object Browser** untuk melihat file JSON yang berhasil diarsipkan.

## Struktur Direktori

| Path | Deskripsi |
|---|---|
| `main.go` | Titik masuk (*entry point*) aplikasi dan perangkaian channel. |
| `config/` | Modul untuk membaca `config.json`. |
| `mocks/` | Generator lalu lintas data (simulasi sumber NOC). |
| `models/` | Definisi struktur data (Structs). |
| `screening/` | Logika Worker Pool, deduplikasi, dan *noise filtering*. |
| `storage/` | Integrasi dan koneksi dengan penyimpanan MinIO. |

## Dokumentasi Arsitektur

Untuk melihat penjelasan mendalam mengenai arsitektur sistem, aliran data (**Ingestion** -> **Screening** -> **Storage**), manajemen Goroutine, dan keputusan desain teknis lainnya, silakan baca [Dokumen Desain Sistem (DESIGN.md)](./DESIGN.md).
