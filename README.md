# Spectrum7 SCADA Availability
Aplikasi yang digunakan untuk penghitungan availability SCADA pada bagian Fasilitas Operasi PLN UP2B Sistem Makassar

>Python3.9+, 
>pandas, 
>Xlsxwriter


## Feature
  1. Read dari file
     - [x] Memuat data historical SCADA dari file
     - [x] Ekstensi yang disupport xls, xlsx, dan xml
  1. Read dari Offline Server
     - [x] Memuat data historical SCADA dari database Offline
     - [x] Sinkronisasi data poin SCADA untuk standarisasi penamaan
  1. Perhitungan RC
     - [x] Perhitungan **SUKSES / GAGAL** RC
     - [x] Perhitungan repetisi kontrol
     - [x] Analisa & hipotesis awal gagal kontrol
     - [x] Rekomendasi tagging untuk bay berdasarkan kontribusi pengurangan Sukses RC
     - [x] _Single-run-only_, cukup running program sekali, file output sudah dapat otomatis menghitung perubahan dari user
     - [x] Menggabungkan dan merekap beberapa output file
     - [x] Support file historical Master Survalent (baru)
  1. Availability RTU
     - [x] Memilah dan menghitung downtime RTU
     - [x] Perhitungan **Availability Link** (baru)
     - [x] ~~Tabel maintenance untuk menganulir event down yang disebabkan selain dari permasalahan peralatan (ex. down Pemeliharaan atau shutdown oleh user)~~ (dihapus)
     - [x] Klasifikasi downtime berdasarkan waktu untuk memudahkan filter dan analisa gangguan
     - [x] _Single-run-only_, cukup running program sekali, file output sudah dapat otomatis menghitung perubahan dari user
     - [x] Menggabungkan dan merekap beberapa output file

#### Update 31-03-2024
Pengembangan support untuk perhitungan RC dari file historical Master Survalent.

#### Update 30-04-2024
Peningkatan performa dalam durasi membuka file maupun proses perhitungan hingga **4x** lebih cepat dari versi sebelumnya.

#### Update 31-05-2024 (Terbaru)
- Pengembangan struktur perhitungan availability RTU
- Penambahan perhitungan availability link komunikasi
- Peningkatan dokumentasi aplikasi 


## Contributor
Putu Agus [(@antara-adiputra)](https://github.com/antara-adiputra/)
