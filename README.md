## Spectrum7 SCADA Availability
Aplikasi yang digunakan untuk penghitungan availability SCADA pada bagian Fasilitas Operasi PLN UP2B Sistem Makassar

>Python3.9+, 
>pandas, 
>Xlsxwriter


### Feature
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
  1. Availability RTU
     - [x] Memilah dan menghitung downtime RTU
     - [x] Tabel maintenance untuk menganulir event down yang disebabkan selain dari permasalahan peralatan (ex. down Pemeliharaan atau shutdown oleh user)
     - [x] _Single-run-only_, cukup running program sekali, file output sudah dapat otomatis menghitung perubahan dari user
     - [x] Menggabungkan dan merekap beberapa output file


### Contributor
Putu Agus [(@antara-adiputra)](https://github.com/antara-adiputra/)
