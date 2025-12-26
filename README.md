# Spectrum7 SCADA Availability
Aplikasi yang digunakan untuk penghitungan availability SCADA pada bagian Fasilitas Operasi PLN UP2B Sistem Makassar

>Python3.10+, 
>pandas, 
>Xlsxwriter,
>nicegui


## Feature
  1. Membaca data dari file
     - [x] Memuat data historical SCADA dari file
     - [x] Ekstensi yang disupport xls, xlsx, dan xml
     - [x] Support file **Historical Message Spectrum**
	 - [x] Support file **Event Log Message Survalent** dan **Status Point Survalent** (baru)
  1. Read dari Offline Server (mulai versi v3.3.0, fitur ditangguhkan untuk dievaluasi kembali)
     - [-] Memuat data historical SCADA dari database Offline
     - [-] Sinkronisasi data poin SCADA untuk standarisasi penamaan
  1. Perhitungan keberhasilan RCD
     - [x] Perhitungan **SUKSES / GAGAL** RCD
     - [x] Perhitungan repetisi kontrol
     - [x] Analisa & hipotesis awal gagal kontrol
     - [x] Rekomendasi tagging untuk bay berdasarkan kontribusi pengurangan Sukses RC
     - [x] _Single-run-only_, cukup running program sekali, file output sudah dapat otomatis menghitung perubahan dari user
     - [x] Menggabungkan dan merekap beberapa output file
  1. Perhitungan availability RTU
     - [x] Memilah dan menghitung downtime RTU
     - [x] Perhitungan **Availability Link**
     - [x] Klasifikasi downtime berdasarkan waktu untuk memudahkan filter dan analisa gangguan
     - [x] _Single-run-only_, cukup running program sekali, file output sudah dapat otomatis menghitung perubahan dari user
     - [x] Menggabungkan dan merekap beberapa output file


#### Update 31-03-2024
Pengembangan support untuk perhitungan RC dari file historical Master Survalent.


#### Update 30-04-2024
Peningkatan performa dalam durasi membuka file maupun proses perhitungan hingga **4x** lebih cepat dari versi sebelumnya.


#### Update 31-05-2024
- Pengembangan struktur perhitungan availability RTU
- Penambahan perhitungan availability link komunikasi
- Peningkatan dokumentasi aplikasi


#### Update 15-07-2024
- Web GUI interface
- Optimalisasi performa perhitungan
- Fitur "Test" untuk memudahkan troubleshooting


#### Update 25-12-2025 (Terbaru)
- Pembaharuan GUI
- Optimalisasi integrasi _core_ dengan tampilan GUI
- Peningkatan _error handling_ untuk meminimalisir _crash_ pada saat pengoperasian
- Pengembangan _logging_ pada _core_ dan GUI untuk memudahkan penelusuran error
- Peningkatan performa _binding state_ untuk meringankan proses GUI


## Contributor
Putu Agus [(@antara-adiputra)](https://github.com/antara-adiputra/)
