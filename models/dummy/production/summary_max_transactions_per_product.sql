{{ config(database="side_project") }}

SELECT	nama_cabang,
		nama_depan,
		tgl_transaksi,
		nama_produk,
		jumlah_pembelian 
FROM
(SELECT
	mc.nama_cabang, 
	mk.nama_depan, 
	tp.tgl_transaksi,
	mp.nama_produk,
	tp.jumlah_pembelian,
	ROW_NUMBER() OVER(PARTITION BY mp.kode_produk ORDER BY tp.jumlah_pembelian DESC, tp.tgl_transaksi) as ranking
  FROM  {{ ref("federated_tr_penjualan") }} tp
  JOIN {{ ref("ms_produk") }} mp 
    ON tp.kode_produk = mp.kode_produk
  JOIN {{ ref("ms_karyawan") }} mk 
    ON tp.kode_kasir = mk.kode_karyawan 
  JOIN {{ ref("ms_cabang") }} mc 
    ON mk.kode_cabang = mc.kode_cabang) as pembelian_maks
WHERE ranking=1
ORDER BY 1,2,3