WITH tabel_keuntungan AS(
SELECT mc.nama_cabang, 
	FORMAT_DATE('%Y-%m (%B)', tp.tgl_transaksi) AS bulan,
	SUM((mhh.harga_berlaku_cabang-mhh.modal_cabang-mhh.biaya_cabang)*tp.jumlah_pembelian) AS keuntungan 
FROM {{ ref("federated_tr_penjualan") }} tp
JOIN {{ ref("ms_cabang") }} mc 
  ON tp.kode_cabang = mc.kode_cabang
JOIN {{ ref("ms_harga_harian") }} mhh 
  ON (mhh.kode_cabang = tp.kode_cabang) 
  AND (mhh.kode_produk = tp.kode_produk) 
  AND (DATE(mhh.tgl_berlaku) = DATE(tp.tgl_transaksi))
GROUP BY 1,2
ORDER BY 1,2)
SELECT nama_cabang, 
	bulan,
	keuntungan,
	CONCAT(ROUND((keuntungan-LAG(keuntungan) OVER (PARTITION BY nama_cabang ORDER BY bulan ASC))/
	  LAG(keuntungan) OVER (PARTITION BY nama_cabang ORDER BY bulan ASC)*100,2), ' %') AS persen_growth
FROM tabel_keuntungan
ORDER BY 1,2