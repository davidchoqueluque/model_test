
SELECT
  num_edad,
  val_scoring_ingreso,
  mto_scoring_ingreso_prom,
  mto_ratio_saldo_tc_ingreso,
  mto_ratio_saldo_ingreso,
  mto_ratio_saldo_veh_ingreso,
  ctd_ent_sbs,
  participacion_deuda_vehicular,
  mto_saldo_sbs,
  nse,
  cal_gral,
  antiguedad_vehiculo,
  des_marca,
  des_modelo,
  num_anio_fabricacion,
  mto_valor_actual,
  des_clase
FROM
  (
    SELECT
      mto_ratio_saldo_tc_ingreso,
      mto_ratio_saldo_veh_ingreso,
      mto_scoring_ingreso_prom,
      mto_ratio_saldo_ingreso,
      num_edad,
      val_scoring_ingreso,
      nse
    FROM
      `rs-prd-dlk-sbx-fsia-8104.prod_featurestore.persona__dynamic_pn`
    WHERE
      id_persona = 'AX-1762750'
  ) tab1 FULL
  OUTER JOIN (
    SELECT
      mto_valor_actual,
      antiguedad_vehiculo
    FROM
      `rs-prd-dlk-sbx-fsia-8104.prod_featurestore.vehiculo__dynamic`
    WHERE
      id_vehiculo = 'BOU530'
  ) tab2 ON 1 = 1 FULL
  OUTER JOIN (
    SELECT
      participacion_deuda_vehicular,
      ctd_ent_sbs,
      mto_saldo_sbs,
      cal_gral
    FROM
      `rs-prd-dlk-sbx-fsia-8104.prod_featurestore.persona__informacion_crediticia`
    WHERE
      id_persona = 'AX-1762750'
  ) tab3 ON 1 = 1 FULL
  OUTER JOIN (
    SELECT
      num_anio_fabricacion,
      des_modelo,
      des_marca
    FROM
      `rs-prd-dlk-sbx-fsia-8104.prod_featurestore.vehiculo__static`
    WHERE
      id_vehiculo = 'BOU530'
  ) tab4 ON 1 = 1;