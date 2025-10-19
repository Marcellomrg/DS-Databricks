WITH tb_daily AS (

        SELECT DISTINCT
                idcliente,
                date(DtCriacao) AS Dtdia

        FROM silver.upsell.transacoes as t1

        ORDER BY IdCliente,dtdia
),
tb_ref AS (

    SELECT dtRef,
            IdCliente

    FROM feature_store.upsell.fs_geral
    WHERE day(dtRef) = 1
   

),
tb_churn AS (

    SELECT 
          t1.dtRef,
          t1.IdCliente,
          CASE WHEN t2.IdCliente is NULL THEN 1 ELSE 0 END AS flChurn

    FROM tb_ref as t1

    LEFT JOIN tb_daily as t2

    ON t1.IdCliente = t2.IdCliente

    AND t1.dtref <= t2.dtdia
    AND t1.dtref > t2.dtdia - INTERVAL 28 day

    GROUP BY ALL
    ORDER BY t1.IdCliente,t1.dtRef
    )

    SELECT * 
    FROM tb_churn
    QUALIFY row_number() OVER (PARTITION BY IdCliente ORDER BY rand()) <= 2 