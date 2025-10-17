
--Quantidade de transações por produto:D28;

--Quantidade de transações:D7,D28,D56,Vida; ---DONE

--Total de transações/quantidade de dias:D7,D28,D56,Vida; --DONE

WITH tb_transacoes AS (

    SELECT 

    * FROM silver.upsell.transacoes

    WHERE DtCriacao - INTERVAL 3 hour < '{dt_ref}'

),

tb_ds  as (

  SELECT IdCliente,
          
          count(CASE WHEN DtCriacao > '{dt_ref}' - INTERVAL 7 day THEN IdTransacao END) AS nrQntTransacoesD7,
          count(CASE WHEN DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN IdTransacao END) AS nrQntTransacoesD28,
          count(CASE WHEN DtCriacao > '{dt_ref}' - INTERVAL 56 day THEN IdTransacao END) AS nrQntTransacoesD56,
          count(IdTransacao) AS nrQntTransacoesVida,

        try_divide(1.0 * count(CASE WHEN DtCriacao > '{dt_ref}' - INTERVAL 7 day THEN IdTransacao END), count(DISTINCT CASE WHEN DtCriacao > '{dt_ref}' - INTERVAL 7 day THEN DATE(DtCriacao) END)) as nrTransacaoDiaD7,
        try_divide (1.0 * count(CASE WHEN DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN IdTransacao END),count(DISTINCT CASE WHEN DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN DATE(DtCriacao) END)) as nrTransacaoDiaD28,
        try_divide (1.0 * count(CASE WHEN DtCriacao > '{dt_ref}' - INTERVAL 56 day THEN IdTransacao END),count(DISTINCT CASE WHEN DtCriacao > '{dt_ref}' - INTERVAL 56 day THEN DATE(DtCriacao) END)) as nrTransacaoDiaD56,
        try_divide (1.0 * count(IdTransacao),count(DISTINCT DATE(DtCriacao))) as nrTransacaoDiaVida


  FROM tb_transacoes

  GROUP BY IdCliente

),

tb_produtos AS (

SELECT IdCliente,
          COUNT(( CASE WHEN t3.DescNomeProduto == 'Churn_5pp' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)) AS nrQntTransacoesChurn_5pp,
          COUNT((CASE WHEN t3.DescNomeProduto == 'Churn_10pp' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)) AS nrQntTransacoesChurn_10pp,
          COUNT((CASE WHEN t3.DescNomeProduto == 'Churn_2pp' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)) AS nrQntTransacoesChurn_2pp,
          COUNT((CASE WHEN t3.DescNomeProduto == 'Airflow Lover' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)) AS nrQntTransacoesAirflowLover,
          COUNT((CASE WHEN t3.DescNomeProduto == 'Daily Loot' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)) AS nrQntTransacoesDailyLoot,
          COUNT((CASE WHEN t3.DescNomeProduto == 'Lista de presença' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)) AS nrQntTransacoesListaPresenca,
          COUNT((CASE WHEN t3.DescNomeProduto == 'Presença Streak' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)) AS nrQntTransacoesPresencaStreak,
          COUNT((CASE WHEN t3.DescNomeProduto == 'R Lover' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)) AS nrQntTransacoesRLover,
          COUNT((CASE WHEN t3.DescNomeProduto == 'Resgatar Ponei' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)) AS nrQntTransacoesResgatarPonei,
          COUNT((CASE WHEN t3.DescNomeProduto == 'Troca de Pontos StreamElements' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)) AS nrQntTransacoesTrocaPontosStreamElements,
          COUNT((CASE WHEN t3.DescNomeProduto LIKE '%Venda de Item:%'AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)) AS
          nrQntTransacoesitemVenda,

          1.0 * try_divide(COUNT((CASE WHEN t3.DescNomeProduto == 'Churn_5pp' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)),COUNT((CASE WHEN t1.DtCriacao > '{dt_ref}'THEN t1.IdTransacao END)))  AS   nrPctTransacoesChurn_5pp,

          1.0 * try_divide(COUNT((CASE WHEN t3.DescNomeProduto == 'Churn_10pp' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)),COUNT((CASE WHEN t1.DtCriacao > '{dt_ref}'THEN t1.IdTransacao END)))  AS   nrPctTransacoesChurn_10pp,

          1.0 * try_divide(COUNT((CASE WHEN t3.DescNomeProduto == 'Churn_2pp' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)),COUNT((CASE WHEN t1.DtCriacao > '{dt_ref}'THEN t1.IdTransacao END))) AS   nrPctTransacoesChurn_2pp,

          1.0 * try_divide(COUNT((CASE WHEN t3.DescNomeProduto == 'Airflow Lover' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)),COUNT((CASE WHEN t1.DtCriacao > '{dt_ref}'THEN t1.IdTransacao END)))  AS nrPctTransacoesAirflowLover,

          1.0 * try_divide(COUNT((CASE WHEN t3.DescNomeProduto == 'Daily Loot' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)),COUNT((CASE WHEN t1.DtCriacao > '{dt_ref}'THEN t1.IdTransacao END)))  AS   nrPctTransacoesDailyLoot,

          1.0 * try_divide(COUNT((CASE WHEN t3.DescNomeProduto == 'Lista de presença' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)),COUNT((CASE WHEN t1.DtCriacao > '{dt_ref}'THEN t1.IdTransacao END)))  AS   nrPctTransacoesListaPresenca,

          1.0 * try_divide(COUNT((CASE WHEN t3.DescNomeProduto == 'Presença Streak' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)),COUNT((CASE WHEN t1.DtCriacao > '{dt_ref}'THEN t1.IdTransacao END)))  AS   nrPctTransacoesPresencaStreak,

          1.0 * try_divide(COUNT((CASE WHEN t3.DescNomeProduto == 'R Lover' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)),COUNT((CASE WHEN t1.DtCriacao > '{dt_ref}'THEN t1.IdTransacao END)))  AS   nrPctTransacoesRLover,

          1.0 * try_divide(COUNT((CASE WHEN t3.DescNomeProduto == 'Resgatar Ponei' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)),COUNT((CASE WHEN t1.DtCriacao > '{dt_ref}'THEN t1.IdTransacao END)))  AS   nrPctTransacoesResgatarPonei,

          1.0 * try_divide(COUNT((CASE WHEN t3.DescNomeProduto == 'Troca de Pontos StreamElements' AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)),COUNT((CASE WHEN t1.DtCriacao > '{dt_ref}'THEN t1.IdTransacao END)))  AS   nrPctTransacoesTrocaPontosStreamElements,

          1.0 * try_divide(COUNT((CASE WHEN t3.DescNomeProduto LIKE '%Venda de Item:%'AND t1.DtCriacao > '{dt_ref}' - INTERVAL 28 day THEN t1.IdTransacao END)),COUNT((CASE WHEN t1.DtCriacao > '{dt_ref}'THEN t1.IdTransacao END)))  AS   nrPctTransacoesitemVenda


          
FROM tb_transacoes as t1

LEFT JOIN silver.upsell.transacao_produto as t2
ON t1.IdTransacao = t2.IdTransacao

LEFT JOIN silver.upsell.produtos as t3
ON t2.IdProduto = t3.IdProduto

WHERE t1.DtCriacao >= '{dt_ref}' - INTERVAL 28 day

GROUP BY IdCliente

)

SELECT  '{dt_ref}' as dtRef,
        t1.*,
        t2.nrQntTransacoesD7,
        t2.nrQntTransacoesD28,
        t2.nrQntTransacoesD56,
        t2.nrQntTransacoesVida,
        t2.nrTransacaoDiaD7,
        t2.nrTransacaoDiaD28,
        t2.nrTransacaoDiaD56,
        t2.nrTransacaoDiaVida
        
        
FROM tb_produtos as t1

LEFT JOIN tb_ds as t2
ON t1.IdCliente = t2.IdCliente
