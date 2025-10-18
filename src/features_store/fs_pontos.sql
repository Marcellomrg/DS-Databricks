--Quantidade de pontos acumulados:D7,D28,D56,Vida; ----DONE

--Quantidade de pontos por produto(absoluto):D28; ---DONE

--Média de pontos por dia:D28;         ----- DONE

--Pontos/Transações:D28;         ----DONE

WITH tb_transacoes AS (

    SELECT t1.IdCliente,
            t1.IdTransacao,
            t1.DtCriacao - INTERVAL 3 hour AS DtTransacao,
            t1.QtdePontos

    FROM silver.upsell.transacoes as t1

    WHERE t1.DtCriacao - INTERVAL 3 hour < '{dt_ref}'
),

tb_ds as (

    SELECT IdCliente,

                sum(QtdePontos) as nrQtdePontosVida,
                sum(CASE WHEN QtdePontos > 0 THEN QtdePontos END) as nrQtdePontosPositivosVida,
                sum(CASE WHEN QtdePontos < 0 THEN ABS(QtdePontos) END) as nrQtdePontosNegativosVida,
                sum(CASE WHEN DtTransacao > '{dt_ref}' - INTERVAL 7 day THEN QtdePontos ELSE 0 END) as nrQtdePontosD7,
                sum(CASE WHEN DtTransacao > '{dt_ref}' - INTERVAL 7 day AND QtdePontos > 0 THEN QtdePontos END) as nrQtdePontosPositivosD7,
                sum(CASE WHEN DtTransacao > '{dt_ref}' - INTERVAL 7 day AND QtdePontos < 0 THEN ABS(QtdePontos) END) as nrQtdePontosNegativosD7,
                sum(CASE WHEN DtTransacao > '{dt_ref}' - INTERVAL 28 day THEN QtdePontos ELSE 0 END) as nrQtdePontosD28,
                sum(CASE WHEN DtTransacao > '{dt_ref}' - INTERVAL 28 day AND QtdePontos > 0 THEN QtdePontos END) as nrQtdePontosPositivosD28,
                sum(CASE WHEN DtTransacao > '{dt_ref}' - INTERVAL 28 day AND QtdePontos < 0 THEN ABS(QtdePontos) END) as nrQtdePontosNegativosD28,
                sum(CASE WHEN DtTransacao > '{dt_ref}' - INTERVAL 56 day THEN QtdePontos ELSE 0 END) as nrQtdePontosD56,
                sum(CASE WHEN DtTransacao > '{dt_ref}' - INTERVAL 56 day AND QtdePontos > 0 THEN QtdePontos END) as nrQtdePontosPositivosD56,
                sum(CASE WHEN DtTransacao > '{dt_ref}' - INTERVAL 56 day AND QtdePontos < 0 THEN ABS(QtdePontos) END) as nrQtdePontosNegativosD56
                
    FROM tb_transacoes

    GROUP BY IdCliente
),
tb_produtos AS (

    SELECT 
            t1.IdCliente,
        SUM(CASE WHEN t3.DescNomeProduto == 'Airflow Lover' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosAirflowLover,
        SUM(CASE WHEN t3.DescNomeProduto == 'Airflow Lover' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivosAirflowLover,
        SUM(CASE WHEN t3.DescNomeProduto == 'Airflow Lover' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativosAirflowLover,
        SUM(CASE WHEN t3.DescNomeProduto == 'Daily Loot' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosDailyLoot,
        SUM(CASE WHEN t3.DescNomeProduto == 'Daily Loot' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivosDailyLoot,
        SUM(CASE WHEN t3.DescNomeProduto == 'Daily Loot' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativosDailyLoot,
        SUM(CASE WHEN t3.DescNomeProduto == 'Lista de presença' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosListaPresenca,
        SUM(CASE WHEN t3.DescNomeProduto == 'Lista de presença' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivosListaPresenca,
        SUM(CASE WHEN t3.DescNomeProduto == 'Lista de presença' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativosListaPresenca,
        SUM(CASE WHEN t3.DescNomeProduto == 'Presença Streak' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosPresencaStreak,
        SUM(CASE WHEN t3.DescNomeProduto == 'Presença Streak' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivosPresencaStreak,
        SUM(CASE WHEN t3.DescNomeProduto == 'Presença Streak' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativosPresencaStreak,
        SUM(CASE WHEN t3.DescNomeProduto == 'R Lover' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosRLover,
        SUM(CASE WHEN t3.DescNomeProduto == 'R Lover' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivosRLover,
        SUM(CASE WHEN t3.DescNomeProduto == 'R Lover' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativosRLover,
        SUM(CASE WHEN t3.DescNomeProduto == 'Resgatar Ponei' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosResgatarPonei,
        SUM(CASE WHEN t3.DescNomeProduto == 'Resgatar Ponei' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivosResgatarPonei,
        SUM(CASE WHEN t3.DescNomeProduto == 'Resgatar Ponei' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativosResgatarPonei,
        SUM(CASE WHEN t3.DescNomeProduto == 'Troca de Pontos StreamElements' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosTrocaPontosStreamElements,
        SUM(CASE WHEN t3.DescNomeProduto == 'Troca de Pontos StreamElements' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivosTrocaPontosStreamElements,
        SUM(CASE WHEN t3.DescNomeProduto == 'Troca de Pontos StreamElements' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativosTrocaPontosStreamElements,
        SUM(CASE WHEN t3.DescNomeProduto LIKE '%Venda de Item:%' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontositemVenda,
        SUM(CASE WHEN t3.DescNomeProduto LIKE '%Venda de Item:%' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivositemVenda,
        SUM(CASE WHEN t3.DescNomeProduto LIKE '%Venda de Item:%' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativositemVenda,

        SUM(CASE WHEN t3.DescNomeProduto == 'ChatMessage' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosChatMessage,
        SUM(CASE WHEN t3.DescNomeProduto == 'ChatMessage' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivosChatMessage,
        SUM(CASE WHEN t3.DescNomeProduto == 'ChatMessage' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativosChatMessage,
        SUM(CASE WHEN t3.DescNomeProduto == 'Churn_10pp' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosChurn_10pp,
        SUM(CASE WHEN t3.DescNomeProduto == 'Churn_10pp' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivosChurn_10pp,
        SUM(CASE WHEN t3.DescNomeProduto == 'Churn_10pp' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativosChurn_10pp,
        SUM(CASE WHEN t3.DescNomeProduto == 'Churn_2pp' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosChurn_2pp,
        SUM(CASE WHEN t3.DescNomeProduto == 'Churn_2pp' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivosChurn_2pp,
        SUM(CASE WHEN t3.DescNomeProduto == 'Churn_2pp' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativosChurn_2pp,
        SUM(CASE WHEN t3.DescNomeProduto == 'Churn_5pp' THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosChurn_5pp,
        SUM(CASE WHEN t3.DescNomeProduto == 'Churn_5pp' AND t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END) as nrQntPontosPositivosChurn_5pp,
        SUM(CASE WHEN t3.DescNomeProduto == 'Churn_5pp' AND t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END) as nrQntPontosNegativosChurn_5pp,

        SUM(t1.QtdePontos)/count(distinct date(t1.DtTransacao)) as nrQntPontosDia,
        SUM(ABS(t1.QtdePontos))/count(distinct date(t1.DtTransacao)) AS nrQntPontosGeralDia,
        SUM(CASE WHEN t1.QtdePontos > 0 THEN t1.QtdePontos ELSE 0 END)/count(distinct date(t1.DtTransacao)) as nrQntPontosPositivosDia,
        SUM(CASE WHEN t1.QtdePontos < 0 THEN ABS(t1.QtdePontos) ELSE 0 END)/count(distinct date(t1.DtTransacao)) as nrQntPontosNegativosDia,

        SUM(t1.Qtdepontos)/count(distinct t1.Idtransacao) as nrQntPontosTransacao,
        SUM(ABS(t1.Qtdepontos))/count(distinct t1.Idtransacao) AS nrQntPontosGeralTransacao,
        SUM(CASE WHEN t1.Qtdepontos > 0 THEN t1.Qtdepontos ELSE 0 END)/count(distinct t1.Idtransacao) as nrQntPontosPositivosTransacao,
        SUM(CASE WHEN t1.Qtdepontos < 0 THEN ABS(t1.Qtdepontos) ELSE 0 END)/count(distinct t1.Idtransacao) as nrQntPontosNegativosTransacao
        

    FROM tb_transacoes as t1

    LEFT JOIN silver.upsell.transacao_produto as t2

    ON t1.IdTransacao = t2.IdTransacao

    LEFT JOIN silver.upsell.produtos as t3

    ON t2.IdProduto = t3.IdProduto

    WHERE t1.DtTransacao > '{dt_ref}' - INTERVAL 28 day

    GROUP BY t1.IdCliente
)

SELECT  '{dt_ref}' AS dtRef,
        t1.*,
        t2.nrQtdePontosVida,
        t2.nrqtdepontospositivosvida,
        t2.nrqtdepontosnegativosvida,
        t2.nrQtdePontosD7,
        t2.nrqtdepontospositivosd7,
        t2.nrqtdepontosnegativosd7,
        t2.nrQtdePontosD28,
        t2.nrqtdepontospositivosd28,
        t2.nrqtdepontosnegativosd28,
        t2.nrQtdePontosD56,
        t2.nrqtdepontospositivosd56,
        t2.nrqtdepontosnegativosd56

FROM tb_produtos as t1

LEFT JOIN tb_ds as t2
ON t1.IdCliente = t2.IdCliente

