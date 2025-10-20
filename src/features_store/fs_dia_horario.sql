--Distribuição de horario/periodo de atividades do usuario:D28; --- DONE

--Tempo de atividade nas lives (primeira vs ultima iteracao no dia):D28; --- DONE

--Quantidade de dias de iteração:D28; --- DONE 

--Data do MAU,dia do mês,semana do mês,mês,ano:D28;

--Tempo semanal de iteração:D28;  --DONE

--Quantidade de lives com iteracao na semana(media) ---DONE

--Tempo médio por live:D28; --- DONE

--Tempo médio por semana:D28;  -- DONE

--Transação por minuto:D28; -- DONE

--Pontos por minuto:D28; ---  DONE

--Mensagem por minuto:D28;   ---  DONE

WITH tb_transacao AS (

        SELECT t1.IdCliente,
                t1.DtCriacao - INTERVAL 3 hour as DtTransacao,
                t1.IdTransacao,
                t1.QtdePontos,
                t3.DescNomeProduto


        FROM silver.upsell.transacoes as t1

        LEFT JOIN silver.upsell.transacao_produto as t2
        ON t1.IdTransacao = t2.IdTransacao

        LEFT JOIN silver.upsell.produtos as t3
        ON t2.IdProduto = t3.IdProduto

        WHERE DtCriacao - INTERVAL 3 hour < "{dt_ref}"

        AND DtCriacao - INTERVAL 3 hour > "{dt_ref}" - INTERVAL 28 day

),

tb_ds AS (

        SELECT date(dttransacao) as DtTransacao,
                IdCliente,
                count(distinct idtransacao) as nrqtdeTransacao,
                sum(qtdepontos) as nrqtdepontos
                

        FROM tb_transacao

        GROUP BY ALL
),
tb_horario AS (

        SELECT 
                t1.IdCliente,
                count(DISTINCT date(t1.dttransacao)) as nrqtdeDiasIteracao,
                count(DISTINCT t1.dttransacao)/4 as nrqtdeSemanasIteracao,
                sum(t2.nrqtdetransacao)/count(DISTINCT t1.dttransacao) as nrqtdeTransacaoDia,
                try_divide(COUNT(CASE WHEN hour(t1.DtTransacao) BETWEEN 0 AND 12 THEN date(t1.dttransacao) END),count(DISTINCT CASE WHEN hour(t1.dttransacao) BETWEEN 0 AND 12 THEN t1.dttransacao END)) AS nrqtdeTransacaoManha,
                try_divide(COUNT(CASE WHEN hour(t1.DtTransacao) BETWEEN 12 AND 18 THEN date(t1.dttransacao) END),count(DISTINCT CASE WHEN hour(t1.dttransacao) BETWEEN 12 AND 18 THEN t1.dttransacao END)) AS nrqtdeTransacaoTarde,
                try_divide(COUNT(CASE WHEN hour(t1.DtTransacao) BETWEEN 18 AND 23 THEN date(t1.dttransacao) END),count(DISTINCT CASE WHEN hour(t1.dttransacao) BETWEEN 18 AND 23 THEN t1.dttransacao END)) AS nrqtdeTransacaoNoite,

                (max(float(to_timestamp(t2.dttransacao))) - min(float(to_timestamp(t2.dttransacao))))/60 AS nrqtdeTempoMinutosDia,

                COUNT(DISTINCT CASE WHEN t1.DescNomeProduto == 'ChatMessage' THEN t1.IdTransacao END ) AS nrqtdeMensagensDia
                
        FROM tb_transacao as t1

        LEFT JOIN tb_ds as t2
        ON t1.idcliente = t2.idcliente

        GROUP BY t1.IdCliente
),
tb_tempo AS (

        SELECT 
                t1.IdCliente,
                SUM(t2.nrqtdeTempoMinutosDia) AS nrqtdeMinutos,
                AVG(t2.nrqtdetempominutosdia) as nrMediaTempoMinutoDia,
                SUM(t2.nrqtdetempominutosdia)/4 as nrMediaTempoMinutoSemanal,
                try_divide(SUM(t2.nrqtdetempominutosdia),count(distinct weekofyear(t1.dttransacao))) as nrAVGTempoMinutoSemanalAtivo,
                try_divide(COUNT(distinct t1.IdTransacao),count(distinct weekofyear(t1.dttransacao))) as nrqtdeLivesSemanal,
                try_divide(SUM(t3.nrqtdepontos),SUM(t2.nrqtdeTempoMinutosDia)) AS nrQntPontosMinuto,
                try_divide(SUM(t3.nrqtdetransacao),SUM(t2.nrqtdeTempoMinutosDia)) AS nrQntTransacaoMinuto,
                try_divide(SUM(t2.nrqtdeMensagensDia),SUM(t2.nrqtdeTempoMinutosDia)) AS nrQntMenssagensMinuto



        FROM tb_transacao as t1

        LEFT JOIN tb_horario as t2
        ON t1.idcliente = t2.idcliente
        
        LEFT JOIN tb_ds as t3
        ON t1.idcliente = t3.idcliente

        GROUP BY t1.IdCliente
)

SELECT  "{dt_ref}" as dtRef,
        dayofweek("{dt_ref}") AS nrDiaSemana,
        dayofmonth("{dt_ref}") AS nrDiaMes,
        weekofyear("{dt_ref}") AS nrSemanaAno,
        month("{dt_ref}") AS nrMes,
        year("{dt_ref}") AS nrAno,
        t1.*,
        t2.nrmediatempominutodia,
        t2.nrmediatempominutosemanal,
        t2.nrAVGTempoMinutoSemanalAtivo,
        t2.nrqtdeMinutos,
        t2.nrqtdelivessemanal,
        t2.nrqntpontosminuto,
        t2.nrqnttransacaominuto,
        t2.nrqntmenssagensminuto

        

FROM tb_horario as t1

LEFT JOIN tb_tempo as t2
ON t1.IdCliente = t2.IdCliente


