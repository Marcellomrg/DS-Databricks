WITH tb_ativa as (

     SELECT *  
     FROM silver.upsell.transacoes

     WHERE DtCriacao - INTERVAL 3 hour < '{dt_ref}'
     AND DtCriacao - interval 3 hour >= "{dt_ref}" - INTERVAL 28 day

     ORDER BY DtCriacao DESC
 ),

tb_recencia as (
     SELECT 
          IdCliente,
          min(date_diff("{dt_ref}",DtCriacao - INTERVAL 3 hour)) as Recencia
     
     
     FROM tb_ativa


     GROUP BY IdCliente
     ORDER BY IdCliente
),

tb_vida as (
     SELECT 
          idCliente,
          sum(QtdePontos) as SaldoAtual,
          max(date_diff("{dt_ref}",DtCriacao - INTERVAL 3 hour)) as IdadeBase

     FROM silver.upsell.transacoes

     WHERE DtCriacao - INTERVAL 3 hour < '{dt_ref}'
     AND IdCliente IN (SELECT DISTINCT IdCliente FROM tb_recencia)

     GROUP BY idcliente
),

tb_final as (
     SELECT 
          t1.*,
          t2.saldoatual,
          t2.idadebase,
          t3.flEmail  as flEmail


     FROM tb_recencia as t1

     LEFT JOIN tb_vida as t2
     ON t1.IdCliente = t2.idCliente

     LEFT JOIN silver.upsell.clientes as t3
     ON t1.IdCliente = t3.IdCliente
  
)

SELECT    "{dt_ref}" as dtRef,
          *

 FROM tb_final
