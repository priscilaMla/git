--Custo do produto frente a outros do original (perguntar se n é melhor usar o p_venda)
select po.num_orig, pr.codpro, po.num_fab, po.fantasia, pr.p_custo, pr.p_venda from"D-1".prd_tipo pr
join "D-1".produto po on pr.codpro = po.codpro
---where po.num_orig = '700888' and pr.cd_tploja = '01'


--query de devoluções
SELECT 
        pe.cd_produto AS cod_pro, e.dt_emissao, pe.qt_devolve AS devolucoes, pe.cd_loja
FROM "D-1".prod_ent pe
JOIN "D-1".entrada e ON e.cd_loja = pe.cd_loja AND e.sg_serie = pe.sg_serie AND e.nu_nota = pe.nu_nota
JOIN "D-1".cliente cli ON cli.codcli = pe.cd_cliente
WHERE
        e.dt_emissao >= current_date - 1826 AND e.in_cancela = 'N'
        AND e.in_clifor = 'C' AND UPPER(e.nfeenvstat) NOT LIKE '%DENEG%'
        AND pe.cd_cfop NOT IN ('1949', '2949', '1603')
        AND cli.codcli NOT IN ('99999','88888','21097')
        AND cli.codcid <> '0501' AND cli.codarea <> '112' AND pe.cd_loja != '08'


--query de zerado no estoque
select *  from"D-1".prd_zero
where cd_loja = '01'

--query de promoções
select pp.cd_produto AS cod_pro, pro.dt_virada, pro.dt_valida 
from"D-1".prod_pro pp
join "D-1".promocao pro on pp.sq_promoca = pro.sq_promoca and pp.cd_tploja = pro.cd_tploja

where pro.cd_tploja = '01'