--Custo do produto frente a outros do original (perguntar se n é melhor usar o p_venda)
select po.num_orig, pr.codpro, po.num_fab, po.fantasia, pr.p_custo from"D-1".prd_tipo pr
join "D-1".produto po on pr.codpro = po.codpro
---where po.num_orig = '700888' and pr.cd_tploja = '01'


