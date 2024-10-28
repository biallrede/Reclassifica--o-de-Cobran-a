import pandas as pd 
from credentials import credenciais_banco, credenciais_banco_teste, credenciais_banco_token

def consulta_servico_cobranca():
    conn = credenciais_banco()   
    query = '''
                select * from (
                select 
                a.data_venda::date,
                a.id_cliente_servico,
                c.codigo_cliente,
                d.descricao as plano,
                b.id_forma_cobranca,
                b.descricao as forma_cobranca,
                CASE WHEN UPPER(d.descricao) like '%ALL%' and UPPER(b.descricao) like '%ALL%' then 1
                    WHEN UPPER(d.descricao) like '%CONEX%' and UPPER(b.descricao) like '%CONEX%' then 1
                    WHEN (UPPER(d.descricao) like '%CP NET%'
                        OR UPPER(d.descricao) like '%C P NET%')
                        AND UPPER(b.descricao) like '%C P NET%' then 1
                    WHEN UPPER(d.descricao) like '%CPM%' and UPPER(b.descricao) like '%CPM NET%' then 1
                    WHEN UPPER(d.descricao) like '%MCP%' and UPPER(b.descricao) like '%MCP%' then 1
                    WHEN UPPER(d.descricao) like '%SKILL.NET%' and UPPER(b.descricao) like '%SKILLNET%' then 1
                    WHEN UPPER(d.descricao) like '%\FLAVIO%' and UPPER(b.descricao) like '%\FLAVIO%' then 1
                    WHEN UPPER(d.descricao) like '%LINKWAP%' and UPPER(b.descricao) like '%LINKWAP%' then 1
                    WHEN UPPER(d.descricao) like '%LOGTEL%' and UPPER(b.descricao) like '%LOGTEL%' then 1
                    WHEN UPPER(d.descricao) like '%TI5%' and UPPER(b.descricao) like '%TI5%' then 1
					WHEN UPPER(d.descricao) like '%TILOG%' and UPPER(b.descricao) like '%TI5%' then 1
                    WHEN UPPER(d.descricao) like '%UNILINK%' and UPPER(b.descricao) like '%UNI%' then 1
                    WHEN UPPER(d.descricao) like '%OBTI%' and UPPER(b.descricao) like '%OBTI%' then 1
                    WHEN UPPER(d.descricao) like '%MASTER%' and (UPPER(b.descricao) like '%MTEL%' 
                                                                OR UPPER(b.descricao) like '%TEC%'
                                                                OR UPPER(b.descricao) like '%MSVA%'
                                                                OR UPPER(b.descricao) like '%SEG-VAR%'
                                                                OR UPPER(b.descricao) like '%SEG-GOV%') then 1
                    WHEN UPPER(d.descricao) like '%LIGO%' and UPPER(b.descricao) like '%LIGO%' then 1
                    WHEN UPPER(b.descricao) like '%M&A%' then 1
                    ELSE 0 end status_cobranca
                from cliente_servico a
                left join forma_cobranca b on b.id_forma_cobranca = a.id_forma_cobranca
                left join cliente c on c.id_cliente = a.id_cliente
                left join servico d on d.id_servico = a.id_servico
                where a.data_cancelamento isnull
                and a.data_habilitacao notnull
                ) as consulta 
                where consulta.status_cobranca = 0
                --and consulta.id_cliente_servico = 1088061
                '''
        
    df = pd.read_sql(query,conn)
    return df

def consulta_grupo_cliente(id_cliente_servico):
    conn = credenciais_banco()
    query = '''
            SELECT B.descricao AS nome_tag
                    FROM cliente_servico_grupo A
                    LEFT JOIN grupo_cliente_servico B ON B.id = A.id_grupo_cliente_servico
                    WHERE ativo = true
                    and A.id_cliente_servico = {id_cliente_servico}

            '''.format(id_cliente_servico=id_cliente_servico)
    df = pd.read_sql(query,conn)
    return df

def consulta_informacoes_servico(id_cliente_servico):
    conn = credenciais_banco()
    query = '''
            select 
            a.id_servico,
            a.id_cliente_servico,
            a.id_servico_status,
            a.id_vencimento,
            a.valor,
            a.data_venda,
            a.agrupamento_nota,
            a.agrupamento_fatura,
            a.carne,
            a.tipo_cobranca,
            a.validade,
			a.id_usuario_vendedor,
			a.anotacoes,
			a.referencia,
			b.id_servico_tecnologia
            from cliente_servico a
            left join servico_tecnologia b on b.id_servico_tecnologia = a.id_servico_tecnologia
            where a.id_cliente_servico = {id_cliente_servico}

            '''.format(id_cliente_servico=id_cliente_servico)
    df = pd.read_sql(query,conn)
    return df

def consulta_token():
    conn = credenciais_banco_token()
    query = '''
            select token from API_TOKEN_HUBSOFT
            '''
    df = pd.read_sql(query,conn)
    return df