import requests
from dotenv import load_dotenv
import os
import json
from query import consulta_grupo_cliente,consulta_informacoes_servico, consulta_token
import pandas as pd

def gera_dados_rota(id_cliente_servico,id_forma_cobranca_correto):
    load_dotenv()
    # Defina o token de autenticação Bearer
    df_token = consulta_token()
    token = str(df_token.loc[0,'token'])
    # token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6Ijc1MTEwODgzMDRkNTcyMmEwOGI0MDVmOGY5YThhMWM1ZDZmYTZmZDZjM2YzZmZmMGZiYTBlOTYxODc0ZjI2NDVlZmU4ZmQyZTdlYjJlYThmIn0.eyJhdWQiOiIyMzYiLCJqdGkiOiI3NTExMDg4MzA0ZDU3MjJhMDhiNDA1ZjhmOWE4YTFjNWQ2ZmE2ZmQ2YzNmM2ZmZjBmYmEwZTk2MTg3NGYyNjQ1ZWZlOGZkMmU3ZWIyZWE4ZiIsImlhdCI6MTcyOTU0NTgyNiwibmJmIjoxNzI5NTQ1ODI2LCJleHAiOjE3MzIxMzc4MjYsInN1YiI6IjU3MTUiLCJzY29wZXMiOltdfQ.VO_luf4YCEE9h50npZeCOVzusg215oxSedYQwwYrUlvfdd0RXZ12q86Y2UbzWvB8rOUVdI_nCGXvUY4JXX1m1T4IiuKAd87U6n5qxieuX2v4kf7xY2eOn4kXOzF4s2BHmvMBRcbrtXb4Q_qMXvB4Zvpidr9MpU4_tOOCFd_zBApy05ZHAqYMhgd7A65NHrWBlFFUU_lNSpxeCknV_aHbGDU2aZ9rSgMnb5zjt9NRvzVh8MjaersJohALOJ4jzwVW1zlc2Xa_2nPtbYIOmEgzwO8uOUmYJxfP1JtaPuG-uK4zTCBye48uR_rC-CdFCv2LMGgr4E_foBfzY0h0SvMhXDj-xRLijXFBSPEy1Jq0OzK4qzh_10HEWioCI3XsElFOJ68GVv1BtBItfrMb7bCfne52h82mj5UHn5KncwsvJMMaTL2wslwUoksXwRLCrjOBfdIiqDsF6woU9uckduDwSxYXL1QCTECFi5B6eWnNu7cmvBD3Wefbv5LKtg2BJwHTx1K9CTTFTY7VFOkZolpY0y1tyltrbZx-RpyUhjH7yGy58LzrfaemD8xTpSjxeZcwuB3q3syNqudCo9KLcmTYd4cbdTKHa0RcRaXdCJlOaZ8P9rRPEkUGcZYhr0pk3gAYpsWpTXVEJg0NHPvleletrFofdylHjKJZKNUmBcEGiQ4'
    rota = os.getenv("URL_PROD") 
    # print(rota)
    url = "{rota}/api/v1/cliente/servico/{id_cliente}/edit".format(rota=rota,id_cliente=id_cliente_servico)
    print(url)
    headers = {'Content-Type': 'application/json',
                "Authorization": f"Bearer {token}", 
               'Cookie': 'hubsoft_session=njJaMHt39p2ZY7USCUu1tUDcoYzwsxJp2Djt8QHS'}
    try:
        response = requests.get(url, headers=headers)

        dados = response.json()
        response = requests.get(url, headers=headers)

        forma_dict = []
        if response.status_code == 200:
            dados = response.json()
            dados_servico = pd.DataFrame(consulta_informacoes_servico(id_cliente_servico))
            

            for i in range(0,len(dados_servico)):
                id_servico = dados_servico.loc[i,"id_servico"] 
                id_servico_status = dados_servico.loc[i,"id_servico_status"]
                id_vencimento = dados_servico.loc[i,"id_vencimento"]
                valor = dados_servico.loc[i,"valor"]
                data_venda = dados_servico.loc[i,"data_venda"]
                agrupamento_nota = dados_servico.loc[i,"agrupamento_nota"]
                agrupamento_fatura = dados_servico.loc[i,"agrupamento_fatura"]
                carne = dados_servico.loc[i,"carne"]
                tipo_cobranca = dados_servico.loc[i,"tipo_cobranca"]
                validade = dados_servico.loc[i,"validade"]
                referencia = dados_servico.loc[i,"referencia"]
                anotacoes = dados_servico.loc[i,"anotacoes"]
                vendedor = dados_servico.loc[i,"id_usuario_vendedor"]
                id_servico_tecnologia = dados_servico.loc[i,"id_servico_tecnologia"]

            # servico_tecnologia = dados.get("servico_tecnologia",[])
            # servico_tecnologia_dict = servico_tecnologia[0] if isinstance(servico_tecnologia, list) and servico_tecnologia else {}

    
            
            servico_encontrado = next((servico for servico in dados.get("servico_tecnologia", []) if servico["id_servico_tecnologia"] == id_servico_tecnologia), None)
            servico_tecnologia = servico_encontrado


            # servico_taxa_instalacao = dados.get("servico_taxa_instalacao",[])
            # for servico in servico_taxa_instalacao:
            #     servico["id_forma_cobranca"] = id_forma_cobranca_correto  # Corrigido para usar [] para atribuição

            forma_cobranca = dados.get("formas_cobranca",[])
            # print(forma_cobranca)
            for forma in forma_cobranca:
                if forma.get("id_forma_cobranca") == id_forma_cobranca_correto:
                    forma_dict = forma
            
            # print(forma_dict)
            
            # print(forma_dict)
            #para pegar o grupo correto do cliente pq ele retorna todos os grupos do sistema
            grupos_requisicao = dados.get("grupos", [])
            df_grupo_clientes = consulta_grupo_cliente(id_cliente_servico)
            nome_tags = df_grupo_clientes['nome_tag'].to_list()
            dados_atualizados = []

            for grupo in grupos_requisicao:
                if grupo['descricao'] in nome_tags:
                    # Se o grupo estiver na lista de nome_tags, adicione aos dados atualizados
                    dados_atualizados.append(grupo)
            
            dados_rota = json.dumps({
                "id_servico": int(id_servico),  # Certifique-se de converter para int
                "id_servico_tecnologia": int(id_servico_tecnologia),
                "id_cliente_servico": int(id_cliente_servico),  # Conversão para int
                "id_servico_status": int(id_servico_status),  # Conversão para int
                "id_vencimento": int(id_vencimento),  # Conversão para int
                "id_forma_cobranca": int(id_forma_cobranca_correto),
                "id_usuario_vendedor": int(vendedor),
                "anotacoes": str(anotacoes),
                "referencia": str(referencia),
                "valor": float(valor),  # Conversão para float, se necessário
                "data_venda": str(data_venda),  # Conversão para string, se necessário
                "agrupamento_nota": str(agrupamento_nota),
                "agrupamento_fatura": str(agrupamento_fatura),
                "carne": bool(carne),
                "tipo_cobranca": str(tipo_cobranca),
                "validade": str(validade),  # Conversão para string, se necessário
                "forma_cobranca": forma_dict,
                "servico_tecnologia": servico_tecnologia,
                # "servico_taxa_instalacao": servico_taxa_instalacao,
                "grupos": dados_atualizados
            })

            dados_rota = dados_rota.replace("'",'"')
            dados_rota = dados_rota.replace("None","null")
            dados_rota = dados_rota.replace("True","true")
            dados_rota = dados_rota.replace("False","false")
            print(dados_rota)
            return dados_rota
        else:
                return response.status_code
    except:
        print('Erro ao consultar api')
    
def executa_correcao(id_cliente_servico, dados_rota):
    load_dotenv()
    # token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6Ijc1MTEwODgzMDRkNTcyMmEwOGI0MDVmOGY5YThhMWM1ZDZmYTZmZDZjM2YzZmZmMGZiYTBlOTYxODc0ZjI2NDVlZmU4ZmQyZTdlYjJlYThmIn0.eyJhdWQiOiIyMzYiLCJqdGkiOiI3NTExMDg4MzA0ZDU3MjJhMDhiNDA1ZjhmOWE4YTFjNWQ2ZmE2ZmQ2YzNmM2ZmZjBmYmEwZTk2MTg3NGYyNjQ1ZWZlOGZkMmU3ZWIyZWE4ZiIsImlhdCI6MTcyOTU0NTgyNiwibmJmIjoxNzI5NTQ1ODI2LCJleHAiOjE3MzIxMzc4MjYsInN1YiI6IjU3MTUiLCJzY29wZXMiOltdfQ.VO_luf4YCEE9h50npZeCOVzusg215oxSedYQwwYrUlvfdd0RXZ12q86Y2UbzWvB8rOUVdI_nCGXvUY4JXX1m1T4IiuKAd87U6n5qxieuX2v4kf7xY2eOn4kXOzF4s2BHmvMBRcbrtXb4Q_qMXvB4Zvpidr9MpU4_tOOCFd_zBApy05ZHAqYMhgd7A65NHrWBlFFUU_lNSpxeCknV_aHbGDU2aZ9rSgMnb5zjt9NRvzVh8MjaersJohALOJ4jzwVW1zlc2Xa_2nPtbYIOmEgzwO8uOUmYJxfP1JtaPuG-uK4zTCBye48uR_rC-CdFCv2LMGgr4E_foBfzY0h0SvMhXDj-xRLijXFBSPEy1Jq0OzK4qzh_10HEWioCI3XsElFOJ68GVv1BtBItfrMb7bCfne52h82mj5UHn5KncwsvJMMaTL2wslwUoksXwRLCrjOBfdIiqDsF6woU9uckduDwSxYXL1QCTECFi5B6eWnNu7cmvBD3Wefbv5LKtg2BJwHTx1K9CTTFTY7VFOkZolpY0y1tyltrbZx-RpyUhjH7yGy58LzrfaemD8xTpSjxeZcwuB3q3syNqudCo9KLcmTYd4cbdTKHa0RcRaXdCJlOaZ8P9rRPEkUGcZYhr0pk3gAYpsWpTXVEJg0NHPvleletrFofdylHjKJZKNUmBcEGiQ4'
    df_token = consulta_token()
    token = str(df_token.loc[0,'token'])
    rota = os.getenv("URL_PROD")
    url = f"{rota}/api/v1/cliente/servico/{id_cliente_servico}"
    
    headers = {
        'Content-Type': 'application/json',
        "Authorization": f"Bearer {token}", 
        'Cookie': 'hubsoft_session=njJaMHt39p2ZY7USCUu1tUDcoYzwsxJp2Djt8QHS'
    }

    # Print dados_rota antes da requisição
    print("Dados enviados para a API:", dados_rota)
    
    try:
        # Enviar a requisição diretamente sem json.dumps() em dados_rota
        response = requests.put(url, headers=headers, data=dados_rota)
        print("Resposta da API:", response.status_code, response.json())
        return response.json()

    except Exception as e:
        print(f'Erro ao efetuar a mudança da forma de cobrança: {e}')


    

  
