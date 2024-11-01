import pandas as pd
from datetime import datetime
from query import consulta_servico_cobranca
from rotas import gera_dados_rota, executa_correcao
import schedule
from apscheduler.schedulers.background import BackgroundScheduler
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from tabulate import tabulate

def enviar_email(mensagem_texto):
    mensagem = 'A automação de correção de forma cobrança foi executada para os seguintes casos:\n {df_servico_forma_cobranca}'.format(df_servico_forma_cobranca=mensagem_texto)
    assunto = 'Automação forma cobrança'
    # Configurações do servidor SMTP
    MAIL_HOST = "mail.allrede.net.br"
    MAIL_PORT = 465
    MAIL_USERNAME = "novosprodutosdev@allrede.net.br"
    MAIL_PASSWORD = "Devallrede.1010@"

    # Configurações do e-mail
    MAIL_FROM_ADDRESS = "novosprodutosdev@allrede.net.br"
    MAIL_FROM_NAME = "Allrede"
    MAIL_TO_ADDRESS = "leidiane.rodrigues@allrede.com.br,amanda.lima@allrede.com.br"
    # "leidiane.rodrigues@allrede.com.br,amanda.lima@allrede.com.br"
    #MAIL_TO_ADDRESS = "jorge.pacheco@allrede.com.br,leidiane.rodrigues@allrede.com.br,amanda.lima@allrede.com.br,erick.oliveira@allrede.com.br"
    MAIL_SUBJECT = assunto
    MAIL_BODY = mensagem

    # Criar a mensagem de e-mail
    mensagem = MIMEMultipart()
    mensagem["From"] = f"{MAIL_FROM_NAME} <{MAIL_FROM_ADDRESS}>"
    # Use split para obter uma lista de destinatários
    to_addresses = MAIL_TO_ADDRESS.split(',')
    mensagem["To"] = ', '.join(to_addresses)
    mensagem["Subject"] = MAIL_SUBJECT

    # Adicionar corpo ao e-mail
    mensagem.attach(MIMEText(MAIL_BODY, "plain"))

    # Configurar conexão SMTP
    try:
        servidor_smtp = smtplib.SMTP_SSL(MAIL_HOST, MAIL_PORT)
        servidor_smtp.login(MAIL_USERNAME, MAIL_PASSWORD)

        # Enviar e-mail
        servidor_smtp.sendmail(MAIL_FROM_ADDRESS, to_addresses, mensagem.as_string())

        print("E-mail enviado com sucesso!")

    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")

    finally:
        # Fechar a conexão com o servidor SMTP
        if servidor_smtp:
            servidor_smtp.quit()

def verifica_servico_forma_cobranca():
    retorno = ''
    servico = ''
    mensagem = ''
    mensagem_texto = ''
    df_servico_forma_cobranca = consulta_servico_cobranca()
    df_de_para = pd.read_excel("de_para.xlsx")
    for i in range(0,len(df_servico_forma_cobranca)):
        id_cliente_servico = df_servico_forma_cobranca['id_cliente_servico'][i]
        plano = df_servico_forma_cobranca['plano'][i]
        forma_cob_antiga = df_servico_forma_cobranca['forma_cobranca'][i]
        # id_forma_cobranca_atual = df_servico_forma_cobranca['id_forma_cobranca'][i]
        # forma_cobranca = df_servico_forma_cobranca['forma_cobranca'][i]
        plano = df_servico_forma_cobranca['plano'][i]

        # Verifica se o valor de 'plano' está presente na coluna 'servico' do DataFrame df_de_para
        for j in range(0,len(df_de_para)):
            plano_normalizado = str(plano).strip().upper()
            # print(plano_normalizado)
            # print(servico)
            servico = df_de_para.loc[j,'servico']
            if servico in plano_normalizado:
                # Obtém o valor correspondente de 'id_forma_cobranca'
                id_forma_cobranca = df_de_para.loc[j,'id_forma_cobranca']
                forma_cobranca_correto = df_de_para.loc[j,'descricao']
                # descricao_forma_cobranca = df_de_para['descricao'][cont]
                # print(id_cliente_servico,id_forma_cobranca)
                dados_rota = gera_dados_rota(id_cliente_servico,id_forma_cobranca)
                # print(dados_rota)
                retorno = executa_correcao(id_cliente_servico,dados_rota)
                mensagem.append([id_cliente_servico, plano, forma_cob_antiga, forma_cobranca_correto])
                tabela = tabulate(mensagem, 
                              headers=["ID Cliente Serviço", "Plano", "De", "Para"], 
                              tablefmt='html',
                              colalign=("center", "center", "right"))
                mensagem_texto += tabela    
            else:
                id_forma_cobranca = 0  # ou outro valor padrão
    enviar_email(mensagem_texto)
    print('fim.....')
        
        


scheduler = BackgroundScheduler()

def rotina1():
    verifica_servico_forma_cobranca()
    

schedule.every().day.at("17:55").do(rotina1)
scheduler.start()

while (1 == 1):
    schedule.run_pending()
    threading.Event().wait(1)

# verifica_servico_forma_cobranca()