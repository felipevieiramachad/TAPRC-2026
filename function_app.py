import json
import logging
import os
from datetime import datetime, timezone

import azure.functions as func
import mssql_python
from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()


# =========================================================
# CONEXÃO COM O BANCO DE ORIGEM
# =========================================================


def conectar_banco():
    sql_server = os.getenv("SQL_HOST")
    sql_database = os.getenv("SQL_DATABASE")
    sql_user = os.getenv("SQL_USER")
    sql_pass = os.getenv("SQL_PASSWORD")

    if not all([sql_server, sql_database, sql_user, sql_pass]):
        raise RuntimeError(
            "Uma ou mais variáveis de ambiente do banco não foram encontradas."
        )

    return mssql_python.connect(
        server=sql_server,
        database=sql_database,
        uid=sql_user,
        pwd=sql_pass,
        encrypt="yes",
        trust_server_certificate="no",
    )


# =========================================================
# DATA LAKE - BRONZE
# =========================================================


def criar_diretorios(file_system_client, caminho):
    caminho_atual = ""

    for parte in caminho.split("/"):
        if caminho_atual:
            caminho_atual = f"{caminho_atual}/{parte}"
        else:
            caminho_atual = parte

        try:
            file_system_client.create_directory(caminho_atual)
        except ResourceExistsError:
            pass


def salvar_json_bronze(entidade, columns, rows):
    connection_string = os.getenv("DATALAKE_CONNECTION_STRING")

    if not connection_string:
        raise RuntimeError("Variável DATALAKE_CONNECTION_STRING não encontrada.")

    registros = [dict(zip(columns, row)) for row in rows]

    now = datetime.now(timezone.utc)

    diretorio = f"itsm/{entidade}/" f"ano={now:%Y}/" f"mes={now:%m}/" f"dia={now:%d}"

    nome_arquivo = f"{entidade}_{now:%Y%m%dT%H%M%SZ}.json"

    datalake = DataLakeServiceClient.from_connection_string(connection_string)

    file_system = datalake.get_file_system_client("bronze")

    criar_diretorios(
        file_system,
        diretorio,
    )

    directory_client = file_system.get_directory_client(diretorio)

    file_client = directory_client.get_file_client(nome_arquivo)

    conteudo_json = json.dumps(
        registros,
        ensure_ascii=False,
        default=str,
        indent=2,
    )

    file_client.upload_data(
        conteudo_json.encode("utf-8"),
        overwrite=True,
    )

    return f"bronze/{diretorio}/{nome_arquivo}"


# =========================================================
# EXTRAÇÃO GENÉRICA
# =========================================================


def extrair_e_salvar(tabela, entidade_lake):
    logging.info(
        "Iniciando extração da tabela itsm.%s",
        tabela,
    )

    try:
        with conectar_banco() as conn:
            cursor = conn.cursor()

            cursor.execute(f"SELECT * FROM itsm.{tabela}")

            rows = cursor.fetchall()

            columns = [column[0] for column in cursor.description]

            logging.info(
                "itsm.%s: %s registros encontrados",
                tabela,
                len(rows),
            )

        arquivo = salvar_json_bronze(
            entidade_lake,
            columns,
            rows,
        )

        logging.info(
            "Arquivo gravado com sucesso em %s",
            arquivo,
        )

    except Exception as e:
        logging.error(
            "Erro ao processar itsm.%s: %s",
            tabela,
            str(e),
        )
        raise


# =========================================================
# ANALISTA
# =========================================================


@app.function_name(name="extract_analista")
@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_analista(myTimer: func.TimerRequest) -> None:
    extrair_e_salvar(
        "analista",
        "analistas",
    )


# =========================================================
# CSAT AVALIACAO
# =========================================================


@app.function_name(name="extract_csat_avaliacao")
@app.timer_trigger(
    schedule="5 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_csat_avaliacao(
    myTimer: func.TimerRequest,
) -> None:
    extrair_e_salvar(
        "csat_avaliacao",
        "csat_avaliacoes",
    )


# =========================================================
# CATEGORIA
# =========================================================


@app.function_name(name="extract_categoria")
@app.timer_trigger(
    schedule="10 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    extrair_e_salvar(
        "categoria",
        "categorias",
    )


# =========================================================
# FILA
# =========================================================


@app.function_name(name="extract_fila")
@app.timer_trigger(
    schedule="15 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_fila(myTimer: func.TimerRequest) -> None:
    extrair_e_salvar(
        "fila",
        "filas",
    )


# =========================================================
# CHAMADO
# =========================================================


@app.function_name(name="extract_chamado")
@app.timer_trigger(
    schedule="20 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    extrair_e_salvar(
        "chamado",
        "chamados",
    )


# =========================================================
# SLA
# =========================================================


@app.function_name(name="extract_sla")
@app.timer_trigger(
    schedule="25 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_sla(myTimer: func.TimerRequest) -> None:
    extrair_e_salvar(
        "sla",
        "slas",
    )


# =========================================================
# CHAMADO SLA
# =========================================================


@app.function_name(name="extract_chamado_sla")
@app.timer_trigger(
    schedule="30 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_chamado_sla(
    myTimer: func.TimerRequest,
) -> None:
    extrair_e_salvar(
        "chamado_sla",
        "chamados_sla",
    )


# =========================================================
# SOLICITANTE
# =========================================================


@app.function_name(name="extract_solicitante")
@app.timer_trigger(
    schedule="35 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_solicitante(
    myTimer: func.TimerRequest,
) -> None:
    extrair_e_salvar(
        "solicitante",
        "solicitantes",
    )


# =========================================================
# CHAMADO STATUS HISTORICO
# =========================================================


@app.function_name(name="extract_chamado_status_historico")
@app.timer_trigger(
    schedule="40 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_chamado_status_historico(
    myTimer: func.TimerRequest,
) -> None:
    extrair_e_salvar(
        "chamado_status_historico",
        "chamados_status_historico",
    )


# =========================================================
# CLIENTE ORGANIZACAO
# =========================================================


@app.function_name(name="extract_cliente_organizacao")
@app.timer_trigger(
    schedule="50 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extract_cliente_organizacao(
    myTimer: func.TimerRequest,
) -> None:
    extrair_e_salvar(
        "cliente_organizacao",
        "clientes_organizacao",
    )
