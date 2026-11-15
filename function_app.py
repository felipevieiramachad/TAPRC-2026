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


def conectar_banco_destino():
    sql_server = os.getenv("DEST_SQL_HOST")
    sql_database = os.getenv("DEST_SQL_DATABASE")
    sql_user = os.getenv("DEST_SQL_USER")
    sql_pass = os.getenv("DEST_SQL_PASSWORD")

    if not all([sql_server, sql_database, sql_user, sql_pass]):
        raise RuntimeError(
            "Uma ou mais variáveis do banco de destino não foram encontradas."
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


def ler_ultimo_json_bronze(entidade):
    connection_string = os.getenv("DATALAKE_CONNECTION_STRING")

    if not connection_string:
        raise RuntimeError("Variável DATALAKE_CONNECTION_STRING não encontrada.")

    datalake = DataLakeServiceClient.from_connection_string(connection_string)

    file_system = datalake.get_file_system_client("bronze")

    now = datetime.now(timezone.utc)

    diretorio = f"itsm/{entidade}/" f"ano={now:%Y}/" f"mes={now:%m}/" f"dia={now:%d}"

    paths = file_system.get_paths(path=diretorio)

    arquivos = sorted(
        [
            path.name
            for path in paths
            if not path.is_directory and path.name.endswith(".json")
        ],
        reverse=True,
    )

    if not arquivos:
        raise RuntimeError(f"Nenhum arquivo encontrado em {diretorio}.")

    for arquivo in arquivos:
        file_client = file_system.get_file_client(arquivo)

        conteudo = file_client.download_file().readall()

        if not conteudo.strip():
            logging.warning(
                "Arquivo vazio ignorado: %s",
                arquivo,
            )
            continue

        try:
            registros = json.loads(conteudo.decode("utf-8"))
        except json.JSONDecodeError:
            logging.warning(
                "Arquivo JSON inválido ignorado: %s",
                arquivo,
            )
            continue

        logging.info(
            "%s: %s registros lidos do Bronze",
            entidade,
            len(registros),
        )

        logging.info(
            "Arquivo utilizado: %s",
            arquivo,
        )

        return registros

    raise RuntimeError(f"Nenhum JSON válido encontrado em {diretorio}.")


def popular_categoria_destino():
    registros = ler_ultimo_json_bronze("categorias")

    with conectar_banco_destino() as conn:
        cursor = conn.cursor()

        cursor.execute("SET IDENTITY_INSERT dbo.categoria ON")

        try:
            for registro in registros:
                cursor.execute(
                    """
                    UPDATE dbo.categoria
                    SET
                        cd_categoria = ?,
                        nm_categoria = ?,
                        ds_descricao = ?,
                        fl_ativo = ?,
                        dt_inclusao = ?,
                        dt_atualizacao = ?,
                        nm_sistema_origem = ?,
                        cd_registro_origem = ?
                    WHERE id_categoria = ?
                    """,
                    (
                        registro["cd_categoria"],
                        registro["nm_categoria"],
                        registro["ds_descricao"],
                        registro["fl_ativo"],
                        registro["dt_inclusao"],
                        registro["dt_atualizacao"],
                        registro["nm_sistema_origem"],
                        registro["cd_registro_origem"],
                        registro["id_categoria"],
                    ),
                )

                if cursor.rowcount == 0:
                    cursor.execute(
                        """
                        INSERT INTO dbo.categoria (
                            id_categoria,
                            cd_categoria,
                            nm_categoria,
                            ds_descricao,
                            fl_ativo,
                            dt_inclusao,
                            dt_atualizacao,
                            nm_sistema_origem,
                            cd_registro_origem
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            registro["id_categoria"],
                            registro["cd_categoria"],
                            registro["nm_categoria"],
                            registro["ds_descricao"],
                            registro["fl_ativo"],
                            registro["dt_inclusao"],
                            registro["dt_atualizacao"],
                            registro["nm_sistema_origem"],
                            registro["cd_registro_origem"],
                        ),
                    )

            conn.commit()

        finally:
            cursor.execute("SET IDENTITY_INSERT dbo.categoria OFF")

        logging.info(
            "Tabela dbo.categoria atualizada com %s registros",
            len(registros),
        )


def popular_tabela_destino(
    entidade_bronze,
    tabela_destino,
    chave_primaria,
    colunas,
):
    registros = ler_ultimo_json_bronze(entidade_bronze)

    with conectar_banco_destino() as conn:
        cursor = conn.cursor()

        cursor.execute(f"SET IDENTITY_INSERT dbo.{tabela_destino} ON")

        try:
            for indice, registro in enumerate(registros, start=1):
                valor_id = registro[chave_primaria]

                cursor.execute(
                    f"""
                    SELECT 1
                    FROM dbo.{tabela_destino}
                    WHERE {chave_primaria} = ?
                    """,
                    (valor_id,),
                )

                existe = cursor.fetchone() is not None

                if existe:
                    colunas_update = [
                        coluna for coluna in colunas if coluna != chave_primaria
                    ]

                    set_sql = ", ".join(f"{coluna} = ?" for coluna in colunas_update)

                    valores = [registro[coluna] for coluna in colunas_update]

                    valores.append(valor_id)

                    cursor.execute(
                        f"""
                        UPDATE dbo.{tabela_destino}
                        SET {set_sql}
                        WHERE {chave_primaria} = ?
                        """,
                        tuple(valores),
                    )

                else:
                    nomes_colunas = ", ".join(colunas)

                    placeholders = ", ".join("?" for _ in colunas)

                    valores = [registro[coluna] for coluna in colunas]

                    cursor.execute(
                        f"""
                        INSERT INTO dbo.{tabela_destino} (
                            {nomes_colunas}
                        )
                        VALUES ({placeholders})
                        """,
                        tuple(valores),
                    )

                if indice % 25 == 0 or indice == len(registros):
                    logging.info(
                        "dbo.%s: %s/%s registros processados",
                        tabela_destino,
                        indice,
                        len(registros),
                    )

            cursor.execute(f"SET IDENTITY_INSERT dbo.{tabela_destino} OFF")

            conn.commit()

        except Exception:
            conn.rollback()
            raise

        logging.info(
            "Tabela dbo.%s atualizada com %s registros",
            tabela_destino,
            len(registros),
        )


def popular_todas_tabelas():
    popular_tabela_destino(
        "categorias",
        "categoria",
        "id_categoria",
        [
            "id_categoria",
            "cd_categoria",
            "nm_categoria",
            "ds_descricao",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    )

    popular_tabela_destino(
        "clientes_organizacao",
        "cliente_organizacao",
        "id_cliente_organizacao",
        [
            "id_cliente_organizacao",
            "cd_cliente_organizacao",
            "nm_cliente_organizacao",
            "nr_cnpj",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    )

    popular_tabela_destino(
        "filas",
        "fila",
        "id_fila",
        [
            "id_fila",
            "cd_fila",
            "nm_fila",
            "ds_descricao",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    )

    popular_tabela_destino(
        "slas",
        "sla",
        "id_sla",
        [
            "id_sla",
            "cd_sla",
            "nm_sla",
            "qt_meta_minutos",
            "ds_descricao",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    )

    popular_tabela_destino(
        "analistas",
        "analista",
        "id_analista",
        [
            "id_analista",
            "cd_analista",
            "nm_analista",
            "ds_email",
            "ds_nivel",
            "id_fila_atual",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    )

    popular_tabela_destino(
        "solicitantes",
        "solicitante",
        "id_solicitante",
        [
            "id_solicitante",
            "cd_solicitante",
            "id_cliente_organizacao",
            "nm_solicitante",
            "ds_email",
            "ds_telefone",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    )

    popular_tabela_destino(
        "chamados",
        "chamado",
        "id_chamado",
        [
            "id_chamado",
            "nr_chamado",
            "ds_tipo_chamado",
            "ds_status_chamado",
            "ds_prioridade",
            "dt_criacao",
            "dt_resolucao",
            "dt_ultima_atualizacao",
            "id_analista_atual",
            "id_reporter",
            "id_categoria",
            "id_cliente_organizacao",
            "id_fila_atual",
            "ds_titulo",
            "ds_descricao",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    )

    popular_tabela_destino(
        "chamados_sla",
        "chamado_sla",
        "id_chamado_sla",
        [
            "id_chamado_sla",
            "id_chamado",
            "id_sla",
            "fl_breach",
            "qt_tempo_restante_minutos",
            "qt_tempo_decorrido_minutos",
            "qt_meta_minutos",
            "dt_referencia",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    )

    popular_tabela_destino(
        "chamados_status_historico",
        "chamado_status_historico",
        "id_chamado_status_historico",
        [
            "id_chamado_status_historico",
            "id_chamado",
            "ds_status_chamado",
            "dt_inicio_status",
            "dt_fim_status",
            "qt_tempo_status_minutos",
            "id_analista_responsavel",
            "id_fila",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    )

    popular_tabela_destino(
        "csat_avaliacoes",
        "csat_avaliacao",
        "id_csat_avaliacao",
        [
            "id_csat_avaliacao",
            "id_chamado",
            "id_analista",
            "nr_score",
            "ds_comentario",
            "dt_avaliacao",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    )

    logging.info("Carga completa do banco de destino finalizada com sucesso.")


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


# =========================================================
# CARGA DIÁRIA DO BANCO DE DESTINO
# =========================================================


@app.function_name(name="carga_diaria_banco_destino")
@app.timer_trigger(
    schedule="0 0 6 * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True,
)
def carga_diaria_banco_destino(
    myTimer: func.TimerRequest,
) -> None:
    logging.info("Iniciando carga diária do banco de destino.")

    popular_todas_tabelas()

    logging.info("Carga diária do banco de destino concluída.")
