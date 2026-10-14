import logging
import azure.functions as func
import os
import mssql_python

app = func.FunctionApp()


# =========================================================
# CONEXÃO COM O BANCO
# =========================================================

def conectar_banco():

    sql_server = os.getenv("SQL_HOST")
    sql_database = os.getenv("SQL_DATABASE")
    sql_user = os.getenv("SQL_USER")
    sql_pass = os.getenv("SQL_PASSWORD")

    if not all([
        sql_server,
        sql_database,
        sql_user,
        sql_pass
    ]):
        raise Exception(
            "Uma ou mais variáveis de ambiente do banco não foram encontradas."
        )

    return mssql_python.connect(
        server=sql_server,
        database=sql_database,
        uid=sql_user,
        pwd=sql_pass,
        encrypt="yes",
        trust_server_certificate="no"
    )


# =========================================================
# ANALISTA
# =========================================================

@app.function_name(name="extract_analista")
@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def extract_analista(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração da tabela itsm.analista")

    try:
        with conectar_banco() as conn:

            cursor = conn.cursor()

            cursor.execute("SELECT * FROM itsm.analista")

            rows = cursor.fetchall()

            # Mostra somente os 5 primeiros registros
            for row in rows[:5]:
                logging.info("%s", row)

            logging.info(
                "itsm.analista: %s registros encontrados",
                len(rows)
            )

    except Exception as e:
        logging.error(
            "Erro ao ler itsm.analista: %s",
            str(e)
        )
        raise


# =========================================================
# CATEGORIA
# =========================================================

@app.function_name(name="extract_categoria")
@app.timer_trigger(
    schedule="10 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def extract_categoria(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração da tabela itsm.categoria")

    try:
        with conectar_banco() as conn:

            cursor = conn.cursor()

            cursor.execute("SELECT * FROM itsm.categoria")

            rows = cursor.fetchall()

            # Mostra somente os 5 primeiros registros
            for row in rows[:5]:
                logging.info("%s", row)

            logging.info(
                "itsm.categoria: %s registros encontrados",
                len(rows)
            )

    except Exception as e:
        logging.error(
            "Erro ao ler itsm.categoria: %s",
            str(e)
        )
        raise


# =========================================================
# CHAMADO
# =========================================================

@app.function_name(name="extract_chamado")
@app.timer_trigger(
    schedule="20 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def extract_chamado(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração da tabela itsm.chamado")

    try:
        with conectar_banco() as conn:

            cursor = conn.cursor()

            cursor.execute("SELECT * FROM itsm.chamado")

            rows = cursor.fetchall()

            # Mostra somente os 5 primeiros registros
            for row in rows[:5]:
                logging.info("%s", row)

            logging.info(
                "itsm.chamado: %s registros encontrados",
                len(rows)
            )

    except Exception as e:
        logging.error(
            "Erro ao ler itsm.chamado: %s",
            str(e)
        )
        raise


# =========================================================
# CHAMADO SLA
# =========================================================

@app.function_name(name="extract_chamado_sla")
@app.timer_trigger(
    schedule="30 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando extração da tabela itsm.chamado_sla")

    try:
        with conectar_banco() as conn:

            cursor = conn.cursor()

            cursor.execute("SELECT * FROM itsm.chamado_sla")

            rows = cursor.fetchall()

            # Mostra somente os 5 primeiros registros
            for row in rows[:5]:
                logging.info("%s", row)

            logging.info(
                "itsm.chamado_sla: %s registros encontrados",
                len(rows)
            )

    except Exception as e:
        logging.error(
            "Erro ao ler itsm.chamado_sla: %s",
            str(e)
        )
        raise


# =========================================================
# CHAMADO STATUS HISTORICO
# =========================================================

@app.function_name(name="extract_chamado_status_historico")
@app.timer_trigger(
    schedule="40 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def extract_chamado_status_historico(
    myTimer: func.TimerRequest
) -> None:

    logging.info(
        "Iniciando extração da tabela itsm.chamado_status_historico"
    )

    try:
        with conectar_banco() as conn:

            cursor = conn.cursor()

            cursor.execute(
                "SELECT * FROM itsm.chamado_status_historico"
            )

            rows = cursor.fetchall()

            # Mostra somente os 5 primeiros registros
            for row in rows[:5]:
                logging.info("%s", row)

            logging.info(
                "itsm.chamado_status_historico: %s registros encontrados",
                len(rows)
            )

    except Exception as e:
        logging.error(
            "Erro ao ler itsm.chamado_status_historico: %s",
            str(e)
        )
        raise


# =========================================================
# CLIENTE ORGANIZACAO
# =========================================================

@app.function_name(name="extract_cliente_organizacao")
@app.timer_trigger(
    schedule="50 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def extract_cliente_organizacao(
    myTimer: func.TimerRequest
) -> None:

    logging.info(
        "Iniciando extração da tabela itsm.cliente_organizacao"
    )

    try:
        with conectar_banco() as conn:

            cursor = conn.cursor()

            cursor.execute(
                "SELECT * FROM itsm.cliente_organizacao"
            )

            rows = cursor.fetchall()

            # Mostra somente os 5 primeiros registros
            for row in rows[:5]:
                logging.info("%s", row)

            logging.info(
                "itsm.cliente_organizacao: %s registros encontrados",
                len(rows)
            )

    except Exception as e:
        logging.error(
            "Erro ao ler itsm.cliente_organizacao: %s",
            str(e)
        )
        raise

# =========================================================
# CSAT AVALIACAO
# =========================================================

@app.function_name(name="extract_csat_avaliacao")
@app.timer_trigger(
    schedule="5 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:

    logging.info(
        "Iniciando extração da tabela itsm.csat_avaliacao"
    )

    try:
        with conectar_banco() as conn:

            cursor = conn.cursor()

            cursor.execute(
                "SELECT * FROM itsm.csat_avaliacao"
            )

            rows = cursor.fetchall()

            for row in rows[:5]:
                logging.info("%s", row)

            logging.info(
                "itsm.csat_avaliacao: %s registros encontrados",
                len(rows)
            )

    except Exception as e:
        logging.error(
            "Erro ao ler itsm.csat_avaliacao: %s",
            str(e)
        )
        raise


# =========================================================
# FILA
# =========================================================

@app.function_name(name="extract_fila")
@app.timer_trigger(
    schedule="15 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def extract_fila(myTimer: func.TimerRequest) -> None:

    logging.info(
        "Iniciando extração da tabela itsm.fila"
    )

    try:
        with conectar_banco() as conn:

            cursor = conn.cursor()

            cursor.execute(
                "SELECT * FROM itsm.fila"
            )

            rows = cursor.fetchall()

            for row in rows[:5]:
                logging.info("%s", row)

            logging.info(
                "itsm.fila: %s registros encontrados",
                len(rows)
            )

    except Exception as e:
        logging.error(
            "Erro ao ler itsm.fila: %s",
            str(e)
        )
        raise


# =========================================================
# SLA
# =========================================================

@app.function_name(name="extract_sla")
@app.timer_trigger(
    schedule="25 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def extract_sla(myTimer: func.TimerRequest) -> None:

    logging.info(
        "Iniciando extração da tabela itsm.sla"
    )

    try:
        with conectar_banco() as conn:

            cursor = conn.cursor()

            cursor.execute(
                "SELECT * FROM itsm.sla"
            )

            rows = cursor.fetchall()

            for row in rows[:5]:
                logging.info("%s", row)

            logging.info(
                "itsm.sla: %s registros encontrados",
                len(rows)
            )

    except Exception as e:
        logging.error(
            "Erro ao ler itsm.sla: %s",
            str(e)
        )
        raise


# =========================================================
# SOLICITANTE
# =========================================================

@app.function_name(name="extract_solicitante")
@app.timer_trigger(
    schedule="35 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def extract_solicitante(myTimer: func.TimerRequest) -> None:

    logging.info(
        "Iniciando extração da tabela itsm.solicitante"
    )

    try:
        with conectar_banco() as conn:

            cursor = conn.cursor()

            cursor.execute(
                "SELECT * FROM itsm.solicitante"
            )

            rows = cursor.fetchall()

            for row in rows[:5]:
                logging.info("%s", row)

            logging.info(
                "itsm.solicitante: %s registros encontrados",
                len(rows)
            )

    except Exception as e:
        logging.error(
            "Erro ao ler itsm.solicitante: %s",
            str(e)
        )
        raise