import logging
import os

import azure.functions as func
import pyodbc

app = func.FunctionApp()


@app.function_name(name="extract_categoria")
@app.timer_trigger(
    schedule="0 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def extract_categoria(myTimer: func.TimerRequest) -> None:

    logging.info("Iniciando leitura da tabela itsm.categoria")

    # Recupera as credenciais das variáveis de ambiente
    sql_server = os.environ["SQL_HOST"]
    sql_database = os.environ["SQL_DATABASE"]
    sql_user = os.environ["SQL_USER"]
    sql_pass = os.environ["SQL_PASSWORD"]

    # String de conexão com SQL Server
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # Abre conexão com o banco
        with pyodbc.connect(conn_str) as conn:

            cursor = conn.cursor()

            # Consulta a tabela escolhida
            query = "SELECT * FROM itsm.categoria"

            cursor.execute(query)

            rows = cursor.fetchall()

            # Mostra uma amostra dos dados encontrados (5 primeiros)
            for row in rows[:5]:
                logging.info(row)

            logging.info(
                f"Consulta concluída. Registros encontrados: {len(rows)}"
            )

    except Exception as e:
        logging.error(
            f"Erro ao ler a tabela itsm.categoria: {str(e)}"
        )
        raise
