import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Body, Query
from openai import AsyncOpenAI, OpenAIError
from openai.types.beta.threads.run import RequiredAction, LastError
from openai.types.beta.threads.run_submit_tool_outputs_params import ToolOutput
from pydantic import BaseModel, ValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pdfminer.high_level import extract_text_to_fp
from io import BytesIO, StringIO
from typing import List, Dict, Optional
import pandas as pd
import asyncio
import fitz
from bank_processors.bank_processor_factory import BankProcessorFactory
from openpyxl import load_workbook

app = FastAPI()
load_dotenv()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # used to run with react server
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = AsyncOpenAI(
    api_key=os.getenv("API_KEY"),
)

BANKS_TO_ASSISTANT_ID = {
    # "BANAMEX": "asst_IwRnr13nxU1PQRqKPhuXnkhA",
    "BANAMEX": "asst_1M8w2HKrJqJdjkPji7eF7JJK",
    "BANBAJIO": "asst_HFb4PfC3IuhImfaesLbDECso",
    "BANORTE": "asst_n4BnRdAVz8xYKZ7o47eur5XX",
    "BANREGIO": "asst_Ogn34gOlZI3VJ6GFpS218KTs",
    "BBVA": "asst_RbZOEgD5GQBscV62qiR329eT",
    "BX+": "asst_mLkxpzOSBF5YadZiiZhC4y3U",
    "CHASE": "asst_68agxxAZQii7q0vS5oKS4L1s",
    "HSBC": "asst_9Mzw7u5Z684PnipVujq1kNxa",
    "SANTANDER": "asst_1KzdIHmIMalKqkGFIlD3uxqU",
    "SCOTIABANK": "asst_dFXuIoZeyYjixPJVO5SNHx9X",
}

run_finished_states = ["completed", "failed", "cancelled", "expired", "requires_action"]


class RunStatus(BaseModel):
    run_id: str
    thread_id: str
    status: str
    required_action: Optional[RequiredAction]
    last_error: Optional[LastError]


class ThreadMessage(BaseModel):
    content: str
    role: str
    hidden: bool
    id: str
    created_at: int


class Thread(BaseModel):
    messages: List[ThreadMessage]


class CreateMessage(BaseModel):
    content: str


class Transaction(BaseModel):
    fecha: str
    tipo: str
    monto: float
    saldo: float


class CompareRequest(BaseModel):
    assistant_transactions: List[Transaction]
    aux_transactions: List[Transaction]


def get_assistant_id(bank: str) -> str:
    if bank not in BANKS_TO_ASSISTANT_ID:
        raise HTTPException(status_code=400, detail="Banco no válido")
    return BANKS_TO_ASSISTANT_ID[bank]


def convert_pdf_to_text(pdf_path, bank):
    # output_buffer = BytesIO()
    with open(pdf_path, "rb") as pdf_file:
        # extract_text_to_fp(pdf_file, output_buffer, output_type="tag")

        processor = BankProcessorFactory.get_processor(bank, pdf_file)
        process_data = processor.process()
        process_data = "\n".join(
            [" ".join(map(str, row)).strip() for row in process_data]
        )

    return process_data


def convert_scanned_pdf_to_text(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        text = ""
        for page in doc:
            text += page.get_text()
        return text
    except Exception as e:
        return str(e)


async def extract_text_from_aux(file: UploadFile):
    try:
        content = await file.read()  # Read file as binary
        excel_data = BytesIO(content)  # Convert to BytesIO for pandas

        # Read the file starting from row 7 (row index 6 in pandas)
        df = pd.read_excel(excel_data, skiprows=6, usecols="A,E:H")

        # Rename columns manually
        df.columns = ["Fecha", "Referencia", "Cargos", "Abonos", "Saldo"]

        saldo_inicial = df.iloc[1]["Saldo"]

        df_data = df.iloc[4:].reset_index(drop=True)

        df_data = df[
            df["Fecha"].replace([None, "", " ", 0], pd.NA).notna()
        ].reset_index(drop=True)

        df_data["Cargos"] = pd.to_numeric(df_data["Cargos"], errors="coerce").fillna(0)
        df_data["Abonos"] = pd.to_numeric(df_data["Abonos"], errors="coerce").fillna(0)
        df_data["Saldo"] = pd.to_numeric(df_data["Saldo"], errors="coerce").fillna(0)

        total_cargos = round(df_data["Cargos"].sum(), 2)
        total_abonos = round(df_data["Abonos"].sum(), 2)
        total_saldo = round(saldo_inicial + (total_cargos - total_abonos), 2)

        # Convert data to JSON format
        extracted_text = {
            "saldo_inicial": saldo_inicial,
            "datos": [
                {
                    "fecha": row["Fecha"],
                    "tipo": (
                        "saldo inicial"
                        if row["Cargos"] == 0 and row["Abonos"] == 0
                        else "retiro" if row["Abonos"] > 0 else "deposito"
                    ),
                    "monto": row["Abonos"] if row["Abonos"] > 0 else row["Cargos"],
                    "saldo": row["Saldo"],
                }
                for _, row in df_data.iterrows()
            ],
            "total_cargos": total_cargos,
            "total_abonos": total_abonos,
            "total_saldo": total_saldo,
        }

        print(extracted_text)
        return extracted_text
    except Exception as e:
        return {"error": f"Error processing the Excel file: {str(e)}"}


async def extract_text_from_previous(file: UploadFile):
    try:
        # Leer el archivo
        content = await file.read()
        previous_data = BytesIO(content)
        df = pd.read_excel(previous_data, header=None)

        # Eliminar filas y columnas completamente vacías
        df.dropna(how="all", inplace=True)
        df.dropna(axis=1, how="all", inplace=True)

        # Convertir DataFrame a lista de listas
        data = df.values.tolist()

        # Extraer información clave
        empresa = data[0][1] if len(data[0]) > 1 else None
        mes = data[1][1] if len(data[1]) > 1 else None
        cuenta = data[2][1] if len(data[2]) > 1 else None

        saldo_contabilidad = (
            data[3][-1] if isinstance(data[3][-1], (int, float)) else None
        )
        saldo_estado_cuenta = (
            data[7][-1] if isinstance(data[7][-1], (int, float)) else None
        )
        saldo_bancos = data[12][-1] if isinstance(data[12][-1], (int, float)) else None
        diferencia = data[6][-1] if isinstance(data[6][-1], (int, float)) else None

        depositos = data[4][-1] if isinstance(data[4][-1], (int, float)) else None
        retiros = data[5][-1] if isinstance(data[5][-1], (int, float)) else None
        depositos_en_transito = (
            data[9][-1] if isinstance(data[9][-1], (int, float)) else None
        )
        cheques_en_transito = (
            data[10][-1] if isinstance(data[10][-1], (int, float)) else None
        )

        # Construcción del diccionario final
        extracted_text = {
            "empresa": empresa,
            "mes": mes,
            "cuenta": cuenta,
            "saldos": {
                "saldo_contabilidad": saldo_contabilidad,
                "saldo_estado_cuenta": saldo_estado_cuenta,
                "saldo_bancos": saldo_bancos,
                "diferencia": diferencia,
            },
            "transacciones": {
                "depositos": depositos,
                "retiros": retiros,
                "depositos_en_transito": depositos_en_transito,
                "cheques_en_transito": cheques_en_transito,
            },
        }
        print(extracted_text)
        return extracted_text
    except Exception as e:
        return {"error": f"Error processing the Excel file: {str(e)}"}


async def extract_text_from_csv(file: UploadFile):
    try:
        content = await file.read()
        decoded_content = content.decode("utf-8")
        df = pd.read_csv(StringIO(decoded_content))

        extracted_text = df.where(pd.notna(df), None).to_dict(orient="records")

        return extracted_text
    except Exception as e:
        return {"error": f"Error al procesar el archivo Excel: {str(e)}"}


async def wait_on_run(thread_id: str, run_id: str, polling_interval: int = 3):
    """
    Waits for an OpenAI run to complete.

    Args:
        thread_id (str): The ID of the thread.
        run_id (str): The ID of the run.
        polling_interval (int): Time in seconds to wait between API checks.

    Returns:
        RunStatus: The final status of the run.
    """
    while True:
        run = await client.beta.threads.runs.retrieve(
            thread_id=thread_id, run_id=run_id
        )

        if run.status in run_finished_states:
            return RunStatus(
                run_id=run.id,
                thread_id=thread_id,
                status=run.status,
                required_action=run.required_action,
                last_error=run.last_error,
            )

        await asyncio.sleep(polling_interval)


@app.post("/extract_aux/")
async def extract_aux(file: UploadFile = File(...)):
    try:
        text_data = await extract_text_from_aux(file)

        if isinstance(text_data, dict) and "error" in text_data:
            return JSONResponse(content=text_data, status_code=400)

        return {"aux_transactions": text_data}
    except Exception as e:
        return JSONResponse(
            content={"error": f"Error al procesar el archivo Excel: {str(e)}"},
            status_code=500,
        )


@app.post("/extract_pdf/")
async def extract_pdf(file: UploadFile = File(...), bank: str = Form(...)):
    assistant = get_assistant_id(bank)
    file_location = f"temp_{file.filename}"

    try:
        with open(file_location, "wb") as f:
            f.write(await file.read())

        try:
            text = convert_pdf_to_text(file_location, bank.lower())
            if not text:
                text = convert_scanned_pdf_to_text(file_location)

        except Exception as e:
            return JSONResponse(
                content={"error": f"Error al procesar PDF: {str(e)}"}, status_code=500
            )

        return {
            "assistant": assistant,
            "filename": file.filename,
            "extracted_text": text,
        }
    finally:
        os.remove(file_location)


@app.post("/extract_previous/")
async def extract_previous(file: UploadFile = File(...)):
    try:
        text_data = await extract_text_from_previous(file)

        if isinstance(text_data, dict) and "error" in text_data:
            return JSONResponse(content=text_data, status_code=400)

        return {"previous_transactions": text_data}
    except Exception as e:
        return JSONResponse(
            content={"error": f"Error al procesar el archivo Excel: {str(e)}"},
            status_code=500,
        )


@app.post("/api/new")
async def post_new(data: dict = Body(...)):
    try:
        extracted_text = data.get("extracted_text")
        assistant = data.get("assistant")

        if not extracted_text or not assistant:
            raise HTTPException(status_code=400, detail="Faltan datos en la solicitud.")

        thread = await client.beta.threads.create()

        message = await client.beta.threads.messages.create(
            thread_id=thread.id, role="user", content=extracted_text
        )

        run = await client.beta.threads.runs.create(
            thread_id=thread.id, assistant_id=assistant
        )

        run_status = await wait_on_run(thread.id, run.id)

        if run_status.status != "completed":
            return JSONResponse(
                content={
                    "error": f"Error al procesar la solicitud: {run_status.status}"
                },
                status_code=500,
            )

        messages = await client.beta.threads.messages.list(thread_id=thread.id)

        assistant_messages = [msg for msg in messages.data if msg.role == "assistant"]

        assistant_response = (
            assistant_messages[-1].content[0].text.value
            if assistant_messages
            else "No hay respuesta del asistente."
        )

        return {"assistant_transactions": assistant_response}
    except OpenAIError as e:
        raise HTTPException(status_code=500, detail=f"Error de OpenAI: {str(e)}")


@app.post("/create_conciliation/")
async def create_conciliation():
    try:
        archivo_excel = "format.xlsx"
        wb = load_workbook(archivo_excel)
        hoja = wb.active

        hoja["C1"] = "TEST"
        hoja["C2"] = "MARZO 2025"
        hoja["C3"] = "BANORTE 123"
        hoja["B6"] = "SALDO EN CONTABILIDAD AL 31 DE MARZO 2025"

        saldo_aux = 10052.82
        total_depositos = 0
        total_retiros = 0
        diferencia = (saldo_aux + total_depositos) - total_retiros
        saldo_edo_cuenta = 10052.82
        total_depositos_transito = 0
        total_cheques_transito = 0
        saldo_banco = (
            saldo_edo_cuenta + total_depositos_transito
        ) - total_cheques_transito
        sobrante = diferencia - saldo_banco

        hoja["H6"] = saldo_aux
        hoja["H9"] = total_depositos
        hoja["H17"] = total_retiros
        hoja["H25"] = diferencia
        hoja["H28"] = saldo_edo_cuenta
        hoja["H31"] = total_depositos_transito
        hoja["H39"] = total_cheques_transito
        hoja["H47"] = saldo_banco
        hoja["H48"] = sobrante

        nombre_archivo = "CONCILIACION_MARZO_2025.xlsx"
        wb.save(nombre_archivo)

        # Devolver el archivo para descarga
        return FileResponse(
            path=nombre_archivo,
            filename=nombre_archivo,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        return JSONResponse(
            content={"error": f"Error al crear conciliación: {str(e)}"}, status_code=500
        )


@app.post("/compare_transactions/")
async def compare_transactions(request: CompareRequest):
    try:
        # Normalizar transacciones del PDF
        normalized_assistant_transactions = [
            {
                "fecha": t.fecha.upper(),
                "tipo": t.tipo.lower(),
                "monto": t.monto,
                "saldo": t.saldo,
            }
            for t in request.assistant_transactions
        ]

        # Normalizar transacciones del CSV
        normalized_aux_transactions = []
        for t in request.aux_transactions:
            if t.Fecha is None:  # Ignorar filas sin fecha (por ejemplo, la fila TOTAL)
                continue
            monto = 0.0
            tipo = ""
            if t.Abonos:
                monto = float(t.Abonos.replace(",", ""))
                tipo = "deposito"
            elif t.Cargos:
                monto = float(t.Cargos.replace(",", ""))
                tipo = "retiro"
            saldo = float(t.Saldo.replace(",", ""))
            normalized_aux_transactions.append(
                {
                    "fecha": t.Fecha.upper(),
                    "tipo": tipo,
                    "monto": monto,
                    "saldo": saldo,
                }
            )

        # Comparar transacciones
        discrepancies = []
        for pdf_txn in normalized_assistant_transactions:
            csv_txn = next(
                (
                    t
                    for t in normalized_aux_transactions
                    if t["fecha"] == pdf_txn["fecha"] and t["tipo"] == pdf_txn["tipo"]
                ),
                None,
            )
            if not csv_txn:
                discrepancies.append(
                    {
                        "type": "Falta en CSV",
                        "transaction": pdf_txn,
                    }
                )
            elif (
                csv_txn["monto"] != pdf_txn["monto"]
                or csv_txn["saldo"] != pdf_txn["saldo"]
            ):
                discrepancies.append(
                    {
                        "type": "Discrepancia en monto/saldo",
                        "assistant_transaction": pdf_txn,
                        "aux_transaction": csv_txn,
                    }
                )

        # Verificar transacciones en el CSV que no están en el PDF
        for csv_txn in normalized_aux_transactions:
            pdf_txn = next(
                (
                    t
                    for t in normalized_assistant_transactions
                    if t["fecha"] == csv_txn["fecha"] and t["tipo"] == csv_txn["tipo"]
                ),
                None,
            )
            if not pdf_txn:
                discrepancies.append(
                    {
                        "type": "Falta en PDF",
                        "transaction": csv_txn,
                    }
                )

        return {"discrepancies": discrepancies}
    except Exception as e:
        return JSONResponse(
            content={"error": f"Error al comparar transacciones: {str(e)}"},
            status_code=500,
        )


# @app.get("/api/threads/{thread_id}/runs/{run_id}")
# async def get_run(thread_id: str, run_id: str):
#     run = await client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run_id)

#     return RunStatus(
#         run_id=run.id,
#         thread_id=thread_id,
#         status=run.status,
#         required_action=run.required_action,
#         last_error=run.last_error,
#     )


# @app.get("/api/threads/{thread_id}")
# async def get_thread(thread_id: str):
#     messages = await client.beta.threads.messages.list(thread_id=thread_id)

#     result = [
#         ThreadMessage(
#             content=message.content[0].text.value,
#             role=message.role,
#             hidden="type" in message.metadata and message.metadata["type"] == "hidden",
#             id=message.id,
#             created_at=message.created_at,
#         )
#         for message in messages.data
#     ]

#     return Thread(
#         messages=result,
#     )


# @app.post("/api/threads/{thread_id}")
# async def post_thread(thread_id: str, message: CreateMessage):
#     await client.beta.threads.messages.create(
#         thread_id=thread_id, content=message.content, role="user"
#     )

#     run = await client.beta.threads.runs.create(
#         thread_id=thread_id, assistant_id=assistant_id
#     )

#     return RunStatus(
#         run_id=run.id,
#         thread_id=thread_id,
#         status=run.status,
#         required_action=run.required_action,
#         last_error=run.last_error,
#     )
