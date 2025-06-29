import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Body
from openai import AsyncOpenAI, OpenAIError
from openai.types.beta.threads.run import RequiredAction, LastError
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from io import BytesIO
from typing import List, Optional
import pandas as pd
import asyncio
from bank_processors.bank_processor_factory import BankProcessorFactory
from openpyxl import load_workbook
from mistralai import Mistral
import datetime
import math

app = FastAPI()
load_dotenv()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = AsyncOpenAI(
    api_key=os.getenv("API_KEY"),
)

mistral_api_key = os.getenv("MISTRAL_API")
mistral_client = Mistral(api_key=mistral_api_key)


BANKS_TO_ASSISTANT_ID = {
    "BANAMEX": "asst_1M8w2HKrJqJdjkPji7eF7JJK",
    "BANBAJIO": "asst_YRhqGDSFvH8K5siImwzl32sx",
    "BANORTE": "asst_dHWzuDg0D0rxyXGay3hC9uEM",
    "BANREGIO": "asst_h9ScdBXkSI3V5yV7w1BTg7f5",
    "BBVA": "asst_Bmuha99CJW515evmB4vljYH7",
    "BX+": "asst_mLkxpzOSBF5YadZiiZhC4y3U",
    "CHASE": "asst_68agxxAZQii7q0vS5oKS4L1s",
    "HSBC": "asst_9Mzw7u5Z684PnipVujq1kNxa",
    "SANTANDER": "asst_HOQwBr6KtWHAfv2JbrNySRN5",
    "SCOTIABANK": "asst_6lN2cWBcCQ8RuiOqWWA7clgu"
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


async def convert_pdf_to_text(pdf_path, bank):
    with open(pdf_path, "rb") as pdf_file:
        processor = BankProcessorFactory.get_processor(bank, pdf_file)
        process_data = processor.process()
        process_data = "\n".join(
            [" ".join(map(str, row)).strip() for row in process_data]
        )

    return process_data

async def convert_scanned_pdf_to_text(pdf_path):
    try:
        with open(pdf_path, "rb") as file:
            uploaded_pdf = mistral_client.files.upload(
                file={
                    "file_name": pdf_path,
                    "content": file,
                },
                purpose="ocr"
            )

        signed_url = mistral_client.files.get_signed_url(file_id=uploaded_pdf.id)
        print(signed_url)
        ocr_response_pages =  mistral_client.ocr.process(
            model="mistral-ocr-latest",
            document={
                "type": "document_url",
                "document_url": signed_url.url
            }
        ).pages
        
        text = ""
        for page in ocr_response_pages:
            text += page.markdown + "\n"
        
        print(f"Texto : {text}")
        
        await mistral_client.files.delete_async(file_id=uploaded_pdf.id)
        
        #Create a .txt with the ocr text extracted
        with open(datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", "w", encoding="utf-8") as file:
            file.write(text)
            
        return text
    except Exception as e:
        raise Exception(f"Error en convert_scanned_pdf_to_text: {str(e)}")

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
        total_saldo = saldo_inicial + (total_cargos - total_abonos)

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

def is_nan(value):
    try:
        return math.isnan(value)
    except:
        return False

async def extract_text_from_previous(file: UploadFile):
    try:
        # Leer el archivo
        content = await file.read()
        previous_data = BytesIO(content)
        df = pd.read_excel(previous_data, header=None)

        # Convertir DataFrame a lista de listas
        data = df.values.tolist()

        # Extraer información clave
        empresa = data[0][2] if len(data[0]) > 1 else None
        mes = data[1][2] if len(data[1]) > 1 else None
        cuenta = data[2][2] if len(data[2]) > 1 else None

        saldo_contabilidad = (
            data[5][-1] if isinstance(data[5][-1], (int, float)) else None
        )
        saldo_estado_cuenta = (
            data[27][-1] if isinstance(data[27][-1], (int, float)) else None
        )
        saldo_bancos = data[46][-1] if isinstance(data[46][-1], (int, float)) else None
        saldo_segun_contabilidad = data[24][-1] if isinstance(data[24][-1], (int, float)) else None

        depositos = data[8][-1] if isinstance(data[8][-1], (int, float)) else None
        retiros = data[16][-1] if isinstance(data[16][-1], (int, float)) else None
        depositos_en_transito = (
            data[30][-1] if isinstance(data[30][-1], (int, float)) else None
        )
        cheques_en_transito = (
            data[38][-1] if isinstance(data[38][-1], (int, float)) else None
        )
        diferencia = data[47][-1] if isinstance(data[47][-1], (int, float)) else None

        transacciones_depositos = [[item for item in data[i][0:7:2] if not is_nan(item)] 
                    for i in range(9, 15)
                ]
        transacciones_retiros = [
                    [item for item in data[i][0:7:2] if not is_nan(item)] 
                    for i in range(17, 23)
                ]
        transacciones_depositos_en_transito = [
                    [item for item in data[i][0:7:2] if not is_nan(item)] 
                    for i in range(31, 37)
                ]
        transacciones_cheques_en_transito = [
                    [item for item in data[i][0:7:2] if not is_nan(item)] 
                    for i in range(39, 45)
                ]

        # Construcción del diccionario final
        extracted_text = {
            "empresa": empresa,
            "mes": mes,
            "cuenta": cuenta,
            "saldos": {
                "saldo_contabilidad": saldo_contabilidad,
                "saldo_segun_contabilidad": saldo_segun_contabilidad,
                "saldo_estado_cuenta": saldo_estado_cuenta,
                "saldo_bancos": saldo_bancos,
                "diferencia": diferencia,
            },
            "depositos": {
                "total": depositos,
                "transacciones": transacciones_depositos,
            },
            "retiros": {
                "total": retiros,
                "transacciones": transacciones_retiros,
            },
            "depositos_en_transito": {
                "total": depositos_en_transito,
                "transacciones": transacciones_depositos_en_transito,
            },
            "cheques_en_transito": {
                "total": cheques_en_transito,
                "transacciones": transacciones_cheques_en_transito,
            },
        }
        print(extracted_text)
        return extracted_text
    except Exception as e:
        return {"error": f"Error processing the Excel file: {str(e)}"}

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

@app.post("/extract_account/")
async def extract_account(file: UploadFile = File(...), bank: str = Form(...)):
    assistant = get_assistant_id(bank)
    file_location = f"temp_{file.filename}"

    try:
        with open(file_location, "wb") as f:
            f.write(await file.read())

        try:
            text = await convert_pdf_to_text(file_location, bank.lower())
        except Exception as e:
            print(f"Error en convert_pdf_to_text: {e}")
            text = None

        if not text:
            try:
                text = await convert_scanned_pdf_to_text(file_location)
            except Exception as e:
                print(f"Error en convert_scanned_pdf_to_text: {e}")
                return JSONResponse(
                    content={"error": f"Error al procesar PDF: {str(e)}"},
                    status_code=500
                )

        return {
            "assistant": assistant,
            "filename": file.filename,
            "extracted_text": text,
        }
    finally:
        if os.path.exists(file_location):
            os.remove(file_location)

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

        hoja["C1"] = "EVOLUCION MULTIMEDIA MEXICO S DE RL DE CV"
        hoja["C2"] = "FEBRERO 2025"
        hoja["C3"] = "BANBAJIO CTA: 90201"
        hoja["B6"] = "SALDO EN CONTABILIDAD AL 28 DE FEBRERO 2025"

        saldo_contabilidad = 0 #75372.25
        total_depositos = 0
        total_retiros = 0
        saldo_segun_contabilidad = (saldo_contabilidad + total_depositos) - total_retiros
        saldo_estado_cuenta = 0 #76728.27
        total_depositos_transito = 0
        total_cheques_transito = 0 #1356.81 
        saldo_bancos = (
            saldo_estado_cuenta + total_depositos_transito
        ) - total_cheques_transito
        diferencia = saldo_segun_contabilidad - saldo_bancos

        hoja["H6"] = saldo_contabilidad
        hoja["H9"] = total_depositos
        hoja["H17"] = total_retiros
        hoja["H25"] = saldo_segun_contabilidad
        hoja["H28"] = saldo_estado_cuenta
        hoja["H31"] = total_depositos_transito
        hoja["H39"] = total_cheques_transito
        hoja["H47"] = saldo_bancos
        hoja["H48"] = diferencia

        nombre_archivo = "conciliacion.xlsx"
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