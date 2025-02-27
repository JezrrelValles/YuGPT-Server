from typing import List, Optional
import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Body
from openai import AsyncOpenAI, OpenAIError
from openai.types.beta.threads.run import RequiredAction, LastError
from openai.types.beta.threads.run_submit_tool_outputs_params import ToolOutput
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pdfminer.high_level import extract_text_to_fp
from io import BytesIO, StringIO
import pandas as pd
import asyncio
import fitz


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
    "BANAMEX": "asst_IwRnr13nxU1PQRqKPhuXnkhA",
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


def get_assistant_id(bank: str) -> str:
    if bank not in BANKS_TO_ASSISTANT_ID:
        raise HTTPException(status_code=400, detail="Banco no válido")
    return BANKS_TO_ASSISTANT_ID[bank]


def convert_pdf_to_text(pdf_path):
    output_buffer = BytesIO()
    with open(pdf_path, "rb") as pdf_file:
        extract_text_to_fp(pdf_file, output_buffer, output_type="tag")
        text_content = output_buffer.getvalue().decode("utf-8")
    return text_content


def convert_scanned_pdf_to_text(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        text = ""
        for page in doc:
            text += page.get_text()
        return text
    except Exception as e:
        return str(e)


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


@app.post("/convert_csv/")
async def convert_csv(file: UploadFile = File(...)):
    try:
        text_data = await extract_text_from_csv(file)

        if isinstance(text_data, dict) and "error" in text_data:
            return JSONResponse(content=text_data, status_code=400)

        return {"data": text_data}
    except Exception as e:
        return JSONResponse(
            content={"error": f"Error al procesar el archivo CSV: {str(e)}"},
            status_code=500,
        )


@app.post("/convert_pdf/")
async def convert_pdf(file: UploadFile = File(...), bank: str = Form(...)):
    assistant = get_assistant_id(bank)
    file_location = f"temp_{file.filename}"

    try:
        with open(file_location, "wb") as f:
            f.write(await file.read())

        try:
            text = convert_pdf_to_text(file_location)
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

        return {
            # "run_id": run.id,
            # "thread_id": thread.id,
            # "status": run_status.status,
            "assistant_response": assistant_response
        }
    except OpenAIError as e:
        raise HTTPException(status_code=500, detail=f"Error de OpenAI: {str(e)}")


@app.get("/api/threads/{thread_id}/runs/{run_id}")
async def get_run(thread_id: str, run_id: str):
    run = await client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run_id)

    return RunStatus(
        run_id=run.id,
        thread_id=thread_id,
        status=run.status,
        required_action=run.required_action,
        last_error=run.last_error,
    )


@app.get("/api/threads/{thread_id}")
async def get_thread(thread_id: str):
    messages = await client.beta.threads.messages.list(thread_id=thread_id)

    result = [
        ThreadMessage(
            content=message.content[0].text.value,
            role=message.role,
            hidden="type" in message.metadata and message.metadata["type"] == "hidden",
            id=message.id,
            created_at=message.created_at,
        )
        for message in messages.data
    ]

    return Thread(
        messages=result,
    )


@app.post("/api/threads/{thread_id}")
async def post_thread(thread_id: str, message: CreateMessage):
    await client.beta.threads.messages.create(
        thread_id=thread_id, content=message.content, role="user"
    )

    run = await client.beta.threads.runs.create(
        thread_id=thread_id, assistant_id=assistant_id
    )

    return RunStatus(
        run_id=run.id,
        thread_id=thread_id,
        status=run.status,
        required_action=run.required_action,
        last_error=run.last_error,
    )
