import os
from datetime import datetime
from typing import Any, Dict, Optional
from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel

app = FastAPI(title="RoboJuggle Ad Agent Ingestion API", version="1.0.0")

# Cosmos DB Configuration via Managed Identity
COSMOS_ENDPOINT = os.getenv(
    "COSMOS_ENDPOINT",
    "https://db-robojuggle-adagent-eastus.documents.azure.com:443/",
)
DATABASE_NAME = os.getenv("COSMOS_DATABASE", "robojuggle-ad-agent")
CONTAINER_NAME = "ConversionEvents"

credential = DefaultAzureCredential()
client = CosmosClient(COSMOS_ENDPOINT, credential=credential)
database = client.get_database_client(DATABASE_NAME)
container = database.get_container_client(CONTAINER_NAME)


class WebhookPayload(BaseModel):
    tenantId: str
    conversionName: str
    conversionValue: float
    currency: Optional[str] = "USD"
    gclid: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    customData: Optional[Dict[str, Any]] = None


@app.get("/")
def health_check():
    return {"status": "online", "service": "RoboJuggle Ingestion Engine"}


@app.post("/api/v1/events/conversion")
async def receive_conversion_event(payload: WebhookPayload):
    try:
        event_doc = {
            "id": (
                f"{payload.tenantId}_{int(datetime.utcnow().timestamp() * 1000)}"
            ),
            "tenantId": payload.tenantId,
            "conversionName": payload.conversionName,
            "conversionValue": payload.conversionValue,
            "currency": payload.currency,
            "gclid": payload.gclid,
            "email": payload.email,
            "phone": payload.phone,
            "customData": payload.customData or {},
            "processed": False,
            "createdAt": datetime.utcnow().isoformat(),
        }

        # Write directly to the Cosmos DB partition
        container.create_item(body=event_doc)

        return {
            "status": "success",
            "message": "Conversion recorded",
            "id": event_doc["id"],
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Database error: {str(e)}"
        )
