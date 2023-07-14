import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware

from events import service
from events.models import BucketsResponse, BucketsRangeRequest

app = FastAPI()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    """ Handle validation error for pydantic models """
    return JSONResponse(
        status_code=400,
        content={"error": "Validation Error", "detail": exc.errors()},
    )


@app.get("/events/customers/{customer_id}/buckets", response_model=BucketsRangeRequest, status_code=200)
def get_buckets(request: BucketsRangeRequest, customer_id: str):
    """ Get all events buckets started in the time range
    """
    # Get all event buckets for customer in the given time range.
    buckets_response = service.get_buckets(request)
    return buckets_response


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
