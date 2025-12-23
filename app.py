from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text, event
from sqlalchemy.exc import SQLAlchemyError
import logging
import os
from typing import Any, Union
from starlette.middleware.base import BaseHTTPMiddleware
import psycopg
from dotenv import load_dotenv  # Explicitly imported
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database URL and credentials
DATABASE_URL = os.getenv("DATABASE_URL")
REX_API_KEY = os.getenv("REX_API_KEY")
if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL environment variable is required")
if not REX_API_KEY:
    logger.error("REX_API_KEY environment variable is not set")
    raise ValueError("REX_API_KEY environment variable is required")

# Parse connection details from DATABASE_URL for direct psycopg usage
from urllib.parse import urlparse
parsed_url = urlparse(DATABASE_URL)
DB_HOST = parsed_url.hostname
DB_PORT = parsed_url.port
DB_NAME = parsed_url.path[1:]  # Remove leading slash
DB_USER = parsed_url.username
DB_PASSWORD = parsed_url.password

# Adjust DATABASE_URL for SQLAlchemy psycopg dialect
# Ensure the URL starts with "postgresql+psycopg://" instead of "postgresql://"
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = "postgresql+psycopg://" + DATABASE_URL[len("postgresql://"):]

# Initialize FastAPI
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update with your frontend origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting configuration (default 100/hour)
RATE_LIMIT = os.getenv("RATE_LIMIT", "100/hour")
logger.info(f"Using rate limit: {RATE_LIMIT}")

# Initialize SlowAPI limiter
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

# Custom 429 handler
async def custom_rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={"detail": "Rate limit exceeded. Please try again later or contact your administrator."}
    )
app.add_exception_handler(RateLimitExceeded, custom_rate_limit_exceeded_handler)

# Create SQLAlchemy engine with psycopg dialect
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Ensure all SQLAlchemy connections are session-level read-only
@event.listens_for(engine, "connect")
def set_session_readonly(dbapi_connection, connection_record):
    try:
        # dbapi_connection is the raw psycopg connection
        dbapi_connection.set_session(readonly=True, autocommit=False)
        logger.debug("SQLAlchemy DBAPI session set to readonly")
    except Exception as e:
        logger.warning(f"Failed to set SQLAlchemy session to readonly: {e}")

@app.get("/sqlquery_alchemy/")
@limiter.limit(RATE_LIMIT)
async def sqlquery_alchemy(sqlquery: str, api_key: str, request: Request) -> Any:
    """Execute SQL query using SQLAlchemy and return results directly."""
    if api_key != REX_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    logger.debug(f"Received API call to SQLAlchemy endpoint: {request.url}")
    logger.debug(f"SQL Query: {sqlquery}")
    try:
        with engine.connect() as connection:
            trans = connection.begin()
            try:
                connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                result = connection.execute(text(sqlquery))
                if sqlquery.strip().lower().startswith('select'):
                    columns = result.keys()
                    rows = result.fetchall()
                    results = [dict(zip(columns, row)) for row in rows]
                    logger.debug(f"Query executed successfully via SQLAlchemy, returned {len(results)} rows")
                    trans.commit()
                    return results
                else:
                    trans.commit()
                    logger.debug("Non-SELECT query attempted in read-only transaction")
                    return {"status": "success", "message": "Query executed successfully"}
            except:
                trans.rollback()
                raise
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in SQLAlchemy endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.get("/sqlquery_direct/")
@limiter.limit(RATE_LIMIT)
async def sqlquery_direct(sqlquery: str, api_key: str, request: Request) -> Any:
    """Execute SQL query using direct psycopg connection and return results."""
    if api_key != REX_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    logger.debug(f"Received API call to direct connection endpoint: {request.url}")
    logger.debug(f"SQL Query: {sqlquery}")
    try:
        with psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        ) as connection:
            # Set transaction to read-only
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                with connection.cursor() as cursor:
                    cursor.execute(sqlquery)
                    if sqlquery.strip().lower().startswith('select'):
                        results = cursor.fetchall()
                        logger.debug(f"Query executed successfully via direct connection, returned {len(results)} rows")
                        return list(results)
                    else:
                        logger.debug("Non-SELECT query attempted in read-only transaction")
                        return {"status": "success", "message": "Query executed successfully"}
    except psycopg.Error as e:
        logger.error(f"PostgreSQL error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in direct connection endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
@app.get("/")
def health_check():
    """Health check endpoint to verify the service is running."""
    return {"status": "healthy"}
@app.post("/querySalesforceData")
async def query_salesforce_data(payload: dict):
    sqlquery = payload.get("sqlquery")
    api_key = payload.get("api_key")

    if api_key != REX_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    try:
        with psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        ) as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                with connection.cursor() as cursor:
                    cursor.execute(sqlquery)
                    if sqlquery.strip().lower().startswith("select"):
                        columns = [desc.name for desc in cursor.description]
                        rows = cursor.fetchall()
                        data = [dict(zip(columns, row)) for row in rows]
                        return {"status": "success", "data": data}
                    else:
                        return {"status": "success", "message": "Non-select executed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
