from fastapi import FastAPI, APIRouter, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from neo4j import GraphDatabase
import logging
from ssh import transfer_file_via_scp
from relation import router as property_router
from property import router as relation_router

app = FastAPI()
app.include_router(property_router)
app.include_router(relation_router)

origins = [
    "*",
]
# Add CORS middleware to allow requests from specified origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Neo4j connection details
NEO4J_URI = "bolt://192.168.122.104:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "afmdpar"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# SSH connection details
SSH_HOST = '192.168.122.104'
SSH_USERNAME = 'root'
SSH_PASSWORD = '1q2w3e$R%T'
SSH_DESTINATION_PATH = '/dd1/'

@app.post("/create_node/label={label}/")
async def create_node(label: str, file: UploadFile = File(...)):
    try:
        contents = await file.read()
        logging.info(f"Received file: {file.filename} of type: {file.content_type}")

        # Transfer file via SCP
        transfer_result = transfer_file_via_scp(contents, file.filename)
        logging.info(f"File transferred: {transfer_result}")

        # Use the transferred file to create nodes in Neo4j
        local_csv_file = file.filename  # use the same filename for consistency

        with driver.session() as session:
            # Construct the correct file URL for Neo4j LOAD CSV
            remote_csv_url = f"file://{SSH_DESTINATION_PATH}/{local_csv_file}"
            # Create nodes using apoc.periodic.iterate
            query = f"""
            CALL apoc.periodic.iterate(
              "LOAD CSV WITH HEADERS FROM '{remote_csv_url}' AS row FIELDTERMINATOR ',' RETURN row",
              "CREATE (n:{label}) SET n += row",
              {{batchSize: 1000, iterateList: true}})
            """

            result = session.run(query)
            stats = result.single()

        return JSONResponse(content={"message": "Nodes created successfully", "details": stats}, status_code=200)
    except Exception as e:
        logging.error(f"Error in /create_node/: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/h/")
async def hello():
    return "hello"





# docker run -d --name neo4j -p 7474:7474 -p 7687:7687 -v C:/Users/User6/PycharmProjects/etlNeo4j/import:/var/lib/neo4j/import -v C:/Users/User6/Downloads/neo4j-apoc:/var/lib/neo4j/plugins neo4j:5.1
