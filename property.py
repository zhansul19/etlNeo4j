from fastapi import APIRouter, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from neo4j import GraphDatabase
import logging
from ssh import transfer_file_via_scp, SSH_DESTINATION_PATH

router = APIRouter()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Neo4j connection details
NEO4J_URI = "bolt://192.168.122.104:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "afmdpar"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


@router.post("/add_property/person/")
async def add_property(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        logging.info(f"Received file: {file.filename} of type: {file.content_type}")

        # Transfer file via SCP
        transfer_result = transfer_file_via_scp(contents, file.filename)
        logging.info(f"File transferred: {transfer_result}")

        # Use the transferred file to create relationships in Neo4j
        local_csv_file = file.filename  # use the same filename for consistency

        with driver.session() as session:
            # Construct the correct file URL for Neo4j LOAD CSV
            remote_csv_url = f"file:///dd1/{local_csv_file}"
            # Create relationships using apoc.periodic.iterate
            query = f"""
            CALL apoc.periodic.iterate(
              "LOAD CSV WITH HEADERS FROM '{remote_csv_url}' AS row FIELDTERMINATOR ',' RETURN row",
              "MATCH (a:Person {{`ИИН`: row.IIN_1}})
               SET a += apoc.map.removeKeys(row, ['IIN_1'])",
              {{batchSize: 1000, iterateList: true}})
            """

            result = session.run(query)
            stats = result.single()

        return JSONResponse(content={"message": "Properties added successfully", "details": stats}, status_code=200)
    except Exception as e:
        logging.error(f"Error in /add_property person/: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/add_property/company/")
async def add_property(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        logging.info(f"Received file: {file.filename} of type: {file.content_type}")

        # Transfer file via SCP
        transfer_result = transfer_file_via_scp(contents, file.filename)
        logging.info(f"File transferred: {transfer_result}")

        # Use the transferred file to create relationships in Neo4j
        local_csv_file = file.filename  # use the same filename for consistency

        with driver.session() as session:
            # Construct the correct file URL for Neo4j LOAD CSV
            remote_csv_url = f"file:///dd1/{local_csv_file}"
            # Create relationships using apoc.periodic.iterate
            query = f"""
            CALL apoc.periodic.iterate(
              "LOAD CSV WITH HEADERS FROM '{remote_csv_url}' AS row FIELDTERMINATOR ',' RETURN row",
              "MATCH (a:Company {{`БИН`: row.IIN_1}})
               SET a += apoc.map.removeKeys(row, ['IIN_1'])",
              {{batchSize: 1000, iterateList: true}})
            """

            result = session.run(query)
            stats = result.single()

        return JSONResponse(content={"message": "Properties added successfully", "details": stats}, status_code=200)
    except Exception as e:
        logging.error(f"Error in /add_property company/: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/add_property/p2p/type={relationship_type}/")
async def add_property(relationship_type: str, file: UploadFile = File(...)):
    try:
        contents = await file.read()
        logging.info(f"Received file: {file.filename} of type: {file.content_type}")

        # Transfer file via SCP
        transfer_result = transfer_file_via_scp(contents, file.filename)
        logging.info(f"File transferred: {transfer_result}")

        # Use the transferred file to create relationships in Neo4j
        local_csv_file = file.filename  # use the same filename for consistency

        with driver.session() as session:
            # Construct the correct file URL for Neo4j LOAD CSV
            remote_csv_url = f"file:///dd1/{local_csv_file}"
            # Create relationships using apoc.periodic.iterate
            query = f"""
            CALL apoc.periodic.iterate(
              "LOAD CSV WITH HEADERS FROM '{remote_csv_url}' AS row FIELDTERMINATOR ',' RETURN row",
              "MATCH (a:Person {{`ИИН`: row.IIN_1}}), (b:Person {{`ИИН`: row.IIN_2}})
               MATCH (a)-[r:{relationship_type}]->(b)
               SET r += apoc.map.removeKeys(row, ['IIN_1', 'IIN_2'])",
              {{batchSize: 1000, iterateList: true}})
            """

            result = session.run(query)
            stats = result.single()

        return JSONResponse(content={"message": "Relationships created successfully", "details": stats},
                            status_code=200)
    except Exception as e:
        logging.error(f"Error in /create_relationship p2p/: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/add_property/p2u/type={relationship_type}/")
async def add_property(relationship_type: str, file: UploadFile = File(...)):
    print("here")
    try:
        contents = await file.read()
        logging.info(f"Received file: {file.filename} of type: {file.content_type}")

        # Transfer file via SCP
        transfer_result = transfer_file_via_scp(contents, file.filename)
        logging.info(f"File transferred: {transfer_result}")

        # Use the transferred file to create relationships in Neo4j
        local_csv_file = file.filename  # use the same filename for consistency

        with driver.session() as session:
            # Construct the correct file URL for Neo4j LOAD CSV
            remote_csv_url = f"file:///dd1/{local_csv_file}"
            # Create relationships using apoc.periodic.iterate
            query = f"""
            CALL apoc.periodic.iterate(
              "LOAD CSV WITH HEADERS FROM '{remote_csv_url}' AS row FIELDTERMINATOR ',' RETURN row",
              "MATCH (a:Person {{`ИИН`: row.IIN_1}}), (b:Company {{`БИН`: row.IIN_2}})
               MATCH (a)-[r:{relationship_type}]->(b)
               SET r += apoc.map.removeKeys(row, ['IIN_1', 'IIN_2'])",
              {{batchSize: 1000, iterateList: true}})
            """

            result = session.run(query)
            stats = result.single()

        return JSONResponse(content={"message": "Relationships created successfully", "details": stats},
                            status_code=200)
    except Exception as e:
        logging.error(f"Error in /create_relationship p2u/: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/add_property/u2u/type={relationship_type}/")
async def add_property(relationship_type: str, file: UploadFile = File(...)):
    print("here")
    try:
        contents = await file.read()
        logging.info(f"Received file: {file.filename} of type: {file.content_type}")

        # Transfer file via SCP
        transfer_result = transfer_file_via_scp(contents, file.filename)
        logging.info(f"File transferred: {transfer_result}")

        # Use the transferred file to create relationships in Neo4j
        local_csv_file = file.filename  # use the same filename for consistency

        with driver.session() as session:
            # Construct the correct file URL for Neo4j LOAD CSV
            remote_csv_url = f"file:///dd1/{local_csv_file}"
            # Create relationships using apoc.periodic.iterate
            query = f"""
            CALL apoc.periodic.iterate(
              "LOAD CSV WITH HEADERS FROM '{remote_csv_url}' AS row FIELDTERMINATOR ',' RETURN row",
              "MATCH (a:Company {{`БИН`: row.IIN_1}}), (b:Company {{`БИН`: row.IIN_2}})
               MATCH (a)-[r:{relationship_type}]->(b)
               SET r += apoc.map.removeKeys(row, ['IIN_1', 'IIN_2'])",
              {{batchSize: 1000, iterateList: true}})
            """

            result = session.run(query)
            stats = result.single()

        return JSONResponse(content={"message": "Relationships created successfully", "details": stats},
                            status_code=200)
    except Exception as e:
        logging.error(f"Error in /create_relationship u2u/: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/add_property/u2p/type={relationship_type}/")
async def add_property(relationship_type: str, file: UploadFile = File(...)):
    try:
        contents = await file.read()
        logging.info(f"Received file: {file.filename} of type: {file.content_type}")

        # Transfer file via SCP
        transfer_result = transfer_file_via_scp(contents, file.filename)
        logging.info(f"File transferred: {transfer_result}")

        # Use the transferred file to create relationships in Neo4j
        local_csv_file = file.filename  # use the same filename for consistency

        with driver.session() as session:
            # Construct the correct file URL for Neo4j LOAD CSV
            remote_csv_url = f"file:///dd1/{local_csv_file}"
            # Create relationships using apoc.periodic.iterate
            query = f"""
            CALL apoc.periodic.iterate(
              "LOAD CSV WITH HEADERS FROM '{remote_csv_url}' AS row FIELDTERMINATOR ',' RETURN row",
              "MATCH (a:Company {{`БИН`: row.IIN_1}}), (b:Person {{`ИИН`: row.IIN_2}})
               MATCH (a)-[r:{relationship_type}]->(b)
               SET r += apoc.map.removeKeys(row, ['IIN_1', 'IIN_2'])",
              {{batchSize: 1000, iterateList: true}})
            """

            result = session.run(query)
            stats = result.single()

        return JSONResponse(content={"message": "Relationships created successfully", "details": stats},
                            status_code=200)
    except Exception as e:
        logging.error(f"Error in /create_relationship u2p/: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
