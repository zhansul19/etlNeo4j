from fastapi import HTTPException
from paramiko import SSHClient, AutoAddPolicy
from io import BytesIO
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# SSH connection details
SSH_HOST = '192.168.122.104'
SSH_USERNAME = 'root'
SSH_PASSWORD = '1q2w3e$R%T'
SSH_DESTINATION_PATH = '/opt/neo4j7474/import/dd1/'

def transfer_file_via_scp(file_contents: bytes, filename: str):
    try:
        # Establish SSH connection
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(SSH_HOST, username=SSH_USERNAME, password=SSH_PASSWORD)

        # Create a temporary file in memory
        file_obj = BytesIO(file_contents)
        file_obj.seek(0)

        # Transfer file via SCP
        scp = ssh.open_sftp()
        print(f"{SSH_DESTINATION_PATH}/{filename}")
        scp.putfo(file_obj, f"{SSH_DESTINATION_PATH}{filename}")
        scp.close()

        # Close SSH connection
        ssh.close()

        return {"message": f"File {filename} transferred successfully via SCP"}
    except Exception as e:
        logging.error(f"Error during SCP file transfer: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to transfer file: {str(e)}")

