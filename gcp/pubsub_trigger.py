import base64
import json
from google.cloud import storage
import logging

def pubsub_trigger(event, context):
    """
    Triggered from a message on a Cloud Pub/Sub topic.
    
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        if 'data' in event:
            pubsub_message = base64.b64decode(event['data']).decode('utf-8')
            message_json = json.loads(pubsub_message)
            
            # Log the received message
            logging.info(f"Received Pub/Sub message: {message_json}")
            
            # Implement your processing logic here, e.g., move data to GCS,
            # trigger further pipeline steps, invoke Snowflake ingestion, etc.
            # For example, upload message to GCS bucket:
            # storage_client = storage.Client()
            # bucket = storage_client.bucket('your-gcs-bucket-name')
            # blob = bucket.blob(f"events/{context.event_id}.json")
            # blob.upload_from_string(pubsub_message)
            
        else:
            logging.warning('No data in the Pub/Sub message.')

    except Exception as e:
        logging.error(f"Error processing Pub/Sub message: {e}")
        raise e
