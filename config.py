from dotenv import load_dotenv
load_dotenv()
import os

config = {
     'bootstrap.servers': os.getenv('CLUSTER_SERVER'),
     'security.protocol': 'SASL_SSL',
     'sasl.mechanisms': 'PLAIN',
     'sasl.username': os.getenv('CLUSTER_API_KEY'), 
     'sasl.password': os.getenv('CLUSTER_API_SECRET')
     }


sr_config = {
    'url': os.getenv('SR_SERVER'),
    'basic.auth.user.info':f"{os.getenv('SR_API_KEY')}:{os.getenv('SR_API_SECRET')}"
}


schema_str = """{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Temperature",
    "description": "Temperature sensor reading",
    "type": "object",
    "properties": {
      "city": {
        "description": "City name",
        "type": "string"
      },
      "reading": {
        "description": "Current temperature reading",
        "type": "number"
      },
      "unit": {
        "description": "Temperature unit (C/F)",
        "type": "string"
      },
      "timestamp": {
        "description": "Time of reading in ms since epoch",
        "type": "number"
      }
    }
  }"""
