from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

api_key = "AIzaSyCnNHuKC9GvX07oRpzTHnFoUGeH3Hpm6Ks"

with build('youtube', 'v3', developerKey=api_key) as service:

    request = service.channels().list(
        part="statistics",
        id="UCdA45vl6KVQSdmL4i9G80mg"
    )

try:
    response = request.execute()
    print(response)
except HttpError as e:
    print('Error response status code : {0}, reason : {1}'.format(e.status_code, e.error_details))
