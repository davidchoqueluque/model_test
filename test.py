import json
import requests
import pandas as pd


def send_request_to_cloud_run(data, url, bearer_token):
    """Send a POST request to a Cloud Run Service

    Args:
        data (dict): Dictionary with data for the model
        url (str): Url of the Cloud Run service
        bearer_token (str): Authentication token to send the request

    Returns:
        requests.models.Response: Response of the request
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {bearer_token}'
    }
    response = requests.post(url, headers=headers, json=data)
    return response

def test_model_inference(json_file: str, cloud_run_url: str, bearer_token: str, field_to_test: str):
    """Read a json file with payloads, test a ML model, and save the results as CSV.

    Args:
        json_file (str): File name of the json with test cases
        cloud_run_url (str): Url to query a ML model deployed in Cloud Run
        bearer_token (str): Authentication token to call the Cloud run service
        field_to_test (str): Field or property of the model response to evaluate
    """
    rows = []
    with open(json_file, 'r') as file:
        data = json.load(file)
    for item in data:
        payload = item["payload"]
        gt = item['ground_truth']
        response = send_request_to_cloud_run(payload, cloud_run_url, bearer_token).json()
        prediction = response[field_to_test]
        if prediction != gt:
            payload['ground_truth'] = gt
            payload['pred'] = prediction
            rows.append(payload)
    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_FILE, index=False, header=True)

if __name__ == '__main__':
    INPUT_JSON_FILE = 'input.json'
    OUTPUT_FILE = 'fail_predictions.csv'
    CLOUD_RUN_URL = 'https://bento-segmentacion-vehicular-impl-uwr3p4egga-uk.a.run.app/predict'
    BEARER_TOKEN = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjkzYjQ5NTE2MmFmMGM4N2NjN2E1MTY4NjI5NDA5NzA0MGRhZjNiNDMiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJhY2NvdW50cy5nb29nbGUuY29tIiwiYXpwIjoiNjE4MTA0NzA4MDU0LTlyOXMxYzRhbGczNmVybGl1Y2hvOXQ1Mm4zMm42ZGdxLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwiYXVkIjoiNjE4MTA0NzA4MDU0LTlyOXMxYzRhbGczNmVybGl1Y2hvOXQ1Mm4zMm42ZGdxLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwic3ViIjoiMTExOTcwODE4NDMzOTk3ODY1ODA1IiwiaGQiOiJyaW1hYy5jb20ucGUiLCJlbWFpbCI6ImRhdmlkLmNob3F1ZWx1cXVlQHJpbWFjLmNvbS5wZSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJhdF9oYXNoIjoia1drbVgyemN5bjVOaUNmc0NCdEg4ZyIsIm5iZiI6MTcxMjc4MzkxMCwiaWF0IjoxNzEyNzg0MjEwLCJleHAiOjE3MTI3ODc4MTAsImp0aSI6ImUxOWRjZmIwNTY1ZDY1OGRhMzdlMTlkNWIwNjk1YzQ1YWVlOGNjMjUifQ.KLQ7yLpstulAdJIpgTZ_QNLW9SGr9mZr0-LK9YOb6uCaLqwgj52GAYducYxCOELb3nQpNxyPTEMB9wIUVRQ7IJKloz78we79Qpbcb44aTNOuPpyWekpZhMHST5JmU16_CjavIF56aHqd6qpy_ESEQ9O8PdILLIjM4AnhdqXrIyNXQGDxF0SyFRjXtBnX8WFVmer0FHSZfg89VXYkV45iydHdz2vzrFr0fysMq5paUSYFrxF6D7j6lOmJrwEyvUOu_I-9VJ9JlTp0dshmDfAL7cgEW36vnh4TVYsiSDrwEW2wJhlZz6-XOfot3OtnOkdIECOe7GyBdnZGxe3uxTXIlQ'
    FIELD_TO_TEST = "riesgo"
    test_model_inference(INPUT_JSON_FILE, CLOUD_RUN_URL, BEARER_TOKEN, FIELD_TO_TEST)