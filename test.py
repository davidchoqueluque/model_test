import json
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import concurrent
import time


def send_request_to_cloud_run(payload, url, bearer_token):
    """Send a POST request to a Cloud Run Service

    Args:
        payload (dict): Dictionary with data for the model
        url (str): Url of the Cloud Run service
        bearer_token (str): Authentication token to send the request

    Returns:
        requests.models.Response: Response of the request
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {bearer_token}'
    }
    response = requests.post(url, headers=headers, json=payload)
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

def test_model_parallel(json_file: str, url: str, bearer_token: str, field_to_test: str):
    rows = []
    with open(json_file, 'r') as file:
        payloads = json.load(file)
    # Create a ThreadPoolExecutor with 10 threads
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit each payload to the executor
        futures = [executor.submit(send_request_to_cloud_run, data, url, bearer_token) for data in payloads]
        for call_number, data in enumerate(payloads):
            futures.append(executor.submit(send_request_to_cloud_run, data, url, bearer_token))

        # Wait for all futures to complete and get the responses
        responses = [future.result() for future in futures]

    # Print or process the responses as needed
    print(responses)

def send_requests_parallel(json_file: str, url: str, bearer_token: str):
    with open(json_file, 'r') as file:
        payloads = json.load(file)
    responses = []
    fails = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_payload = {executor.submit(send_request_to_cloud_run, payload, url, bearer_token): payload for payload in payloads}
        for future in concurrent.futures.as_completed(future_to_payload):
            payload = future_to_payload[future]
            try:
                response = future.result()
                responses.append(response)
                payload_response = payload
                payload_response['ground_truth'] = payload['riesgo']
                if response.status_code != 200:
                    payload_response['pred'] = None
                    fails.append(payload_response)
                else:
                    response_dict = response.json()
                    if response_dict['riesgo'] != payload['riesgo']:
                        payload_response['pred'] = response_dict['riesgo']
                        fails.append(payload_response)
            except Exception as e:
                print(f"An error occurred for payload {payload}: {e}")
            finally:
                # Print the index of the payload being processed
                index = payloads.index(payload)
                print(f"Processed payload at index {index}/")
    return responses, fails

def list_of_dict_to_csv(mylist, file_path):
    print(f"====> saving to: {file_path}")
    df = pd.DataFrame(mylist)
    df.to_csv(file_path, index=False, header=True)

def list_of_dict_to_json(mylist, file_path):
    # Save the list of dictionaries to a JSON file
    with open(file_path, "w") as json_file:
        json.dump(mylist, json_file, indent=4)

def test_model_inference_v2(json_file: str, cloud_run_url: str, bearer_token: str, field_to_test: str):
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
    for i,item in enumerate(data):
        print(i)
        payload = item
        gt = item['riesgo']
        response = send_request_to_cloud_run(payload, cloud_run_url, bearer_token)
        prediction = response.json()
        if response.status_code != 200:
            payload['ground_truth'] = gt
            payload['pred'] = None
            rows.append(payload)
        else:
            prediction = prediction[field_to_test]
            if prediction != gt:
                payload['ground_truth'] = gt
                payload['pred'] = prediction
                rows.append(payload)
        
    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_FILE, index=False, header=True)

if __name__ == '__main__':
    INPUT_JSON_FILE = 'output_v4.json'
    OUTPUT_FILE = 'fail_predictions_test.csv'
    # CLOUD_RUN_URL = 'https://bento-segmentacion-vehicular-impl-uwr3p4egga-uk.a.run.app/predict'
    CLOUD_RUN_URL = 'https://bento-segmentacion-vehicular-impl-uqottehkva-uk.a.run.app/predict'
    BEARER_TOKEN = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjkzYjQ5NTE2MmFmMGM4N2NjN2E1MTY4NjI5NDA5NzA0MGRhZjNiNDMiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJhY2NvdW50cy5nb29nbGUuY29tIiwiYXpwIjoiNjE4MTA0NzA4MDU0LTlyOXMxYzRhbGczNmVybGl1Y2hvOXQ1Mm4zMm42ZGdxLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwiYXVkIjoiNjE4MTA0NzA4MDU0LTlyOXMxYzRhbGczNmVybGl1Y2hvOXQ1Mm4zMm42ZGdxLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwic3ViIjoiMTExOTcwODE4NDMzOTk3ODY1ODA1IiwiaGQiOiJyaW1hYy5jb20ucGUiLCJlbWFpbCI6ImRhdmlkLmNob3F1ZWx1cXVlQHJpbWFjLmNvbS5wZSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJhdF9oYXNoIjoiVVY1c3NYSkZIR1ZaVnRyck44V1NmQSIsIm5iZiI6MTcxMjg2ODUyMywiaWF0IjoxNzEyODY4ODIzLCJleHAiOjE3MTI4NzI0MjMsImp0aSI6ImMxMzA3OTc5NmNjMThhMTg3YjgwNDU1ODA3MzljOTkyOWM3ZmI2N2QifQ.pzIdpNtTjV9Yk0m7D1-YORwXsblX6vexy_KtPzLERG2t0Nd--KSkllypSYerpFQ4I4q-UuEncWxOi4SCuWTWLda3LkTBnT_BWo2U0V7NnDtD3EnjO3H5hcziresXmkru1MdjOCX8zRjVaPRby_6okWh5CH01qHD_mpZ3GYfA9vOAWnZdUQ9uAIMFGI-xei9jXeWQpqR8zTt7KwXtK1KbLNDXWIR4u1Dzuoi4pVQyYy9T_vpbAcfQ3raHPjDJ2Rki4vafydcJWRV-r6SNBiQ4W-WBlam6JEpWKESSjg52umtRqX_SqXqbU2ZaYTRtPKzh40bj4xxvlNe4oL-n3YZGOw'
    FIELD_TO_TEST = "riesgo"
    # test_model_inference_v2(INPUT_JSON_FILE, CLOUD_RUN_URL, BEARER_TOKEN, FIELD_TO_TEST)

    start_time = time.time()
    # test_model_parallel(INPUT_JSON_FILE, CLOUD_RUN_URL, BEARER_TOKEN, FIELD_TO_TEST)
    responses, fails = send_requests_parallel(INPUT_JSON_FILE, CLOUD_RUN_URL, BEARER_TOKEN)
    end_time = time.time()

    # Calculate the elapsed time in minutes and seconds
    elapsed_time = end_time - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    # Print or process the responses as needed
    print(f"Elapsed time: {minutes} minutes, {seconds} seconds")

    list_of_dict_to_csv(fails, OUTPUT_FILE)
    # list_of_dict_to_json(fails, 'fail_predictions_v5.json')