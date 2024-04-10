# model_test
Test a model deployed in Cloud Run service

## Requirements
- Python3.+
- request
- json

## Usage
- Update the bearer token in test.py
- Run the test script. It will the input.json file and send requests to the model in Cloud Run. The results with bad prediccions will be saved in the fail_predictions.csv file
```
python test.py
```
