import json
import requests as req

with open("dataset_24476_3 (1).txt", 'r') as f:
    lines = f.read().splitlines()
    for digit in lines:
        result = req.get(f"http://numbersapi.com/{digit}/math?json=true")
        if json.loads(result.text)['found']:
            print('Interesting')
        else:
            print('Boring')


