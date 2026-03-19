import json

sample_json = {
  "renters": [
    {
      "city": "Denver",
      "state": "CO",
      "cost": {
        "2026-01-01": 1200.00,
        "2026-02-01": 1215.50
      }
    },
    {
      "city": "Boulder",
      "state": "CO",
      "cost": {
        "2026-01-01": 1500.75,
        "2026-02-01": 1500.75
      }
    }
  ],
  "buyers": [
    {
      "city": "Denver",
      "state": "CO",
      "cost": {
        "2026-01-01": 1300.01,
        "2026-02-01": 1211.53
      }
    },
    {
      "city": "Boulder",
      "state": "CO",
      "cost": {
        "2026-01-01": 1507.75,
        "2026-02-01": 1521.75
      }
    }
  ]
}

def read(filepath):
    print(f"I should be reading from {filepath}")
    return ""

def process(data):
    # algorithms go here
    json = sample_json
    return json

def write(filepath, data):

    # might have to iterate per line or use numpoy 
    with open(filepath, "w") as file:
        file.write(json.dumps(data, indent = 4))

def main():
    data = read("somefile")
    json = process(data)
    write("src/output/dummy_output.json", json)

if __name__ == "__main__":
    main()