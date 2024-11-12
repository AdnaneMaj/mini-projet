# NOAA Meteorological Data Processing

This project is about big data for treating big data

---

## Configuration and installation

Clone the repository 
```bash
```

Set the env and activate it
```bash
$ conda env create -f environment.yml
$ conda activate noaa-env
```

## Run the FastAPI server
```bash
$ uvicorn main:app --reload --host 0.0.0.0 --port 5000
```