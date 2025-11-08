FROM python:3.11-slim

WORKDIR /Datafactory
COPY requirements.txt .

RUN pip install -r requirements.txt


COPY . .
WORKDIR /Datafactory/datafactory/project_structure
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0"]
