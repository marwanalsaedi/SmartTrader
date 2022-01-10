FROM python:3.10-slim
ENV PYTHONUNBUFFERED True
ENV PYTHONPATH /app
WORKDIR /app
COPY . /app
#RUN apt update && apt upgrade -y
RUN pip install --no-cache-dir -r requirements.txt
ENV FLASK_ENV development
ENV GOOGLE_APPLICATION_CREDENTIALS='serviceacc.json'
#CMD exec flask run --host=0.0.0.0
CMD exec python app.py