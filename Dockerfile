FROM python:3.9

WORKDIR /app

COPY ./requirements.txt /app
COPY ./app.py /app
COPY ./youtubecrowler.py /app

RUN pip install -r requirements.txt

RUN "echo $MYSQL_HOST"

EXPOSE 5555

CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:5555"]
