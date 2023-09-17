FROM python:3.9

WORKDIR /app

COPY ./requirements.txt /app/

RUN pip install -r requirements.txt

COPY ./youtube-experiment/* /app/

CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:5555"]