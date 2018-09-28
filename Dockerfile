FROM python:3.7

ADD . /src
EXPOSE 8080
WORKDIR /src

RUN pip install -r requirements.txt

CMD ["python", "main.py"]