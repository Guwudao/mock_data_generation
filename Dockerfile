FROM python:3.8

MAINTAINER Jackie

WORKDIR ./mockdata

ADD . .

RUN pip install -r requirements.txt
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

CMD ["python", "./main.py"]