FROM python:3.8

WORKDIR ./mockdata

ADD . .

RUN pip install -r requirements.txt
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

CMD echo Start Mock Data Generation
CMD ["python", "./main.py"]