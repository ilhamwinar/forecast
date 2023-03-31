FROM python:3.9-slim
ENV TZ="Asia/Jakarta"
COPY ./app /app

RUN python3 -m pip install --upgrade pip
RUN pip3 install mysql-connector-python
RUN pip3 install mysql-connector
RUN pip3 install numpy 
RUN pip3 install schedule 
RUN pip3 install joblib
RUN pip3 install python-decouple
RUN pip3 install xgboost
RUN pip3 install scikit-learn

WORKDIR /

CMD ["python3","./app/forecasting_japek.py"]
