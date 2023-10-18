
FROM python


COPY input.csv /app/
COPY output.csv /app/
COPY dcrapp.py /app/


WORKDIR /app

RUN pip install pandas

#RUN chmod +x /app/preprocess.py

CMD ["python", "dcrapp.py", "input.csv", "output.csv"]
