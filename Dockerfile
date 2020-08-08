From python:3
RUN apt update
RUN pip3 install flask flask_sqlalchemy sqlalchemy
WORKDIR /app
add ./app /app
add ./data /data
ENTRYPOINT ["python","app.py"]