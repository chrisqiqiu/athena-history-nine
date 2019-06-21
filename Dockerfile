FROM python:3

WORKDIR /usr/src/app

COPY . ./

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install awscli
RUN chmod a+x start.sh

ENTRYPOINT ["/bin/bash"]

CMD ["./start.sh"]
