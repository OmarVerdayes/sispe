FROM public.ecr.aws/lambda/python:3.9
COPY hello_world ${LAMBDA_TASK_ROOT}/hello_world
WORKDIR ${LAMBDA_TASK_ROOT}/hello_world
RUN pip install -r requirements.txt
CMD ["hello_world.app.lambda_handler"]