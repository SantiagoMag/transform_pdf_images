# Use the official AWS Lambda Python 3.9 base image
FROM public.ecr.aws/lambda/python:3.9

# Install Poppler utilities
RUN yum -y install poppler-utils

# Install pdf2image and its dependencies
RUN pip install pdf2image boto3

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of Dockerfile)
CMD ["app.lambda_handler"]