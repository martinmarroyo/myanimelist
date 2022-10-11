FROM bitnami/spark:3.2.1
# Change to root so that we can add stuff to the container
USER root
# Add hudi to jar files
COPY hudi-spark3.2-bundle_2.12-0.12.0.jar /opt/bitnami/spark/jars
RUN mkdir /opt/bitnami/spark/localdevelopment
# Add requirements file and install in local development environment
COPY requirements.txt /opt/bitnami/spark/localdevelopment
RUN pip install -r /opt/bitnami/spark/localdevelopment/requirements.txt
EXPOSE 8888
# Start the Jupyter server
CMD ["jupyter-notebook","--ip","0.0.0.0", "--no-browser", "--allow-root"]
