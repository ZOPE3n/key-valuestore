# Use an official Python runtime as a parent image
FROM python:3-alpine

# Set the working directory to /HW1-test
WORKDIR /HW1-test

# Copy the current directory contents into the container at /HW1-test
ADD . /HW1-test
#COPY Flask.txt /HW1-test

# Install Flask if needed 
RUN pip install --no-cache-dir -r Flask.txt

#COPY . /HW1-test

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Run app.py when the container launches
CMD [ "python", "./app.py" ]
