# Use official python 3.11 image as our base image 
FROM python:3.11 


# Define working directory inside container 
WORKDIR /usr


# Copy our requirements to workdir 
COPY requirements.txt .


# Install python dependencies 
RUN pip install --no-cache-dir -r requirements.txt 


RUN apt-get update && apt-get install -y wget \
 && wget https://github.com/quarto-dev/quarto-cli/releases/download/v1.9.37/quarto-1.9.37-linux-amd64.deb \
 && apt-get install -y ./quarto-1.9.37-linux-amd64.deb || apt-get install -f -y \
&& rm quarto-1.9.37-linux-amd64.deb
# Copy all our app code to workdir 
COPY . .


# Run app
CMD [ "quarto", "serve", "app/flights.qmd" ]