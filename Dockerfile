# Use official python 3.11 image as our base image 
FROM python:3.11 


# Define working directory inside container 
WORKDIR /usr/src/app 


# Copy our requirements to workdir 
COPY requirements.txt .


# Install python dependencies 
RUN pip install --no-cache-dir -r requirements.txt 

# Copy all our app code to workdir 
COPY . .


# Run app
CMD [ "quarto", "serve", "app/flights.qmd" ]