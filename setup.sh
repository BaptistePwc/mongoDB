#!/bin/bash
docker-compose up -d
cd sample_training
sudo wget https://dst-de.s3.eu-west-3.amazonaws.com/mongo_fr/books.json
mongoimport -d sample -c books --authenticationDatabase admin --username admin --password pass --file data/db/books.json
python3 exam.py > res.txt