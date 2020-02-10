FROM mongo:4.0
CMD ["mongod","--replSet","test"]