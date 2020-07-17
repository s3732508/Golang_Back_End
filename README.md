# Golang-BackEnd
[Full-stack Software Engineer test], PackForm

Golang Drivers to be installed
1.	mongo-driver
2.	PostgreSQL driver for Golang

Execute the following lines of code to install the above drivers

go get go.mongodb.org/mongo-driver/mongo
go get github.com/lib/pq



Things to Follow:
1.	Install PostgreSQL and MongoDB on local computer
2.	Replace Credentials of PostgreSQL and MongoDB databases. (You can find them in the code commented as Credentials). 
3.	MongoDB URI can be found using MongoDBCompassCommunity application which can be added during installation of MongoDB
4.	PostgreSQL credentials can be found using pgAdmin4 application which can be added during installation of PostgreSQL 
5.	Make sure the data.tar.bz2 file is in the same directory as the Go files. Because the execution searches for the file in the same folder and extracts the csv files in a new folder with name “data”.
6.	Make sure port numbers are unique. Currently the Golang port is set to run on 9000 in localhost.

Starting the Server :

Build:
go build BackEnd.go

Start the Application
BackEnd.exe

Regards
Vikas Peri
