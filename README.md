# kr_toilet
This project is REST API server for providing Korean public toilet information.
Original data is from here. https://www.data.go.kr/dataset/15012892/standard.do
And I made csv file using these data. (Remove unnecessary column using Excel, and convert encodings to UTF-8 from euc-kr)

to generate DB
cd src/script
./init_db.sh

(PRE_CONDITION)
modify init_db.sh and change db name you have and user name you have

db_user="postgres" <-- database user name
db_name="kr_geo" <--- database name
