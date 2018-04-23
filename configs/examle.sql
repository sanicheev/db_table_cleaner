---
table: myTable
database: myDatabase
host: localhost
username: root
password: PASSWORD
limit: 1000
threshold: 7
type: common
select_query: "SELECT Id FROM myTable WHERE DATEDIFF(CURRENT_TIME(), CreatedOn) > {THRESHOLD} AND Status='Processed' LIMIT {LIMIT}"
delete_query: "DELETE FROM myTable WHERE Id IN ({VALUE})"
