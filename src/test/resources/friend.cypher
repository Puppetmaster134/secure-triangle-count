CREATE (Alice:Person {name:'Alice'})
CREATE (Michael:Person {name:'Michael'})
CREATE (Karin:Person {name:'Karin'})
CREATE (Chris:Person {name:'Chris'})
CREATE (Will:Person {name:'Will'})
CREATE (Mark:Person {name:'Mark'})
CREATE (Bob:Person {name:'Bob'})

CREATE
  (Alice)-[:FRIENDS_WITH]->(Michael),
  (Alice)-[:FRIENDS_WITH]->(Karin),
  (Alice)-[:FRIENDS_WITH]->(Chris),
  (Michael)-[:FRIENDS_WITH]->(Karin),
  (Michael)-[:FRIENDS_WITH]->(Chris),
  (Michael)-[:FRIENDS_WITH]->(Will),
  (Karin)-[:FRIENDS_WITH]->(Chris),
  (Karin)-[:FRIENDS_WITH]->(Mark),
  (Chris)-[:FRIENDS_WITH]->(Will),
  (Chris)-[:FRIENDS_WITH]->(Mark),
  (Chris)-[:FRIENDS_WITH]->(Bob)