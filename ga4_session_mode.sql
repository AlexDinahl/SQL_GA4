--https://cloud.google.com/bigquery/docs/sessions-create?hl=en
--https://gist.github.com/valentinumbach/80d8a7254091e99af365df49f8e1d520
--drop table _SESSION.users;
--drop table _SESSION.purchases;

CREATE TEMP TABLE users
(
  id INT64,
  name STRING
);

INSERT INTO users
VALUES (1, 'Steph');

INSERT INTO users
VALUES (2, 'Pilou');

INSERT INTO users
VALUES (3, 'Valentin');


CREATE TEMP TABLE purchases
(
  id INT64,
  date DATE,
  user_id INT64
);

INSERT INTO purchases
VALUES (1, '2023-10-01',2);

INSERT INTO purchases
VALUES (2, '2023-10-01',3);

INSERT INTO purchases
VALUES (3, '2023-10-01',NULL);


select users.* 
from users 
left join purchases
on users.id=purchases.user_id
where purchases.user_id is null;


select * 
from users 
where id not in (select user_id from purchases where user_id is not null);
