create index sm on foo using smerge (uid);
insert into foo values (1, 'axzagd');
insert into foo values (2, 'axzagd');
insert into foo values (3, 'axzagd');
insert into foo values (4, 'axzagd');
insert into foo values (5, 'axzagd');
insert into foo values (6, 'axzagd');
insert into foo values (7, 'axzagd');
insert into foo values (8, 'axzagd');
insert into foo values (9, 'axzagd');
insert into foo values (0, 'axzagd');
insert into foo values (4, 'axzagd');
insert into foo values (5, 'axzagd');
insert into foo values (6, 'axzagd');
insert into foo values (7, 'axzagd');
insert into foo values (8, 'axzagd');
insert into foo values (9, 'axzagd');
insert into foo values (3, 'axzagd');
insert into foo values (4, 'axzagd');
insert into foo values (5, 'axzagd');
insert into foo values (6, 'axzagd');
insert into foo values (7, 'axzagd');
insert into foo values (8, 'axzagd');
insert into foo values (9, 'axzagd');


select * from foo where uid > 3
