DROP TABLE IF EXISTS compliance;
CREATE TABLE compliance (
    id integer primary key,
    branch string not null,
    commitSha string not null,
    committer string not null,
    errors integer not null,
    failed integer not null,
    skipped integer not null,
    passed integer not null,
    duration float not null,
    uploaded datetime not null
);
