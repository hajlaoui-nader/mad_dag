CREATE table if not exists events (
    id varchar(255) primary key,
    timestamp timestamp not null,
    email varchar(255) not null,
    company varchar(255) not null,
    country varchar(255) not null,
    uri varchar(255) not null,
    action varchar(255) not null,
    tags varchar(255) not null
);

CREATE table if not exists temp_ids (
    id varchar(255) primary key
);
