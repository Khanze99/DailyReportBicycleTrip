create database bservice;

create user {myuser} with password {password};

grant all privileges on database bservice to {myuser};

create table IF NOT EXISTS
 bservice.file_bucket(
    name text,
    is_notify boolean default false,
    is_download boolean default false,
    loaded_count_trips boolean default false,
    loaded_average_trip_duration boolean default false,
    loaded_trip_gender boolean default false
    );