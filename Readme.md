# Log Analysis Tool

This is a simple tool developed in Python that prints out reports (in plain text) based on the data in the 'news' database. 

The repot has the following information.
1.	What are the most popular three articles of all time?
2.	Who are the most popular article authors of all time?
3.	On which days did more than 1% of requests lead to errors?

A sample output is available in 'SampleOutput.txt'.

## Getting Started

The following softwares are required to be present in the target machine to run this tool.
-  Python 3.7
-  psycopg2
-  PostgreSQL

## How to Run

Copy the file ‘loganalysis.py’ to the target directory. Execute the following command in the command prompt (for eg: cmd, gitbash etc)

```
python loganalysis.py
```

Tool creates the view 'view_analysiscolumnsconsolidated' to do the analysis. It is created by the following statement.

```
create or replace view
    view_analysiscolumnsconsolidated
as
    select log.path, log.status,
            log.time, articles.title, authors.name
        from log, articles, authors
    where
        log.path = '/article/' || articles.slug
            and
        articles.author = authors.id;
```

## Author

-   Shiv - iamshiv.trainings@gmail.com