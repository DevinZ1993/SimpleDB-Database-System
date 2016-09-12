#! /bin/bash -e

ant dist
java -jar dist/simpledb.jar parser dblp_data/dblp_simpledb.schema

# select p.title from authors a, paperauths pa, papers p, venues v where a.name='E. F. Codd' and a.id = pa.authorid and pa.paperid = p.id and p.venueid = v.id;

# select a2.name, count(pa1.paperid) from authors a1, paperauths pa1, paperauths pa2, authors a2 where a1.name= 'Michael Stonebraker' and a1.id=pa1.authorid and pa1.paperid = pa2.paperid and pa2.authorid = a2.id group by a2.name order by a2.name;

