scp -P 220 users_t.dat uhadoop@cm.dcc.uchile.cl:/data/2023/uhadoop/proyects/group14/.
ssh -p 220 uhadoop@cm.dcc.uchile.cl

hdfs dfs -get /uhadoop2023/group14/results/queries_1 /data/2023/uhadoop/proyects/group14/queries_1
hdfs dfs -get /uhadoop2023/group14/results/queries_2 /data/2023/uhadoop/proyects/group14/queries_2
hdfs dfs -get /uhadoop2023/group14/results/queries_3 /data/2023/uhadoop/proyects/group14/queries_3

cd /data/2023/uhadoop/proyects/group14/queries_1
tar -czvf group14.tar.gz *
