cd
cd ../../work/
mkdir -p ./node-$2/
cd /home/gmangeno/2A/Donnees-reparties/hagidoop/src/
nohup java hdfs.HdfsServer $1 /work/node-$2/ > /home/gmangeno/2A/Donnees-reparties/logServer-$2.txt &
echo "Server deployed on port $1"
nohup java daemon.WorkerImpl $(($1+1)) /work/node-$2/ > /home/gmangeno/2A/Donnees-reparties/logWorker-$2.txt &
echo "Worker deployed on port $(($1+1))"